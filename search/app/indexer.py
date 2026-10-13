"""Indexador: consume eventos de mensajes y los escribe en Elasticsearch.

Como en Discord, el indexado es asíncrono y por lotes: los eventos se acumulan
hasta BATCH_SIZE o FLUSH_SECONDS y se envían con una sola petición `_bulk`.
Un evento solo se confirma (ack) cuando Elasticsearch lo aceptó, por lo que
una caída del indexador o de Elasticsearch no pierde cambios: se reintentan.

Idempotencia y orden: cada documento usa `version_type=external` con la
versión del mensaje. Un evento repetido o atrasado produce un 409 y se
descarta, así el índice converge al último estado aunque los eventos lleguen
duplicados o desordenados (consistencia eventual).
"""
import json
import logging
import os
import time

import pika
from elasticsearch import ApiError, TransportError

from app.index import ALIAS, client, ensure_index, to_document, wait_for

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://chat:chat@rabbitmq:5672/")
QUEUE = "search.indexer"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
FLUSH_SECONDS = float(os.environ.get("FLUSH_SECONDS", "0.5"))
EVENT_TYPES = {"chat.message.created", "chat.message.updated",
               "chat.message.deleted"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s indexer %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)
log = logging.getLogger("indexer")
es = client()


def declare(channel) -> None:
    """Topología idempotente; coincide con rabbitmq/definitions.json."""
    channel.exchange_declare(exchange="chat", exchange_type="topic", durable=True)
    channel.exchange_declare(exchange="chat.dlx", exchange_type="fanout",
                             durable=True)
    channel.queue_declare(queue=f"{QUEUE}.dlq", durable=True,
                          arguments={"x-queue-type": "quorum"})
    channel.queue_bind(queue=f"{QUEUE}.dlq", exchange="chat.dlx")
    channel.queue_declare(queue=QUEUE, durable=True, arguments={
        "x-queue-type": "quorum", "x-dead-letter-exchange": "chat.dlx"})
    channel.queue_bind(queue=QUEUE, exchange="chat", routing_key="chat.message.*")


def parse(body: bytes) -> dict | None:
    try:
        event = json.loads(body)
        if event["type"] not in EVENT_TYPES:
            return None
        return to_document(event["data"])
    except (ValueError, KeyError, TypeError):
        return None


def flush(channel, batch: list) -> None:
    """Envía el lote a Elasticsearch y confirma cada evento según su resultado."""
    operations, tags = [], []
    for method, body in batch:
        document = parse(body)
        if document is None:
            # Evento inválido: no tiene sentido reintentarlo, va a la DLQ.
            channel.basic_nack(method.delivery_tag, requeue=False)
            continue
        operations += [{"index": {"_index": ALIAS, "_id": document["id"],
                                  "routing": document["channel_id"],
                                  "version": document["version"],
                                  "version_type": "external"}},
                       document]
        tags.append(method.delivery_tag)
    if not operations:
        return
    try:
        response = es.bulk(operations=operations)
    except (ApiError, TransportError) as error:
        log.warning("bulk failed, requeueing %d events: %s", len(tags), error)
        for tag in tags:
            channel.basic_nack(tag, requeue=True)
        time.sleep(2)
        return
    indexed = stale = 0
    for tag, item in zip(tags, response["items"]):
        result = item["index"]
        if result["status"] < 300:
            indexed += 1
            channel.basic_ack(tag)
        elif result["status"] == 409:
            stale += 1  # duplicado o fuera de orden: ya hay una versión mayor
            channel.basic_ack(tag)
        elif result["status"] == 429 or result["status"] >= 500:
            channel.basic_nack(tag, requeue=True)
        else:
            log.error("rejected %s: %s", result["_id"], result.get("error"))
            channel.basic_nack(tag, requeue=False)
    log.info("bulk: %d indexed, %d stale", indexed, stale)


def run() -> None:
    wait_for(es)
    ensure_index(es)
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    declare(channel)
    channel.basic_qos(prefetch_count=BATCH_SIZE * 2)
    log.info("consuming %s", QUEUE)
    batch: list = []
    deadline = time.monotonic() + FLUSH_SECONDS
    for method, _, body in channel.consume(QUEUE, inactivity_timeout=FLUSH_SECONDS):
        if method is not None:
            batch.append((method, body))
        if batch and (len(batch) >= BATCH_SIZE or time.monotonic() >= deadline
                      or method is None):
            flush(channel, batch)
            batch = []
        if not batch:
            deadline = time.monotonic() + FLUSH_SECONDS


if __name__ == "__main__":
    while True:
        try:
            run()
        except pika.exceptions.AMQPError as error:
            # Los eventos sin ack vuelven a la cola y se reprocesan.
            log.warning("broker connection lost: %r", error)
            time.sleep(2)
