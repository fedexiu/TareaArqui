"""Definición del índice de búsqueda compartida por la API y el indexador.

Igual que en Discord, el índice es una proyección derivada: se puede borrar y
reconstruir a partir de los eventos sin tocar el servicio dueño de los datos.
"""
import os
import re
import time
from datetime import datetime, timezone

from elasticsearch import Elasticsearch

ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")
# Los clientes usan siempre el alias; el índice físico lleva versión para
# poder reindexar con un mapping nuevo y cambiar el alias sin downtime.
ALIAS = "messages"
INDEX = "messages-v1"

MENTION = re.compile(r"<@([0-9a-fA-F-]{36})>")
LINK = re.compile(r"https?://[^\s<>]+")

SETTINGS = {
    "number_of_shards": 2,
    "number_of_replicas": 0,
    "refresh_interval": "1s",
    "analysis": {"analyzer": {"chat_text": {
        "type": "custom", "tokenizer": "standard",
        "filter": ["lowercase", "asciifolding"]}}},
}

MAPPINGS = {
    "dynamic": "strict",
    "properties": {
        "id": {"type": "keyword"},
        "channel_id": {"type": "keyword"},
        "author_id": {"type": "keyword"},
        "content": {"type": "text", "analyzer": "chat_text"},
        "mentions": {"type": "keyword"},
        "links": {"type": "keyword"},
        "has": {"type": "keyword"},
        "created_at": {"type": "date"},
        "edited_at": {"type": "date"},
        "deleted": {"type": "boolean"},
        "version": {"type": "long"},
        "indexed_at": {"type": "date"},
    },
}


def client() -> Elasticsearch:
    return Elasticsearch(ELASTICSEARCH_URL, request_timeout=10)


def wait_for(es: Elasticsearch) -> None:
    while True:
        try:
            es.cluster.health(wait_for_status="yellow", timeout="5s")
            return
        except Exception:
            time.sleep(2)


def ensure_index(es: Elasticsearch) -> None:
    if not es.indices.exists(index=INDEX):
        es.indices.create(index=INDEX, settings=SETTINGS, mappings=MAPPINGS,
                          aliases={ALIAS: {}})


def to_document(message: dict) -> dict:
    """Extrae del mensaje los atributos buscables, como `has:` y `mentions:`."""
    content = message.get("content", "")
    mentions = sorted(set(MENTION.findall(content)))
    links = sorted(set(LINK.findall(content)))
    has = (["link"] if links else []) + (["mention"] if mentions else [])
    return {
        "id": message["id"],
        "channel_id": message["channel_id"],
        "author_id": message["author_id"],
        "content": content,
        "mentions": mentions,
        "links": links,
        "has": has,
        "created_at": message["created_at"],
        "edited_at": message.get("edited_at"),
        "deleted": bool(message.get("deleted", False)),
        "version": message["version"],
        "indexed_at": datetime.now(timezone.utc).isoformat(),
    }
