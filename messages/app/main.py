"""Servicio dueño de mensajes; usa APIs públicas para validar referencias.

Los cambios se publican al broker con un outbox embebido en el propio
documento: el mensaje y su evento pendiente se escriben en una sola operación
atómica de MongoDB, y un relay en segundo plano los entrega a RabbitMQ
(entrega al menos una vez). Así ningún cambio se pierde si el broker cae.
"""
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from uuid import uuid4

import pika
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field
from pymongo import ASCENDING, MongoClient, ReturnDocument

database = MongoClient(os.environ["MONGODB_URL"]).chat_messages
messages = database.messages
events: list[dict] = []
USERS_URL = os.environ.get("USERS_URL", "http://users:8000")
CHANNELS_URL = os.environ.get("CHANNELS_URL", "http://channels:8000")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL")
EXCHANGE = "chat"
# Campos internos que nunca salen por la API.
PUBLIC = {"_id": 0, "outbox": 0}


class MessageCreate(BaseModel):
    author_id: str
    content: str = Field(min_length=1, max_length=4_000)


class MessageUpdate(BaseModel):
    content: str = Field(min_length=1, max_length=4_000)


def resource_exists(base_url: str, path: str) -> bool:
    try:
        with urlopen(f"{base_url}{path}", timeout=2) as response:
            return response.status == 200
    except HTTPError as error:
        if error.code == 404:
            return False
        raise HTTPException(status_code=502, detail="Dependent service failed")
    except URLError:
        raise HTTPException(status_code=503, detail="Dependent service unavailable")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def snapshot(message: dict) -> dict:
    return {key: value for key, value in message.items()
            if key not in ("_id", "outbox")}


def new_event(event_type: str, message: dict) -> dict:
    """Evento con el estado completo del mensaje (event-carried state).

    `data.version` crece en cada cambio; los consumidores lo usan para
    descartar eventos duplicados o que lleguen fuera de orden.
    """
    event = {"event_id": str(uuid4()), "type": event_type,
             "subject_id": message["id"], "occurred_at": now(),
             "data": snapshot(message)}
    events.append(event)
    return event


def relay_outbox() -> None:
    """Entrega los eventos pendientes al broker y los retira del outbox."""
    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = connection.channel()
            channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic",
                                     durable=True)
            channel.confirm_delivery()
            while True:
                pending = messages.find({"outbox.event_id": {"$exists": True}},
                                        {"id": 1, "outbox": 1}).limit(100)
                delivered = 0
                for message in pending:
                    for event in message["outbox"]:
                        channel.basic_publish(
                            exchange=EXCHANGE, routing_key=event["type"],
                            body=json.dumps(event),
                            properties=pika.BasicProperties(
                                delivery_mode=pika.DeliveryMode.Persistent,
                                content_type="application/json",
                                message_id=event["event_id"]))
                        messages.update_one(
                            {"id": message["id"]},
                            {"$pull": {"outbox": {"event_id": event["event_id"]}}})
                        delivered += 1
                if not delivered:
                    connection.sleep(0.5)
        except Exception:  # broker caído: los eventos esperan en el outbox
            time.sleep(2)


@asynccontextmanager
async def lifespan(_: FastAPI):
    messages.create_index([("id", ASCENDING)], unique=True)
    messages.create_index([("outbox.event_id", ASCENDING)], sparse=True)
    if RABBITMQ_URL:
        threading.Thread(target=relay_outbox, daemon=True).start()
    yield


app = FastAPI(title="Chat Messages", version="1.1.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/v1/channels/{channel_id}/messages", status_code=status.HTTP_201_CREATED)
def create_message(channel_id: str, body: MessageCreate):
    if not resource_exists(CHANNELS_URL, f"/api/v1/channels/{channel_id}"):
        raise HTTPException(status_code=404, detail="Channel not found")
    if not resource_exists(USERS_URL, f"/api/v1/users/{body.author_id}"):
        raise HTTPException(status_code=422, detail="Author does not exist")
    message = {"id": str(uuid4()), "channel_id": channel_id,
               "created_at": now(), "edited_at": None, "version": 1,
               **body.model_dump()}
    message["outbox"] = [new_event("chat.message.created", message)]
    messages.insert_one(message)
    return snapshot(message)


@app.get("/api/v1/channels/{channel_id}/messages")
def list_messages(channel_id: str):
    return list(messages.find({"channel_id": channel_id, "deleted": {"$ne": True}},
                              PUBLIC))


def change_message(channel_id: str, message_id: str, event_type: str,
                   changes: dict) -> dict:
    """Aplica un cambio con control de concurrencia optimista por versión."""
    current = messages.find_one({"id": message_id, "channel_id": channel_id,
                                 "deleted": {"$ne": True}}, PUBLIC)
    if current is None:
        raise HTTPException(status_code=404, detail="Message not found")
    updated = {**current, **changes, "version": current["version"] + 1}
    event = new_event(event_type, updated)
    result = messages.find_one_and_update(
        {"id": message_id, "version": current["version"]},
        {"$set": {**changes, "version": updated["version"]},
         "$push": {"outbox": event}},
        projection=PUBLIC, return_document=ReturnDocument.AFTER)
    if result is None:
        events.remove(event)
        raise HTTPException(status_code=409, detail="Message changed concurrently")
    return result


@app.patch("/api/v1/channels/{channel_id}/messages/{message_id}")
def update_message(channel_id: str, message_id: str, body: MessageUpdate):
    return change_message(channel_id, message_id, "chat.message.updated",
                          {"content": body.content, "edited_at": now()})


@app.delete("/api/v1/channels/{channel_id}/messages/{message_id}",
            status_code=status.HTTP_204_NO_CONTENT)
def delete_message(channel_id: str, message_id: str):
    # Borrado lógico: el documento conserva el outbox hasta publicar el evento.
    change_message(channel_id, message_id, "chat.message.deleted",
                   {"deleted": True, "content": "", "deleted_at": now()})
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/api/v1/events")
def list_events():
    return events
