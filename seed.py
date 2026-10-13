"""Crea usuarios, canales y mensajes de ejemplo usando solo las APIs públicas.

Uso (con el stack levantado):  python3 seed.py
Solo usa la librería estándar. Pasa por el gateway web (puerto 8080).
"""
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE = os.environ.get("BASE_URL", "http://localhost:8080/api/v1")

USERS = ["ana", "beto", "carla", "diego", "elena"]
CHANNELS = ["general", "arquitectura", "random"]
# (canal, autor, contenido). {nombre} se reemplaza por una mención <@id>.
MESSAGES = [
    ("general", "ana", "¡Hola a todos! Bienvenidos al chat del curso"),
    ("general", "beto", "Hola {ana}, ¿cuándo es la próxima reunión?"),
    ("general", "ana", "La reunión es el jueves a las 10:00"),
    ("general", "carla", "Dejo el calendario aquí: https://calendar.example.com/curso"),
    ("general", "diego", "Gracias {carla}, justo lo estaba buscando"),
    ("general", "elena", "¿Alguien tiene los apuntes de la clase de microservicios?"),
    ("general", "beto", "{elena} están en https://drive.example.com/apuntes-u3"),
    ("arquitectura", "carla", "¿Usamos RabbitMQ o Kafka para los eventos de mensajería?"),
    ("arquitectura", "diego", "RabbitMQ alcanza para este volumen; Kafka si queremos replay de eventos"),
    ("arquitectura", "ana", "Discord indexa los mensajes en Elasticsearch de forma asíncrona y por lotes"),
    ("arquitectura", "beto", "El artículo: https://discord.com/blog/how-discord-indexes-billions-of-messages"),
    ("arquitectura", "elena", "Ojo con la consistencia eventual: la búsqueda puede ir unos segundos atrasada"),
    ("arquitectura", "carla", "{ana} {diego} ¿revisamos el patrón outbox en la reunión del jueves?"),
    ("arquitectura", "diego", "Sí, y también la idempotencia del consumidor con versiones externas"),
    ("arquitectura", "ana", "Propongo enrutar por canal en Elasticsearch, como Discord enruta por servidor"),
    ("random", "elena", "¿Alguien quiere pizza para la reunión?"),
    ("random", "beto", "Yo me apunto 🍕"),
    ("random", "diego", "Miren este meme de microservicios https://example.com/memes/microservicios.png"),
    ("random", "carla", "{beto} jajaja tremendo"),
    ("random", "ana", "Recuerden que el viernes no hay clases"),
]


def call(method: str, path: str, body: dict | None = None):
    request = Request(f"{BASE}{path}", method=method,
                      data=json.dumps(body).encode() if body else None,
                      headers={"content-type": "application/json"})
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def found() -> int:
    # Mientras el índice no existe la API responde 202 sin resultados.
    return call("GET", "/search/messages?content=reunion&limit=1").get("total_results", 0)


def wait_until_ready() -> None:
    for _ in range(60):
        try:
            call("GET", "/users")
            call("GET", "/channels")
            return
        except (URLError, HTTPError, ConnectionError):
            time.sleep(2)
    sys.exit("Los servicios no responden. ¿Corriste `docker compose up -d --build`?")


def main() -> None:
    wait_until_ready()
    users = {u["display_name"]: u for u in call("GET", "/users")}
    for name in USERS:
        if name not in users:
            users[name] = call("POST", "/users", {"display_name": name})
    channels = {c["name"]: c for c in call("GET", "/channels")}
    for name in CHANNELS:
        if name not in channels:
            channels[name] = call("POST", "/channels", {"name": name})
    print(f"{len(USERS)} usuarios y {len(CHANNELS)} canales listos")

    mentions = {name: f"<@{user['id']}>" for name, user in users.items()}
    for channel, author, content in MESSAGES:
        call("POST", f"/channels/{channels[channel]['id']}/messages",
             {"author_id": users[author]["id"], "content": content.format(**mentions)})
    print(f"{len(MESSAGES)} mensajes creados")

    # Consistencia eventual: los mensajes ya existen, pero el índice tarda un poco.
    started = time.monotonic()
    print(f"Buscando 'reunion' de inmediato: {found()} resultados")
    while found() < 4 and time.monotonic() - started < 60:
        time.sleep(0.2)
    print(f"Después de {time.monotonic() - started:.1f}s: {found()} resultados")
    print("Abre http://localhost:8080")


if __name__ == "__main__":
    main()
