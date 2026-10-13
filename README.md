# Chat en microservicios + búsqueda con Elasticsearch

Base de la Unidad 3 (`03_Comunicación/demo`) extendida con una capacidad de
**búsqueda de mensajes** desacoplada y con **consistencia eventual**, siguiendo
el diseño que Discord describe en *How Discord Indexes Billions of Messages*.

## Cómo correrlo

Requisitos: Docker con Docker Compose, y Python 3.10+ para el script de datos
(solo usa la librería estándar). Elasticsearch necesita ~1 GB de RAM libre.

```bash
cd Tarea
docker compose up -d --build   # 1. levanta todo (la primera vez tarda en descargar imágenes)
python3 seed.py                # 2. crea usuarios, canales y mensajes de ejemplo
```

3. Abre la interfaz en **http://localhost:8080**.

| Para… | Comando |
| --- | --- |
| Ver los logs del indexador | `docker compose logs -f search-indexer` |
| Apagar | `docker compose down` |
| Apagar y borrar todos los datos | `docker compose down -v` |

`seed.py` crea los usuarios `ana`, `beto`, `carla`, `diego` y `elena`, los
canales `general`, `arquitectura` y `random`, y 20 mensajes con menciones y
links. Al final muestra la consistencia eventual: una búsqueda hecha justo
después de crear los mensajes devuelve 0 resultados, y ~1 s después devuelve
los 4 esperados. Si lo corres otra vez, reutiliza los usuarios y canales que
ya existen pero vuelve a crear los mensajes.

### Interfaz

`web/index.html` es una página estática (HTML + JS, sin dependencias) servida
por nginx. El mismo nginx hace de gateway: enruta cada `/api/v1/...` al
microservicio dueño de la ruta (`web/nginx.conf`), así que no hace falta CORS.

* Arriba se elige con qué usuario escribir o se crea uno nuevo.
* A la izquierda están los canales, y se pueden crear nuevos.
* En el centro se envían mensajes. `@nombre` se convierte en una mención
  `<@id>`. Los mensajes propios se pueden editar y borrar.
* A la derecha se busca por texto, autor, canal, mención, `has: link/mention`
  y orden. Al hacer clic en un resultado se abre su canal. El indicador de
  abajo muestra los documentos indexados, los eventos pendientes y la DLQ.

Para ver la consistencia eventual, envía un mensaje y búscalo de inmediato:
al principio no aparece y a los ~1–2 s sí. Con `docker compose stop
elasticsearch`, el chat sigue funcionando, la búsqueda muestra "no
disponible" y los eventos se acumulan como pendientes. Con `docker compose
start elasticsearch`, se indexan y el contador vuelve a 0.

## Servicios

| Servicio | Puerto | Responsabilidad |
| --- | --- | --- |
| `web` | 8080 | Interfaz y gateway hacia las APIs |
| `users` | 8001 | Crear y consultar usuarios |
| `channels` | 8002 | Crear y consultar canales |
| `messages` | 8003 | Crear, listar, editar y borrar mensajes |
| `search` | 8004 | API de consulta (solo lectura sobre Elasticsearch) |
| `search-indexer` | — | Consume eventos y los indexa en Elasticsearch |
| `rabbitmq` | 15672 | Broker de eventos (UI: `chat` / `chat`) |
| `elasticsearch` | 9200 | Índice de búsqueda |

## Arquitectura

```
            POST/PATCH/DELETE
 cliente ──────────────────────► messages ──► MongoDB (mensaje + outbox, 1 escritura atómica)
                                    │
                                    │ relay del outbox (al menos una vez)
                                    ▼
                         RabbitMQ  exchange "chat" (topic)
                                    │ chat.message.*
                                    ▼
                         cola "search.indexer" (quorum, durable) ──► DLQ
                                    │
                                    ▼
                         search-indexer ── _bulk ──► Elasticsearch (alias "messages")
                                                         ▲
 cliente ── GET /search ──► search (API de query) ───────┘
```

* **Desacople.** `search` no llama a `messages` ni lee su base de datos. Todo
  lo que necesita viaja en el evento, que lleva el estado completo del mensaje
  (*event-carried state transfer*). Si `search` o Elasticsearch caen, el chat
  sigue funcionando; la búsqueda se pone al día cuando vuelven.
* **Sin pérdida de eventos.** `messages` guarda el evento en un *outbox*
  embebido en el mismo documento del mensaje (una sola escritura atómica, sin
  necesidad de transacciones). Un hilo *relay* lo publica con *publisher
  confirms* y luego lo retira del outbox. La cola y su binding se crean al
  arrancar RabbitMQ (`rabbitmq/definitions.json`), así que no se pierden
  eventos publicados antes de que el indexador arranque.
* **Consistencia eventual.** El indexador acumula hasta `BATCH_SIZE` eventos o
  espera `FLUSH_SECONDS` y los manda en un solo `_bulk`. Elasticsearch los
  vuelve visibles al siguiente `refresh` (1 s). Lo normal es que un mensaje
  aparezca en la búsqueda 1 o 2 s después de crearse.
* **Idempotencia y orden.** Cada cambio incrementa `version`, y el indexador
  escribe con `version_type=external`. Un evento duplicado (por la entrega al
  menos una vez) o atrasado devuelve 409 y se descarta, así que el índice
  siempre converge a la última versión.
* **Fallos.** Un evento solo se confirma con *ack* cuando Elasticsearch lo
  aceptó. Si Elasticsearch no responde o devuelve 429/5xx, el evento se
  reencola. Si el evento es inválido o tiene un error de mapping, va a la DLQ
  (`search.indexer.dlq`).

### Paralelo con Discord

| Discord | Esta implementación |
| --- | --- |
| Los mensajes se escriben en Cassandra; el índice es una proyección aparte | `messages`/MongoDB es la fuente de verdad; Elasticsearch es una proyección reconstruible |
| Cola de mensajes por indexar y workers que indexan por lotes (*bulk*) | RabbitMQ + `search-indexer` con `_bulk` |
| Un índice se enruta por *guild* | `routing = channel_id`: la búsqueda de un canal consulta un solo shard |
| Extrae al indexar `has: link/embed/file`, `mentions`, etc. | Extrae `has: link/mention`, `mentions` (`<@user_id>`) y `links` |
| `GET /guilds/{id}/messages/search` y `/channels/{id}/messages/search` | `GET /api/v1/search/messages` y `/api/v1/channels/{id}/messages/search` |
| `limit` ≤ 25, `offset` ≤ 9975, `sort_by=timestamp\|relevance` | Los mismos límites y parámetros |
| `202 Index not yet available` (código 110000) con `retry_after` | Lo mismo si el índice todavía no existe |
| Índices versionados con alias para reindexar | Índice `messages-v1` detrás del alias `messages` |

## API de búsqueda

`GET /api/v1/search/messages`: busca en todos los canales.
`GET /api/v1/channels/{channel_id}/messages/search`: busca dentro de un canal.

| Parámetro | Descripción |
| --- | --- |
| `content` | Texto (todas las palabras; no distingue mayúsculas ni tildes) |
| `author_id` | Uno o más autores (`?author_id=a&author_id=b`) |
| `channel_id` | Uno o más canales (solo en la búsqueda global) |
| `mentions` | Usuarios mencionados con `<@id>` |
| `has` | `link`, `mention` |
| `min_date`, `max_date` | Rango de `created_at` (ISO 8601) |
| `sort_by` | `timestamp` (por defecto) o `relevance` |
| `sort_order` | `desc` (por defecto) o `asc` |
| `limit`, `offset` | Paginación (1–25, 0–9975) |

```json
{
  "total_results": 1,
  "messages": [{
    "id": "…", "channel_id": "…", "author_id": "…",
    "content": "Reunión de arquitectura, ver https://example.com",
    "mentions": [], "links": ["https://example.com"], "has": ["link"],
    "created_at": "…", "edited_at": null, "version": 1,
    "hit": true, "highlights": ["**Reunión** de arquitectura, …"]
  }]
}
```

`GET /api/v1/search/status` muestra el retraso del índice: documentos
indexados, eventos pendientes en la cola y eventos en la DLQ.

## Cambios a la base

* `messages`: agrega `PATCH` y `DELETE /api/v1/channels/{cid}/messages/{mid}`
  (el borrado es lógico), el campo `version`, el outbox y los eventos
  `chat.message.updated` y `chat.message.deleted`.
* `users`: agrega `GET /api/v1/users` (la interfaz lo usa para listar usuarios).
* `users` y `channels`: corrige un error de la base. `insert_one` agregaba
  `_id` (un ObjectId) al dict que luego se devolvía, y el `POST` respondía 500.

## Probar la consistencia eventual con curl

```bash
U=$(curl -s -XPOST localhost:8001/api/v1/users -H 'content-type: application/json' -d '{"display_name":"ana"}' | jq -r .id)
C=$(curl -s -XPOST localhost:8002/api/v1/channels -H 'content-type: application/json' -d '{"name":"general"}' | jq -r .id)
curl -s -XPOST localhost:8003/api/v1/channels/$C/messages -H 'content-type: application/json' \
     -d "{\"author_id\":\"$U\",\"content\":\"Reunión de arquitectura https://example.com\"}"
curl -s "localhost:8004/api/v1/search/messages?content=reunion" | jq .total_results  # 0 (aún no indexado)
sleep 2
curl -s "localhost:8004/api/v1/search/messages?content=reunion&has=link" | jq .total_results  # 1

# Tolerancia a fallos: con Elasticsearch caído se pueden seguir creando mensajes
docker compose stop elasticsearch     # crear mensajes: responden 201; la búsqueda responde 503
docker compose start elasticsearch    # el indexador procesa los eventos pendientes
```

## Limitaciones conocidas

* No hay *backfill*. Los mensajes creados antes de que existiera la cola no
  se indexan. Discord resuelve esto con jobs de indexado histórico; aquí
  haría falta que `messages` expusiera un endpoint de exportación paginado.
* Los resultados no traen mensajes de contexto alrededor del hit, como hace
  Discord. Para obtenerlos, el cliente consulta `messages`.
* No hay *stemming*: buscar "reunión" no encuentra "reuniones".
