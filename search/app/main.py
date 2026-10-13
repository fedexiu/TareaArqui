"""API de consulta de mensajes, inspirada en la búsqueda de Discord.

Solo lee de Elasticsearch: nunca llama a `messages` ni a su base de datos. Los
resultados son eventualmente consistentes; un mensaje recién creado aparece
cuando el indexador procesa su evento (normalmente en ~1-2 s).
"""
import json
import os
from base64 import b64encode
from datetime import datetime
from enum import Enum
from typing import Annotated
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from elasticsearch import ApiError, NotFoundError, TransportError
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.index import ALIAS, client

app = FastAPI(title="Chat Search", version="1.0.0")
es = client()
RABBITMQ_API_URL = os.environ.get("RABBITMQ_API_URL", "http://rabbitmq:15672")
RABBITMQ_CREDENTIALS = os.environ.get("RABBITMQ_CREDENTIALS", "chat:chat")
MAX_LIMIT = 25        # mismos límites que la API de Discord
MAX_OFFSET = 9_975


class Has(str, Enum):
    link = "link"
    mention = "mention"


class SortBy(str, Enum):
    timestamp = "timestamp"
    relevance = "relevance"


class SortOrder(str, Enum):
    desc = "desc"
    asc = "asc"


def index_not_ready() -> JSONResponse:
    # Discord responde 202 mientras el índice todavía se está construyendo.
    return JSONResponse(status_code=202, headers={"Retry-After": "2"}, content={
        "message": "Index not yet available. Try again later",
        "code": 110000, "retry_after": 2})


def search(channel_ids: list[str], content: str | None, author_ids: list[str],
           mentions: list[str], has: list[Has], min_date: datetime | None,
           max_date: datetime | None, sort_by: SortBy, sort_order: SortOrder,
           limit: int, offset: int):
    filters: list[dict] = [{"term": {"deleted": False}}]
    if channel_ids:
        filters.append({"terms": {"channel_id": channel_ids}})
    if author_ids:
        filters.append({"terms": {"author_id": author_ids}})
    for mentioned in mentions:  # todos los mencionados deben aparecer
        filters.append({"term": {"mentions": mentioned}})
    for feature in has:
        filters.append({"term": {"has": feature.value}})
    if min_date or max_date:
        filters.append({"range": {"created_at": {
            key: value.isoformat() for key, value in
            (("gte", min_date), ("lte", max_date)) if value}}})
    must = ([{"match": {"content": {"query": content, "operator": "and"}}}]
            if content else [])
    by_date = {"created_at": {"order": sort_order.value}}
    sort = (["_score", by_date] if sort_by is SortBy.relevance and content
            else [by_date])
    try:
        response = es.search(
            index=ALIAS,
            # Enrutar por canal consulta solo los shards que lo contienen,
            # igual que Discord enruta las búsquedas de un servidor.
            routing=",".join(channel_ids) or None,
            query={"bool": {"must": must, "filter": filters}},
            sort=sort, from_=offset, size=limit, track_total_hits=True,
            highlight={"fields": {"content": {}},
                       "pre_tags": ["**"], "post_tags": ["**"]})
    except NotFoundError:
        return index_not_ready()
    except (ApiError, TransportError):
        raise HTTPException(status_code=503, detail="Search unavailable")
    hits = response["hits"]
    return {
        "total_results": hits["total"]["value"],
        "messages": [{**{key: value for key, value in hit["_source"].items()
                         if key not in ("deleted", "indexed_at")},
                      "hit": True,
                      "highlights": hit.get("highlight", {}).get("content", [])}
                     for hit in hits["hits"]],
    }


ContentQuery = Annotated[str | None, Query(max_length=1024,
                                           description="Texto a buscar")]
AuthorQuery = Annotated[list[str], Query(description="Filtrar por autor(es)")]
MentionsQuery = Annotated[list[str], Query(description="Usuarios mencionados con <@id>")]
HasQuery = Annotated[list[Has], Query(description="El mensaje contiene...")]
LimitQuery = Annotated[int, Query(ge=1, le=MAX_LIMIT)]
OffsetQuery = Annotated[int, Query(ge=0, le=MAX_OFFSET)]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/search/messages")
def search_all(content: ContentQuery = None, author_id: AuthorQuery = [],
               channel_id: Annotated[list[str], Query()] = [],
               mentions: MentionsQuery = [], has: HasQuery = [],
               min_date: datetime | None = None, max_date: datetime | None = None,
               sort_by: SortBy = SortBy.timestamp,
               sort_order: SortOrder = SortOrder.desc,
               limit: LimitQuery = MAX_LIMIT, offset: OffsetQuery = 0):
    """Busca en todos los canales (equivalente a la búsqueda de un servidor)."""
    return search(channel_id, content, author_id, mentions, has, min_date,
                  max_date, sort_by, sort_order, limit, offset)


@app.get("/api/v1/channels/{channel_id}/messages/search")
def search_channel(channel_id: str, content: ContentQuery = None,
                   author_id: AuthorQuery = [], mentions: MentionsQuery = [],
                   has: HasQuery = [], min_date: datetime | None = None,
                   max_date: datetime | None = None,
                   sort_by: SortBy = SortBy.timestamp,
                   sort_order: SortOrder = SortOrder.desc,
                   limit: LimitQuery = MAX_LIMIT, offset: OffsetQuery = 0):
    """Busca dentro de un canal."""
    return search([channel_id], content, author_id, mentions, has, min_date,
                  max_date, sort_by, sort_order, limit, offset)


def queue_depth(name: str) -> int | None:
    request = Request(f"{RABBITMQ_API_URL}/api/queues/%2F/{quote(name)}", headers={
        "Authorization": "Basic " + b64encode(RABBITMQ_CREDENTIALS.encode()).decode()})
    try:
        with urlopen(request, timeout=2) as response:
            return json.load(response).get("messages", 0)
    except (URLError, ValueError):
        return None


@app.get("/api/v1/search/status")
def search_status():
    """Muestra qué tan atrasado está el índice respecto a los eventos."""
    try:
        documents = es.count(index=ALIAS)["count"]
    except (ApiError, TransportError):  # sin índice o Elasticsearch caído
        documents = None
    return {"indexed_documents": documents,
            "pending_events": queue_depth("search.indexer"),
            "dead_letter_events": queue_depth("search.indexer.dlq")}
