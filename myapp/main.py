from fastapi import FastAPI, Request, Query
import logging
import os
import datetime
import uuid
from opensearchpy import OpenSearch
from typing import Optional, List
import sys
import json

# Determine environment: dev or prod
ENV = os.getenv("APP_ENV", "prod").lower()

# Setup log file path
log_path = "/var/log/myapp/app.log"
os.makedirs(os.path.dirname(log_path), exist_ok=True)

# Create handlers
file_handler = logging.FileHandler(log_path)
file_handler.setLevel(logging.INFO)

# JSON formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "@timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage()
        }
        if hasattr(record, "request_id"):
            log_record["request_id"] = record.request_id
        if hasattr(record, "user_id"):
            log_record["user_id"] = record.user_id
        if hasattr(record, "event"):
            log_record["event"] = record.event
        if hasattr(record, "method"):
            log_record["method"] = record.method
        if hasattr(record, "path"):
            log_record["path"] = record.path
        if hasattr(record, "status_code"):
            log_record["status_code"] = record.status_code
        if hasattr(record, "duration_s"):
            log_record["duration_s"] = record.duration_s
        return json.dumps(log_record)

json_formatter = JsonFormatter()
file_handler.setFormatter(json_formatter)

handlers = [file_handler]
if ENV == "dev":
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(json_formatter)
    handlers.append(console_handler)

logger = logging.getLogger("myapp")
logger.setLevel(logging.INFO)
for h in handlers:
    logger.addHandler(h)

logging.getLogger("opensearch").setLevel(logging.WARNING)

# OpenSearch setup
OP_HOST = os.getenv("OPENSEARCH_HOST", "opensearch-node")
OP_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
OP_USER = os.getenv("OPENSEARCH_USER", "admin")
OP_PASS = os.getenv("OPENSEARCH_PASS", "YourStrongPassword123!")
BUSINESS_INDEX = os.getenv("BUSINESS_INDEX", "app-logs-business")
REQUEST_INDEX  = os.getenv("REQUEST_INDEX",  "app-logs-requests")

client = OpenSearch(
    hosts=[{"host": OP_HOST, "port": OP_PORT}],
    http_auth=(OP_USER, OP_PASS),
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

app = FastAPI()

@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    start_time = datetime.datetime.utcnow()
    response = await call_next(request)
    duration = (datetime.datetime.utcnow() - start_time).total_seconds()

    record = logging.LogRecord(
        name="myapp",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg=f"request_id={request_id} method={request.method} path={request.url.path} status={response.status_code} duration={duration}",
        args=(),
        exc_info=None
    )
    record.request_id = request_id
    record.method = request.method
    record.path = request.url.path
    record.status_code = response.status_code
    record.duration_s = duration
    logger.handle(record)

    doc = {
        "@timestamp": start_time.isoformat() + "Z",
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "status_code": response.status_code,
        "duration_s": duration
    }
    try:
        client.index(index=REQUEST_INDEX, body=doc, refresh=True)
    except Exception as e:
        logger.error(f"OpenSearch indexing error (request log): {e}")

    return response

@app.post("/user/{user_id}/login")
async def user_login(user_id: int):
    logger.info(
        f"user_login user_id={user_id}",
        extra={"user_id": user_id, "event": "user_login"}
    )
    doc = {
        "@timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "event": "user_login",
        "user_id": user_id,
        "service": "user-service",
        "message": "User login succeeded"
    }
    try:
        client.index(index=BUSINESS_INDEX, body=doc, refresh=True)
    except Exception as e:
        logger.error(f"OpenSearch indexing error (business log): {e}")

    return {"status": "ok", "user_id": user_id}

@app.get("/logs/business", response_model=List[dict])
def get_business_logs(
    user_id: Optional[int] = None,
    event: Optional[str] = None,
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
    size: int = Query(50, ge=1, le=1000)
):
    must_clauses = []
    if user_id is not None:
        must_clauses.append({"term": {"user_id": user_id}})
    if event is not None:
        must_clauses.append({"term": {"event": event}})
    if start_time is not None or end_time is not None:
        range_clause = {}
        if start_time is not None:
            range_clause["gte"] = start_time.isoformat() + "Z"
        if end_time is not None:
            range_clause["lte"] = end_time.isoformat() + "Z"
        must_clauses.append({"range": {"@timestamp": range_clause}})

    query_body = {
        "query": {"bool": {"must": must_clauses}},
        "sort": [{"@timestamp": {"order": "desc"}}],
        "size": size
    }

    resp = client.search(index=BUSINESS_INDEX, body=query_body)
    hits = resp["hits"]["hits"]
    return [hit["_source"] for hit in hits]

@app.get("/hello")
async def hello():
    return {"message": "Hello, world!"}
