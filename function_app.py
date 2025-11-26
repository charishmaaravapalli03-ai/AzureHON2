# function_app.py
import os
import uuid
import json
import logging
import datetime
from typing import Iterator

import azure.functions as func
from azure.storage.blob import ContainerClient, BlobClient
import pyodbc

# Configuration (read from local.settings.json or Function App settings)
SQL_CONN_STR = os.getenv("SQL_CONN_STR", "")  # ODBC connection string
STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
ARCHIVE_CONTAINER = os.getenv("ARCHIVE_CONTAINER", "archive")
ARCHIVE_PREFIX = os.getenv("ARCHIVE_PREFIX", "orders")
ARCHIVE_BATCH_SIZE = int(os.getenv("ARCHIVE_BATCH_SIZE", "1000"))
STAGING_FETCH_BATCH = int(os.getenv("STAGING_FETCH_BATCH", "500"))

app = func.FunctionApp()

def get_db_conn():
    if not SQL_CONN_STR:
        raise RuntimeError("SQL_CONN_STR application setting is not set.")
    # pyodbc connection used for Azure SQL
    return pyodbc.connect(SQL_CONN_STR, autocommit=False)

def make_blob_name(ts: datetime.datetime) -> str:
    y = ts.strftime("%Y")
    m = ts.strftime("%m")
    d = ts.strftime("%d")
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    return f"{ARCHIVE_PREFIX}/{y}/{m}/{d}/orders{stamp}.ndjson"

def move_batches_to_staging(conn, run_id: uuid.UUID, batch_size: int) -> int:
    total = 0
    cur = conn.cursor()
    while True:
        # Adjust the columns in OUTPUT/ArchiveStaging and WHERE clause to match your schema.
        sql = f"""
        DELETE TOP ({batch_size})
        FROM Orders
        OUTPUT DELETED.id, DELETED.order_number, DELETED.customer_id, DELETED.created_at, ? 
        INTO ArchiveStaging (id, order_number, customer_id, created_at, run_id)
        WHERE created_at < DATEADD(day, -30, SYSUTCDATETIME());
        """
        cur.execute(sql, str(run_id))
        # rowcount is number of rows affected by the DELETE batch
        rows = cur.rowcount
        conn.commit()
        if not rows or rows == 0:
            break
        total += rows
    return total

def staging_row_generator(conn, run_id: uuid.UUID, fetch_batch: int = 500) -> Iterator[bytes]:
    """
    Yield NDJSON lines as bytes. Uses fetchmany to avoid loading everything into memory.
    Adjust SELECT column list to match ArchiveStaging schema.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT id, order_number, customer_id, created_at
        FROM ArchiveStaging
        WHERE run_id = ?
        ORDER BY id;
    """, str(run_id))

    while True:
        rows = cur.fetchmany(fetch_batch)
        if not rows:
            break
        for r in rows:
            obj = {
                "id": int(r[0]) if r[0] is not None else None,
                "order_number": r[1],
                "customer_id": int(r[2]) if r[2] is not None else None,
                "created_at": r[3].isoformat() if r[3] else None
            }
            yield (json.dumps(obj, default=str) + "\n").encode("utf-8")

def delete_staging_rows(conn, run_id: uuid.UUID) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM ArchiveStaging WHERE run_id = ?;", str(run_id))
    deleted = cur.rowcount
    conn.commit()
    return deleted

def ensure_container(container_name: str) -> ContainerClient:
    if not STORAGE_CONNECTION_STRING:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING application setting is not set.")
    container_client = ContainerClient.from_connection_string(STORAGE_CONNECTION_STRING, container_name=container_name)
    try:
        container_client.create_container()
    except Exception:
        # container probably exists
        pass
    return container_client

def upload_blob_from_generator(blob_client: BlobClient, generator: Iterator[bytes]):
    # The SDK supports passing an iterator of bytes (in newer versions). If it errors locally,
    # fallback to concatenating small chunks or using upload_blob with streaming approach.
    blob_client.upload_blob(data=generator, overwrite=True)

@app.schedule(schedule="0 0 2 * * *", arg_name="timer", run_on_startup=False)
def archive_orders_schedule(timer: func.TimerRequest) -> None:
    utc_now = datetime.datetime.utcnow()
    run_id = uuid.uuid4()
    logging.info(f"[ArchiveOrdersTimer] Starting archive run {run_id} at {utc_now.isoformat()}")

    try:
        conn = get_db_conn()
    except Exception as e:
        logging.exception("Failed to open DB connection.")
        return

    total_moved = 0
    try:
        logging.info("Moving rows from Orders -> ArchiveStaging (batched).")
        total_moved = move_batches_to_staging(conn, run_id, ARCHIVE_BATCH_SIZE)
        logging.info(f"Rows moved to staging: {total_moved}")

        if total_moved == 0:
            logging.info("No eligible rows older than 30 days. Nothing to archive.")
            conn.close()
            return

        # Prepare blob client and stream generator
        container_client = ensure_container(ARCHIVE_CONTAINER)
        blob_name = make_blob_name(utc_now)
        blob_client = container_client.get_blob_client(blob_name)
        logging.info(f"Uploading to blob: {blob_name}")

        gen = staging_row_generator(conn, run_id, STAGING_FETCH_BATCH)
        upload_blob_from_generator(blob_client, gen)
        logging.info("Blob upload complete.")

        # Delete staging rows for this run
        deleted = delete_staging_rows(conn, run_id)
        logging.info(f"Deleted {deleted} rows from ArchiveStaging for run {run_id}.")
        logging.info(f"Archived {total_moved} rows to blob: {blob_client.url}")

    except Exception as e:
        logging.exception("Archive run failed. Staging preserved for inspection/retry.")
    finally:
        try:
            conn.close()
        except Exception:
            pass