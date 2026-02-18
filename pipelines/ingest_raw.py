from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
import os

INCOMING_DIR = Path("data/incoming")
ARCHIVE_DIR = Path("data/archive")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_dsn() -> str:
    load_dotenv()  # reads .env from project root
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN is not set. Put it into .env (see README).")
    return dsn


@dataclass
class FileStats:
    rows_in_file: int
    rows_loaded: int
    rows_deduped: int


INSERT_SQL = """
INSERT INTO raw_events (
  event_id, event_time, ingestion_time, event_name, user_id, session_id,
  product_id, price, device, payload
)
VALUES (
  %(event_id)s::uuid,
  %(event_time)s::timestamptz,
  %(ingestion_time)s::timestamptz,
  %(event_name)s,
  %(user_id)s::bigint,
  %(session_id)s::uuid,
  %(product_id)s::bigint,
  %(price)s::numeric,
  %(device)s,
  %(payload)s::jsonb
)
ON CONFLICT (event_id) DO NOTHING;
"""


def load_jsonl_file(conn: psycopg.Connection, path: Path) -> FileStats:
    rows_in_file = 0
    rows_loaded = 0

    with path.open("r", encoding="utf-8") as f:
        with conn.cursor() as cur:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                rows_in_file += 1
                obj: dict[str, Any] = json.loads(line)

                # store payload as jsonb string
                payload = obj.get("payload") or {}
                obj["payload"] = json.dumps(payload, ensure_ascii=False)

                # normalize optionals
                obj["product_id"] = obj.get("product_id")
                obj["price"] = obj.get("price")
                obj["device"] = obj.get("device")

                cur.execute(INSERT_SQL, obj)
                rows_loaded += cur.rowcount

    return FileStats(rows_in_file, rows_loaded, rows_in_file - rows_loaded)


def archive_file(path: Path) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    dest = ARCHIVE_DIR / path.name
    shutil.move(str(path), str(dest))
    return dest


def insert_ingestion_run(
    conn: psycopg.Connection,
    run_id: str,
    file_name: str,
    started_at: datetime,
    finished_at: datetime,
    stats: FileStats,
    status: str,
    error_message: str | None,
) -> None:
    sql = """
    INSERT INTO ingestion_runs (
      run_id, file_name, started_at, finished_at,
      rows_in_file, rows_loaded, rows_deduped,
      status, error_message
    )
    VALUES (
      %(run_id)s::uuid, %(file_name)s, %(started_at)s::timestamptz, %(finished_at)s::timestamptz,
      %(rows_in_file)s::int, %(rows_loaded)s::int, %(rows_deduped)s::int,
      %(status)s, %(error_message)s
    );
    """
    data = {
        "run_id": run_id,
        "file_name": file_name,
        "started_at": started_at,
        "finished_at": finished_at,
        "rows_in_file": stats.rows_in_file,
        "rows_loaded": stats.rows_loaded,
        "rows_deduped": stats.rows_deduped,
        "status": status,
        "error_message": error_message,
    }
    with conn.cursor() as cur:
        cur.execute(sql, data)


def ingest_one_file(dsn: str, path: Path) -> None:
    run_id = str(uuid.uuid4())
    started_at = utc_now()

    try:
        with psycopg.connect(dsn) as conn:
            stats = load_jsonl_file(conn, path)
            finished_at = utc_now()
            insert_ingestion_run(
                conn, run_id, path.name, started_at, finished_at, stats, "success", None
            )
            conn.commit()

        archived = archive_file(path)
        print(
            f"✅ {path.name}: in_file={stats.rows_in_file}, loaded={stats.rows_loaded}, "
            f"deduped={stats.rows_deduped} -> {archived}"
        )
    except Exception as e:
        # if something fails, re-raise (later we'll improve failure logging)
        raise RuntimeError(f"Failed to ingest {path.name}: {e}") from e


def main() -> None:
    dsn = get_dsn()
    files = sorted(INCOMING_DIR.glob("*.jsonl"))
    if not files:
        print("No new files in data/incoming")
        return

    for path in files:
        ingest_one_file(dsn, path)


if __name__ == "__main__":
    main()