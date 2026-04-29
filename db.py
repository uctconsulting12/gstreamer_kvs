import os
import time
from contextlib import contextmanager
import psycopg2
from psycopg2 import OperationalError

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@postgres:5432/streamdb",
)


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    last_error = None

    for _ in range(15):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS stream_sessions (
                            stream_id TEXT PRIMARY KEY,
                            user_id TEXT NOT NULL,
                            stream_name TEXT NOT NULL,
                            rtsp_url TEXT NOT NULL,
                            status TEXT NOT NULL,
                            error TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            UNIQUE (user_id, stream_name)
                        );
                        """
                    )

                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_stream_sessions_user_status
                        ON stream_sessions(user_id, status);
                        """
                    )

                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_stream_sessions_rtsp_status
                        ON stream_sessions(rtsp_url, status);
                        """
                    )

                conn.commit()
                return

        except OperationalError as exc:
            last_error = exc
            time.sleep(2)

    if last_error:
        raise last_error


def reserve_stream_slot(
    *,
    user_id: str,
    stream_name: str,
    stream_id: str,
    rtsp_url: str,
    max_streams_per_user: int,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s));",
                (user_id,),
            )

            cur.execute(
                """
                SELECT stream_id, status
                FROM stream_sessions
                WHERE user_id = %s AND stream_name = %s
                FOR UPDATE;
                """,
                (user_id, stream_name),
            )

            existing = cur.fetchone()
            if existing and existing[1] in ("STARTING", "RUNNING"):
                conn.commit()
                return {"status": "already_running", "stream": existing[0]}

            cur.execute(
                """
                SELECT stream_id
                FROM stream_sessions
                WHERE rtsp_url = %s
                AND status IN ('STARTING', 'RUNNING')
                AND stream_id <> %s
                LIMIT 1;
                """,
                (rtsp_url, stream_id),
            )

            rtsp_owner = cur.fetchone()
            if rtsp_owner:
                conn.commit()
                return {
                    "status": "blocked",
                    "message": f"RTSP already in use by stream '{rtsp_owner[0]}'",
                }

            cur.execute(
                """
                SELECT COUNT(*)
                FROM stream_sessions
                WHERE user_id = %s
                AND status IN ('STARTING', 'RUNNING');
                """,
                (user_id,),
            )

            active_count = cur.fetchone()[0]
            if active_count >= max_streams_per_user:
                conn.commit()
                return {
                    "status": "blocked",
                    "message": f"User limit reached ({max_streams_per_user})",
                }

            cur.execute(
                """
                INSERT INTO stream_sessions
                (stream_id, user_id, stream_name, rtsp_url, status, error, updated_at)
                VALUES (%s, %s, %s, %s, 'STARTING', NULL, NOW())
                ON CONFLICT (stream_id)
                DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    stream_name = EXCLUDED.stream_name,
                    rtsp_url = EXCLUDED.rtsp_url,
                    status = 'STARTING',
                    error = NULL,
                    updated_at = NOW();
                """,
                (stream_id, user_id, stream_name, rtsp_url),
            )

        conn.commit()
        return {"status": "reserved"}


def set_stream_status(
    stream_id: str, status: str, error: str | None = None
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE stream_sessions
                SET status = %s,
                    error = %s,
                    updated_at = NOW()
                WHERE stream_id = %s;
                """,
                (status, error, stream_id),
            )
        conn.commit()


def get_active_streams_for_user(user_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT stream_id, rtsp_url
                FROM stream_sessions
                WHERE user_id = %s
                AND status IN ('STARTING', 'RUNNING')
                ORDER BY stream_name;
                """,
                (user_id,),
            )
            return cur.fetchall()