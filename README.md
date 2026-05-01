# GStreamer KVS Stream Controller

FastAPI service that starts and stops **Docker containers** running the AWS **Kinesis Video Streams (KVS)** RTSP producer (`kvs_gstreamer_sample`), tracks stream state in **PostgreSQL**, and can mint **HLS playback URLs** for live viewing.

## What it does

1. **Ingest**: Pulls video from an RTSP URL and pushes it to an Amazon KVS stream whose name matches the app’s **stream id** (see below).
2. **Orchestration**: Uses the Docker API (from the host or from a container with the socket mounted) to run/stop the producer image.
3. **State**: Persists reservations, status, and per-user limits in Postgres (`db.py`).
4. **Playback**: Calls the Kinesis Video **Archived Media** API to obtain a short-lived **HLS** session URL for a given stream name.

## Repository layout

| File | Role |
|------|------|
| `app.py` | FastAPI app: HTTP routes, request models, CORS, lifespan hook that starts the background worker manager. |
| `streams.py` | Core logic: Docker runs, queue-based worker, single-stream vs batch worker modes, HLS URL helper. |
| `db.py` | PostgreSQL access: schema init, `reserve_stream_slot`, status updates, listing active streams per user. |
| `docker-compose.yml` | Builds the RTSP/KVS image (`rtspdockertest`), Postgres, and the FastAPI service. |
| `Dockerfile` | Python 3.11 image for the API (`uvicorn` on port 8001). |
| `requirements.txt` | `fastapi`, `uvicorn`, `docker`, `boto3`, `pydantic`, `psycopg2-binary`. |

The producer image is built from `amazon-kinesis-video-streams-demos/producer-cpp/docker-rtsp` and tagged as **`rtspdockertest`** (see `streams.py` constant `IMAGE`).

## Stream naming (KVS + Docker)

Logical streams are scoped by **`user_id`** and **`stream_name`**. The service combines them into a single id:

`stream_id = "{user_id}__{stream_name}"`

That string is:

- The **Docker container name** (single-stream path) or part of the batch worker’s process list.
- The **Kinesis Video stream name** your producer must publish to and that HLS uses.

`user_id` is normalized to lowercase in API models.

## Two ways to run producers

1. **`POST /streams/start`** (via `start_stream`): After reserving a DB slot, starts **one container** named `stream_id` running `./kvs_gstreamer_sample <stream_id> <rtsp_url>`.
2. **`POST /streams/start/batch`** (via `start_stream_batch`): Reserves slots for up to **20** cameras, then starts or replaces a **single worker container** per user named `worker__{user_id}`, whose entrypoint runs **multiple** `kvs_gstreamer_sample` processes in one shell (`sh -c '... & ... & wait'`).

RTSP URLs must not be double-booked: the code and DB block starting a second active stream with the same RTSP URL (unless it’s the same stream being restarted).

## Background worker (`StreamWorkerManager`)

On startup, `app` lifespan calls `manager.start()`, which:

- Initializes the DB (with retries).
- Runs an asyncio loop that consumes `StreamJob` items from a queue (max 100), with **`WORKER_CONCURRENCY`** parallel jobs (default 4).

Single start/stop requests submit jobs and wait on a future; queue overload returns **503**.

## HTTP API (`app.py`)

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/streams/start` | Body: `user_id`, `stream_name`, `rtsp_url`. Queue start job. |
| `POST` | `/streams/start/batch` | Body: `user_id`, `streams[]` (each `stream_name`, `rtsp_url`). Max 20 streams. |
| `POST` | `/streams/stop/{stream_name}` | Query: `user_id`. Stops the container for `user_id__stream_name`. |
| `GET` | `/streams` | Lists **all** Docker container names (`docker.containers.list(all=True)`). |
| `POST` | `/streams/hls` | Body: `user_id`, `stream_name`. Returns `stream_name` (full id) and `hls_url` from AWS. |

HLS uses `PlaybackMode=LIVE`, `ContainerFormat=FRAGMENTED_MP4`, and a long `Expires` (e.g. 43200 seconds). Errors map AWS codes to **404** (unknown stream), **409** (not ready), **403** (access denied), etc.

## Database (`db.py`)

- **`stream_sessions`**: `stream_id` (PK), `user_id`, `stream_name`, `rtsp_url`, `status`, `error`, timestamps. Unique `(user_id, stream_name)`.
- **`reserve_stream_slot`**: Advisory lock per `user_id`, checks duplicate name, RTSP conflict, and **`MAX_STREAMS_PER_USER`** (20). Inserts/updates row with status `STARTING` when reserved.
- Status transitions include e.g. `RUNNING`, `FAILED`, `STOPPING`, `STOPPED`.

Default `DATABASE_URL` in code points at `postgres:5432` for Docker Compose; override with env.

## Configuration

- **Docker**: On Windows, `docker` Python client uses `npipe:////./pipe/docker_engine`; on Unix, `/var/run/docker.sock`.
- **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` are passed into producer containers and used for `boto3` HLS calls. Region for HLS client defaults from `AWS_DEFAULT_REGION` (with a fallback in module constants).
- **Postgres**: `DATABASE_URL` (Compose wires this from `.env`-style variables).

## Running with Docker Compose

Compose builds the KVS RTSP image, starts Postgres (healthcheck), then the FastAPI service with the Docker socket and AWS/DB env vars. Ensure `.env` (or your environment) defines `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DATABASE_URL`, and AWS credentials as referenced in `docker-compose.yml`.

Local run (without Compose) still requires Docker running, Postgres reachable, and the `rtspdockertest` image present.

## Security notes

- CORS is open (`allow_origins=["*"]`) — tighten for production.
- The API controls Docker on the host; protect deployment and network access.
- Long-lived AWS keys in container env are convenient for demos; production often prefers IAM roles or task roles.

## Dependencies

See `requirements.txt`. Producer behavior and KVS setup follow AWS Kinesis Video Streams documentation for the **producer C++ / RTSP** sample in this repo’s `amazon-kinesis-video-streams-demos` tree.
