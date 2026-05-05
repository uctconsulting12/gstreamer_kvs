import os
import shlex
import asyncio
import boto3
import docker

from dataclasses import dataclass
from typing import Literal
from fastapi import HTTPException
from botocore.exceptions import BotoCoreError, ClientError
from docker.errors import NotFound, DockerException

from db import (
    init_db,
    reserve_stream_slot,
    set_stream_status,
    get_active_streams_for_user,
)

IMAGE = "rtspdockertest"
MAX_STREAMS_PER_USER = 20
WORKER_CONCURRENCY = 4
JOB_TIMEOUT_SECONDS = 45

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# kvs_gstreamer_sample forwards these to kvssink. fragment-duration is ms;
# buffer-duration is seconds — keep it >= fragment length for stable ingest.
KVS_GSTREAMER_EXTRA_ARGS: tuple[str, ...] = (
    "fragment-duration=10000",
    "buffer-duration=10",
)


def _kvs_gstreamer_shell_invocation(stream_id: str, rtsp_url: str) -> str:
    parts = ["./kvs_gstreamer_sample", stream_id, rtsp_url, *KVS_GSTREAMER_EXTRA_ARGS]
    return " ".join(shlex.quote(p) for p in parts)


# Docker client
if os.name == "nt":
    client = docker.DockerClient(base_url="npipe:////./pipe/docker_engine")
else:
    client = docker.DockerClient(base_url="unix:///var/run/docker.sock")


# Locks
_stream_locks: dict[str, asyncio.Lock] = {}
_user_worker_locks: dict[str, asyncio.Lock] = {}


def _get_lock(stream_name: str) -> asyncio.Lock:
    if stream_name not in _stream_locks:
        _stream_locks[stream_name] = asyncio.Lock()
    return _stream_locks[stream_name]


def _build_stream_id(user_id: str, stream_name: str) -> str:
    return f"{user_id}__{stream_name}"


def _build_worker_name(user_id: str) -> str:
    return f"worker__{user_id}"


def _get_user_worker_lock(user_id: str) -> asyncio.Lock:
    if user_id not in _user_worker_locks:
        _user_worker_locks[user_id] = asyncio.Lock()
    return _user_worker_locks[user_id]

# Find RTSP container
def _find_rtsp_container(rtsp_url: str):
    """Returns (container_name, container) if RTSP is already used"""
    for c in client.containers.list():
        try:
            cmd = c.attrs["Config"]["Cmd"] or []
            if rtsp_url in cmd:
                return c.name, c
        except Exception:
            continue
    return None, None


async def _wait_until_removed(name: str, timeout: int = 10):
    for _ in range(timeout * 10):
        try:
            client.containers.get(name)
            await asyncio.sleep(0.1)
        except NotFound:
            return
    raise HTTPException(500, f"Container {name} removal timed out")


# =========================================================
# START CONTAINER
# =========================================================
async def _start_container(stream_name: str, rtsp_url: str):
    stream_lock = _get_lock(stream_name)

    async with stream_lock:
        rtsp_container_name, rtsp_container = _find_rtsp_container(rtsp_url)

        # RTSP already used by another stream
        if rtsp_container_name and rtsp_container_name != stream_name:
            return {
                "status": "blocked",
                "message": f"RTSP already in use by stream '{rtsp_container_name}'",
            }

        # 🔁 Same stream + same RTSP → restart
        if rtsp_container_name == stream_name:
            await asyncio.to_thread(rtsp_container.stop, timeout=10)
            await asyncio.to_thread(rtsp_container.remove, force=True)
            await _wait_until_removed(stream_name)

        # 🧹 Same stream name but different RTSP
        try:
            existing = client.containers.get(stream_name)
            await asyncio.to_thread(existing.stop, timeout=10)
            await asyncio.to_thread(existing.remove, force=True)
            await _wait_until_removed(stream_name)
        except NotFound:
            pass

        # ▶ Start container
        try:
            await asyncio.to_thread(
                client.containers.run,
                IMAGE,
                [
                    "./kvs_gstreamer_sample",
                    stream_name,
                    rtsp_url,
                    *KVS_GSTREAMER_EXTRA_ARGS,
                ],
                detach=True,
                name=stream_name,
                restart_policy={"Name": "unless-stopped"},
                environment={
                    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                    "AWS_DEFAULT_REGION": os.environ.get(
                        "AWS_DEFAULT_REGION", "ap-south-1"
                    ),
                },
            )

            return {
                "status": "started",
                "stream": stream_name,
                "rtsp_url": rtsp_url,
            }

        except DockerException as e:
            raise HTTPException(500, str(e))


# =========================================================
# STOP CONTAINER
# =========================================================
async def _stop_container(stream_name: str):
    lock = _get_lock(stream_name)

    async with lock:
        try:
            container = client.containers.get(stream_name)

            await asyncio.to_thread(container.stop, timeout=10)
            await asyncio.to_thread(container.remove, force=True)

            return {"status": "stopped", "stream": stream_name}

        except NotFound:
            return {"error": "stream not found"}

        except DockerException as e:
            raise HTTPException(500, str(e))


async def _start_or_reload_user_worker(user_id: str, streams: list[tuple[str, str]]):
    worker_name = _build_worker_name(user_id)
    worker_lock = _get_user_worker_lock(user_id)

    async with worker_lock:
        try:
            existing = client.containers.get(worker_name)
            await asyncio.to_thread(existing.stop, timeout=10)
            await asyncio.to_thread(existing.remove, force=True)
            await _wait_until_removed(worker_name)
        except NotFound:
            pass

        if not streams:
            return {"status": "empty", "worker": worker_name}

        processes = [
            f"{_kvs_gstreamer_shell_invocation(stream_id, rtsp_url)} &"
            for stream_id, rtsp_url in streams
        ]

        worker_cmd = " ".join(processes + ["wait"])

        try:
            await asyncio.to_thread(
                client.containers.run,
                IMAGE,
                ["sh", "-c", worker_cmd],
                detach=True,
                name=worker_name,
                restart_policy={"Name": "unless-stopped"},
                environment={
                    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                    "AWS_DEFAULT_REGION": os.environ.get(
                        "AWS_DEFAULT_REGION", "ap-south-1"
                    ),
                },
            )

            return {
                "status": "started",
                "worker": worker_name,
                "stream_count": len(streams),
            }

        except DockerException as exc:
            raise HTTPException(status_code=500, detail=str(exc))


# =========================================================
# WORKER SYSTEM
# =========================================================
@dataclass
class StreamJob:
    action: Literal["start", "stop"]
    user_id: str
    stream_name: str
    rtsp_url: str | None = None
    result_fut: asyncio.Future | None = None


class StreamWorkerManager:
    def __init__(self):
        self.queue: asyncio.Queue[StreamJob] = asyncio.Queue(maxsize=100)
        self.sem = asyncio.Semaphore(WORKER_CONCURRENCY)
        self.worker_task: asyncio.Task | None = None

    async def start(self):
        await asyncio.to_thread(init_db)

        if self.worker_task is None or self.worker_task.done():
            self.worker_task = asyncio.create_task(self._worker_loop())

    async def submit(self, job: StreamJob):
        try:
            await asyncio.wait_for(self.queue.put(job), timeout=2)
        except TimeoutError as exc:
            raise HTTPException(status_code=503, detail="Worker queue is busy") from exc

    async def _worker_loop(self):
        while True:
            job = await self.queue.get()

            try:
                async with self.sem:
                    if job.action == "start":
                        result = await self._handle_start(job)
                    else:
                        result = await self._handle_stop(job)

                    if job.result_fut and not job.result_fut.done():
                        job.result_fut.set_result(result)

            except Exception as e:
                if job.result_fut and not job.result_fut.done():
                    job.result_fut.set_exception(e)

            finally:
                self.queue.task_done()

    async def _handle_start(self, job: StreamJob):
        if not job.rtsp_url:
            raise HTTPException(status_code=422, detail="rtsp_url is required")

        stream_id = _build_stream_id(job.user_id, job.stream_name)

        reserve_result = await asyncio.to_thread(
            reserve_stream_slot,
            user_id=job.user_id,
            stream_name=job.stream_name,
            stream_id=stream_id,
            rtsp_url=job.rtsp_url,
            max_streams_per_user=MAX_STREAMS_PER_USER,
        )

        if reserve_result.get("status") != "reserved":
            return reserve_result

        try:
            result = await asyncio.wait_for(
                _start_container(stream_id, job.rtsp_url),
                timeout=JOB_TIMEOUT_SECONDS,
            )

            if result.get("status") == "started":
                await asyncio.to_thread(set_stream_status, stream_id, "RUNNING", None)
            else:
                await asyncio.to_thread(
                    set_stream_status,
                    stream_id,
                    "FAILED",
                    "container start blocked",
                )

            return result

        except TimeoutError as exc:
            await asyncio.to_thread(
                set_stream_status,
                stream_id,
                "FAILED",
                "start timeout",
            )
            raise HTTPException(status_code=504, detail="Start stream timed out") from exc

        except Exception as exc:
            await asyncio.to_thread(set_stream_status, stream_id, "FAILED", str(exc))
            raise

    async def _handle_stop(self, job: StreamJob):
        stream_id = _build_stream_id(job.user_id, job.stream_name)

        await asyncio.to_thread(set_stream_status, stream_id, "STOPPING", None)

        result = await _stop_container(stream_id)

        if result.get("status") == "stopped":
            await asyncio.to_thread(set_stream_status, stream_id, "STOPPED", None)
            return result

        if result.get("error") == "stream not found":
            await asyncio.to_thread(set_stream_status, stream_id, "STOPPED", None)

            remaining_streams = await asyncio.to_thread(
                get_active_streams_for_user, job.user_id
            )

            await asyncio.wait_for(
                _start_or_reload_user_worker(job.user_id, remaining_streams),
                timeout=JOB_TIMEOUT_SECONDS,
            )

            return {
                "status": "stopped",
                "stream": stream_id,
                "mode": "user_worker",
            }

        return result


manager = StreamWorkerManager()


# =========================================================
# PUBLIC API FUNCTIONS
# =========================================================
async def start_stream(user_id: str, stream_name: str, rtsp_url: str):
    fut = asyncio.get_running_loop().create_future()

    await manager.submit(
        StreamJob(
            action="start",
            user_id=user_id,
            stream_name=stream_name,
            rtsp_url=rtsp_url,
            result_fut=fut,
        )
    )

    return await asyncio.wait_for(fut, timeout=JOB_TIMEOUT_SECONDS + 5)


async def stop_stream(user_id: str, stream_name: str):
    fut = asyncio.get_running_loop().create_future()

    await manager.submit(
        StreamJob(
            action="stop",
            user_id=user_id,
            stream_name=stream_name,
            result_fut=fut,
        )
    )

    return await asyncio.wait_for(fut, timeout=JOB_TIMEOUT_SECONDS + 5)


async def start_stream_batch(user_id: str, streams: list[dict[str, str]]):
    results = []
    reserved_stream_ids: list[str] = []

    for item in streams:
        stream_name = item["stream_name"]
        rtsp_url = item["rtsp_url"]
        stream_id = _build_stream_id(user_id, stream_name)

        reserve_result = await asyncio.to_thread(
            reserve_stream_slot,
            user_id=user_id,
            stream_name=stream_name,
            stream_id=stream_id,
            rtsp_url=rtsp_url,
            max_streams_per_user=MAX_STREAMS_PER_USER,
        )

        if reserve_result.get("status") == "reserved":
            reserved_stream_ids.append(stream_id)
            results.append({"stream_name": stream_name, "status": "reserved"})

        elif reserve_result.get("status") == "already_running":
            results.append({"stream_name": stream_name, "status": "already_running"})

        else:
            results.append(
                {
                    "stream_name": stream_name,
                    "status": reserve_result.get("status", "blocked"),
                    "message": reserve_result.get(
                        "message", "reservation blocked"
                    ),
                }
            )

    active_streams = await asyncio.to_thread(
        get_active_streams_for_user, user_id
    )

    if not active_streams:
        return {
            "user_id": user_id,
            "worker": _build_worker_name(user_id),
            "summary": {
                "requested": len(streams),
                "started": 0,
                "failed": len(streams),
            },
            "results": results,
        }

    try:
        await asyncio.wait_for(
            _start_or_reload_user_worker(user_id, active_streams),
            timeout=JOB_TIMEOUT_SECONDS,
        )

        for stream_id in reserved_stream_ids:
            await asyncio.to_thread(set_stream_status, stream_id, "RUNNING", None)

    except Exception as exc:
        for stream_id in reserved_stream_ids:
            await asyncio.to_thread(set_stream_status, stream_id, "FAILED", str(exc))

        for result in results:
            if result["status"] == "reserved":
                result["status"] = "failed"
                result["message"] = str(exc)

    else:
        for result in results:
            if result["status"] == "reserved":
                result["status"] = "started"

    started_count = len(
        [r for r in results if r["status"] in ("started", "already_running")]
    )

    failed_count = len(
        [r for r in results if r["status"] not in ("started", "already_running")]
    )

    return {
        "user_id": user_id,
        "worker": _build_worker_name(user_id),
        "summary": {
            "requested": len(streams),
            "started": started_count,
            "failed": failed_count,
        },
        "results": results,
    }


# =========================================================
# LIST STREAMS
# =========================================================
def list_streams():
    return [c.name for c in client.containers.list(all=True)]


# =========================================================
# GET HLS URL
# =========================================================
def get_hls_url(stream_name: str):
    try:
        kv_client = boto3.client("kinesisvideo", region_name=AWS_REGION)

        endpoint_response = kv_client.get_data_endpoint(
            StreamName=stream_name,
            APIName="GET_HLS_STREAMING_SESSION_URL",
        )

        archived_media_client = boto3.client(
            "kinesis-video-archived-media",
            endpoint_url=endpoint_response["DataEndpoint"],
            region_name=AWS_REGION,
        )

        hls_response = archived_media_client.get_hls_streaming_session_url(
            StreamName=stream_name,
            PlaybackMode="LIVE",
            Expires=43200,
            ContainerFormat="FRAGMENTED_MP4",
        )

        return hls_response["HLSStreamingSessionURL"]

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "ResourceNotFoundException":
            raise HTTPException(status_code=404, detail="Stream not found")

        if error_code in ("InvalidArgumentException", "ResourceInUseException"):
            raise HTTPException(
                status_code=409,
                detail="HLS not available yet. Stream may be inactive.",
            )

        if error_code == "AccessDeniedException":
            raise HTTPException(status_code=403, detail="Access denied")

        raise HTTPException(status_code=500, detail=str(e))

    except BotoCoreError:
        raise HTTPException(status_code=502, detail="AWS communication error")