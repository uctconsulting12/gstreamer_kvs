import os
import asyncio
import boto3
import docker
from fastapi import HTTPException
from botocore.exceptions import BotoCoreError, ClientError
from docker.errors import NotFound, DockerException

IMAGE = "rtspdockertest"

# Docker client
if os.name == "nt":
    client = docker.DockerClient(base_url="npipe:////./pipe/docker_engine")
else:
    client = docker.DockerClient(base_url="unix:///var/run/docker.sock")

# Locks
_stream_locks: dict[str, asyncio.Lock] = {}
_global_lock = asyncio.Lock()


def _get_lock(stream_name: str) -> asyncio.Lock:
    if stream_name not in _stream_locks:
        _stream_locks[stream_name] = asyncio.Lock()
    return _stream_locks[stream_name]


def _find_rtsp_container(rtsp_url: str):
    """
    Returns (container_name, container) if RTSP is already used
    """
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
# START STREAM
# =========================================================
async def start_stream(stream_name: str, rtsp_url: str):
    stream_lock = _get_lock(stream_name)

    async with _global_lock, stream_lock:
        rtsp_container_name, rtsp_container = _find_rtsp_container(rtsp_url)

        # RTSP already used by another stream
        if rtsp_container_name and rtsp_container_name != stream_name:
            return {
                "status": "blocked",
                "message": f"RTSP already in use by stream '{rtsp_container_name}'"
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
                ["./kvs_gstreamer_sample", stream_name, rtsp_url],
                detach=True,
                name=stream_name,
                restart_policy={"Name": "unless-stopped"},
                environment={
                    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                    "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                },
            )

            return {
                "status": "started",
                "stream": stream_name,
                "rtsp_url": rtsp_url
            }

        except DockerException as e:
            raise HTTPException(500, str(e))


# =========================================================
# STOP STREAM
# =========================================================
async def stop_stream(stream_name: str):
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
        kv_client = boto3.client("kinesisvideo", region_name="us-east-1")

        endpoint_response = kv_client.get_data_endpoint(
            StreamName=stream_name,
            APIName="GET_HLS_STREAMING_SESSION_URL"
        )

        archived_media_client = boto3.client(
            "kinesis-video-archived-media",
            endpoint_url=endpoint_response["DataEndpoint"],
            region_name="us-east-1"
        )

        hls_response = archived_media_client.get_hls_streaming_session_url(
            StreamName=stream_name,
            PlaybackMode="LIVE",
            Expires=43200,
            ContainerFormat="FRAGMENTED_MP4"
        )

        return hls_response["HLSStreamingSessionURL"]

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "ResourceNotFoundException":
            raise HTTPException(status_code=404, detail="Stream not found")

        if error_code in ("InvalidArgumentException", "ResourceInUseException"):
            raise HTTPException(
                status_code=409,
                detail="HLS not available yet. Stream may be inactive."
            )

        if error_code == "AccessDeniedException":
            raise HTTPException(status_code=403, detail="Access denied")

        raise HTTPException(status_code=500, detail=str(e))

    except BotoCoreError as e:
        raise HTTPException(status_code=502, detail="AWS communication error")
