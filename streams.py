# import subprocess
# import boto3
# from botocore.exceptions import BotoCoreError, ClientError
# from fastapi import FastAPI,HTTPException

# IMAGE = "rtspdockertest"

# def start_stream(stream_name: str, rtsp_url: str):
#     # Check if container already exists
#     existing = subprocess.run(
#         ["docker", "ps", "-a", "--format", "{{.Names}}"],
#         capture_output=True,
#         text=True
#     ).stdout.splitlines()

#     if stream_name in existing:
#         return {"error": "stream already running"}

#     cmd = [
#         "docker", "run", "-d",
#         "--restart", "unless-stopped",
#         "--name", stream_name,
#         "-e", "AWS_ACCESS_KEY_ID=AKIAXZ54I5RQYZBJETZL",
#         "-e", "AWS_SECRET_ACCESS_KEY=aTDPjP+ouXqCo0ubI6D7rgM/E4ifrh6guJ9ZHTBf",
#         "-e", "AWS_DEFAULT_REGION=us-east-1",
#         IMAGE,
#         "./kvs_gstreamer_sample",
#         stream_name,
#         rtsp_url
#     ]

#     subprocess.run(cmd, check=True)
#     return {"status": "started", "stream": stream_name}

# def stop_stream(stream_name: str):
#     subprocess.run(["docker", "rm", "-f", stream_name])
#     return {"status": "stopped", "stream": stream_name}

# def list_streams():
#     result = subprocess.check_output(
#         ["docker", "ps", "--format", "{{.Names}}"]
#     )
#     return result.decode().splitlines()




# def get_hls_url(stream_name: str):
#     try:
#         # 1. Create Kinesis Video client
#         kv_client = boto3.client(
#             "kinesisvideo",
#             region_name="us-east-1"
#         )

#         # 2. Get data endpoint for HLS
#         endpoint_response = kv_client.get_data_endpoint(
#             StreamName=stream_name,
#             APIName="GET_HLS_STREAMING_SESSION_URL"
#         )

#         data_endpoint = endpoint_response["DataEndpoint"]

#         # 3. Create archived media client
#         archived_media_client = boto3.client(
#             "kinesis-video-archived-media",
#             endpoint_url=data_endpoint,
#             region_name="us-east-1"
#         )

#         # 4. Request HLS streaming session URL
#         hls_response = archived_media_client.get_hls_streaming_session_url(
#             StreamName=stream_name,
#             PlaybackMode="LIVE",
#             Expires= 43200,
#             ContainerFormat="FRAGMENTED_MP4"
#         )

#         return hls_response["HLSStreamingSessionURL"]

#     except (BotoCoreError, ClientError) as e:
#         raise HTTPException(status_code=500, detail=str(e))

 


import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import HTTPException
import docker  # Make sure 'docker' is in requirements.txt

IMAGE = "rtspdockertest"

# Use Docker SDK client
if os.name == "nt":  # Windows
    client = docker.DockerClient(base_url="npipe:////./pipe/docker_engine")
else:  # Linux / Mac
    client = docker.DockerClient(base_url="unix:///var/run/docker.sock")


def start_stream(stream_name: str, rtsp_url: str):
    # Check if container already exists
    existing = [c.name for c in client.containers.list(all=True)]
    if stream_name in existing:
        return {"error": "stream already running"}

    try:
        container = client.containers.run(
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
        return {"status": "started", "stream": stream_name}
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=500, detail=str(e))


def stop_stream(stream_name: str):
    try:
        container = client.containers.get(stream_name)
        container.stop()
        container.remove()
        return {"status": "stopped", "stream": stream_name}
    except docker.errors.NotFound:
        return {"error": "stream not found"}
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=500, detail=str(e))


def list_streams():
    containers = client.containers.list(all=True)
    return [c.name for c in containers]


def get_hls_url(stream_name: str):
    try:
        kv_client = boto3.client("kinesisvideo", region_name="us-east-1")

        endpoint_response = kv_client.get_data_endpoint(
            StreamName=stream_name,
            APIName="GET_HLS_STREAMING_SESSION_URL"
        )

        data_endpoint = endpoint_response["DataEndpoint"]

        archived_media_client = boto3.client(
            "kinesis-video-archived-media",
            endpoint_url=data_endpoint,
            region_name="us-east-1"
        )

        hls_response = archived_media_client.get_hls_streaming_session_url(
            StreamName=stream_name,
            PlaybackMode="LIVE",
            Expires=43200,
            ContainerFormat="FRAGMENTED_MP4"
        )

        return hls_response["HLSStreamingSessionURL"]

    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))
