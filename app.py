from fastapi import FastAPI
from streams import (
    start_stream,
    start_stream_batch,
    stop_stream,
    list_streams,
    get_hls_url,
    manager,
)
from pydantic import BaseModel, field_validator
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager


# ✅ lifespan to start worker
@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.start()
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class StreamStartRequest(BaseModel):
    stream_name: str
    rtsp_url: str
    user_id: str

    @field_validator("user_id")
    @classmethod
    def normalize(cls, v):
        v = v.strip().lower()
        if not v:
            raise ValueError("user_id required")
        return v


class StreamName(BaseModel):
    stream_name: str
    user_id: str


class CameraStream(BaseModel):
    stream_name: str
    rtsp_url: str


class BatchStreamStartRequest(BaseModel):
    user_id: str
    streams: list[CameraStream]

    @field_validator("user_id")
    @classmethod
    def normalize_user(cls, v):
        v = v.strip().lower()
        if not v:
            raise ValueError("user_id required")
        return v

    @field_validator("streams")
    @classmethod
    def validate_streams(cls, v):
        if not v:
            raise ValueError("at least one stream is required")
        if len(v) > 20:
            raise ValueError("maximum 20 streams per request")
        return v


@app.post("/streams/start")
async def start_stream_endpoint(data: StreamStartRequest):
    return await start_stream(
        user_id=data.user_id,
        stream_name=data.stream_name,
        rtsp_url=data.rtsp_url,
    )


@app.post("/streams/start/batch")
async def start_streams_batch_endpoint(data: BatchStreamStartRequest):
    stream_payload = [
        {
            "stream_name": item.stream_name,
            "rtsp_url": item.rtsp_url,
        }
        for item in data.streams
    ]

    return await start_stream_batch(
        user_id=data.user_id,
        streams=stream_payload,
    )


@app.post("/streams/stop/{stream_name}")
async def stop(stream_name: str, user_id: str):
    return await stop_stream(user_id, stream_name)


@app.get("/streams")
def list_all():
    return list_streams()


@app.post("/streams/hls")
def create_hls_stream(data: StreamName):
    stream_id = f"{data.user_id}__{data.stream_name}"
    hls_url = get_hls_url(stream_name=stream_id)

    return {
        "stream_name": stream_id,
        "hls_url": hls_url,
    }