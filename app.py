from fastapi import FastAPI
from streams import start_stream, stop_stream, list_streams, get_hls_url
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import asyncio

app = FastAPI()

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

class StreamName(BaseModel):
    stream_name: str


@app.post("/streams/start")
async def start_stream_endpoint(data: StreamStartRequest):
    result = await start_stream(
        stream_name=data.stream_name,
        rtsp_url=data.rtsp_url
    )

    # non-blocking wait
    await asyncio.sleep(15)

    return result


@app.post("/streams/stop/{stream_name}")
async def stop(stream_name: str):
    return await stop_stream(stream_name)


@app.get("/streams")
def list_all():
    return list_streams()


@app.post("/streams/hls")
def create_hls_stream(data: StreamName):
    hls_url = get_hls_url(stream_name=data.stream_name)
    return {
        "stream_name": data.stream_name,
        "hls_url": hls_url
    }
