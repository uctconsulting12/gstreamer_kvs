from fastapi import FastAPI,HTTPException
from streams import start_stream, stop_stream, list_streams,get_hls_url
from pydantic import BaseModel
import time
from fastapi.middleware.cors import CORSMiddleware

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
def start_stream_endpoint(data: StreamStartRequest):
    stream_started=start_stream(
        stream_name=data.stream_name,
        rtsp_url=data.rtsp_url
    )

    time.sleep(15)
    return stream_started

@app.post("/streams/stop/{stream_name}")
def stop(stream_name: str):
    return stop_stream(stream_name)

@app.get("/streams")
def list_all():
    return list_streams()


@app.post("/streams/hls")
def create_hls_stream(data:StreamName):
    hls_url = get_hls_url(
        stream_name=data.stream_name
    )

    return {
        "stream_name": data.stream_name,
        "hls_url": hls_url
    }
