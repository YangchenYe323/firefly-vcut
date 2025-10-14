import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
import time

import aiohttp
import botocore
import pytz
import requests
import firefly_vcut.db as db
import boto3
from firefly_vcut import bilibili
from firefly_vcut.retry import retry_with_backoff, retry_with_backoff_async
from firefly_vcut.config import STREAMING_RETRY_CONFIG

from .app import app, secret


@app.function(
    timeout=60 * 60,  # 60 minutes
    secrets=[secret],
)
async def stream_recordings():
    # Fetch a recordings to stream audio for
    with db.connection(os.getenv("DATABASE_URL")) as conn:
        recordings = db.recording.list_recordings_to_stream(conn, limit=10)
        if not recordings:
            print("No recordings to stream")
            return

    sessdata = os.getenv("BILIBILI_SESSDATA")
    wbi_key = bilibili.wbi.getWbiKeys(sessdata)

    r2 = boto3.client(
        service_name="s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    for recording in recordings:
        print(f"Streaming recording {recording['title']} ({recording['bvid']})...")

        try:
            object_keys = await stream_recording(
                recording,
                sessdata,
                wbi_key,
                r2,
                os.getenv("R2_BUCKET"),
            )

            print(
                f"Streamed audio from recording {recording['title']} to {object_keys} in bucket {os.getenv('R2_BUCKET')}"
            )

            # Update the recording with the object keys
            with db.connection(os.getenv("DATABASE_URL")) as conn:
                db.recording.update_recording_audio_object_keys(
                    conn, recording["id"], object_keys
                )

                print(
                    f"Updated recording {recording['title']} with object keys {object_keys} in database"
                )
        except Exception as e:
            print(f"Failed to stream recording {recording['title']}: {e}")
            continue


async def stream_recording(
    recording: dict,
    sessdata: str,
    wbi_key: tuple[str, str],
    r2: botocore.client.BaseClient,
    bucket: str,
) -> list[str]:
    """
    Stream audio for a recording and save it to object store.

    Args:
        recording: A dictionary with the following keys:
            - id: The recording ID.
            - title: The title of the recording.
            - bvid: The Bilibili video ID.
        sessdata: The sessdata of the user.
        wbi_key: The wbi key of the user.
        r2: s3 client for the cloudflare r2 bucket
        bucket: The name of the bucket.

    Returns:
        A list of object keys for the streamed audio
    """

    # Get the video info
    info_response = bilibili.video.get_video_info(
        recording["bvid"], os.getenv("BILIBILI_SESSDATA")
    )
    if info_response["code"] != 0:
        raise Exception(f"Failed to get video info: {info_response['message']}")

    if not info_response["data"]["pages"]:
        raise Exception(f"No pages found for video {recording['bvid']}")

    object_keys = []
    for page in info_response["data"]["pages"]:
        audio_object_key = get_audio_object_key(recording, page["page"])
        print(f"Streaming page {page['page']} to {audio_object_key}...")

        if object_exists(r2, bucket, audio_object_key):
            print(f"Audio object {audio_object_key} already exists, skipping...")
            object_keys.append(audio_object_key)
            continue

        stream_url_response = bilibili.video.get_video_stream_url(
            recording["bvid"],
            page["cid"],
            4048,  # Dash audio.
            sessdata,
            wbi_key,
        )

        if stream_url_response["code"] != 0:
            raise Exception(
                f"Failed to get video stream url: {stream_url_response['message']}"
            )

        selected_audio = stream_url_response["data"]["dash"]["audio"][0]
        # Download the audio
        audio_url = selected_audio["baseUrl"]

        # Get the content length of the audio by sending a HEAD request
        def _make_head_request():
            audio_resp = requests.head(
                audio_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                    "Cookie": f"SESSDATA={sessdata}",
                    "Referer": "https://www.bilibili.com/",
                },
            )
            return audio_resp

        audio_resp = retry_with_backoff(_make_head_request, STREAMING_RETRY_CONFIG)

        if audio_resp.status_code != 200:
            raise Exception(
                f"Failed to get audio content length: {audio_resp.status_code}"
            )

        content_length = int(audio_resp.headers["Content-Length"])
        print(f"Audio content length: {content_length}")

        for chunk_size in (20 * 1024 * 1024, 50 * 1024 * 1024, None):
            # Try different chunk sizes and fall back to single threaded upload
            # as bilibili server goes to lunch sometime for no reason and
            # closes the connection for a specific chunk.
            # This manifests as <ContentLengthError: 400, message='Not enough data to satisfy content length header.'>
            # when reading the chunk.

            print(f"Chunking audio with chunk size {chunk_size}...")
            chunks = chunk_audio(content_length, chunk_size)

            try:
                # Start multipart upload
                multipart_upload = r2.create_multipart_upload(
                    Bucket=bucket, Key=audio_object_key, ContentType="audio/mp4"
                )
                upload_id = multipart_upload["UploadId"]
                print(
                    f"Created multipart upload for {audio_object_key} with upload ID {upload_id}"
                )

                async def upload_single_chunk(
                    part_number: int,
                    chunk: tuple[int, int],
                    thread_pool_executor: ThreadPoolExecutor,
                ) -> dict:
                    range_header = (
                        f"bytes={chunk[0]}-{chunk[1]}"
                        if chunk[1] != -1
                        else f"bytes={chunk[0]}-"
                    )

                    header = {
                        "Cookie": f"SESSDATA={sessdata}",
                        "Referer": "https://www.bilibili.com/",
                        "User-Agent": "Mozilla/5.0",
                        "Range": range_header,
                    }

                    async def _make_async_request():
                        async with aiohttp.ClientSession() as session:
                            async with session.get(audio_url, headers=header) as resp:
                                if not resp.ok:
                                    raise Exception(
                                        f"Failed to get audio chunk: {resp.status}, {await resp.text()}"
                                    )

                                # Check if Content-Length matches what we expect
                                content_length = resp.headers.get("Content-Length")
                                expected_size = (
                                    chunk[1] - chunk[0] + 1 if chunk[1] != -1 else None
                                )

                                if content_length and expected_size:
                                    if int(content_length) != expected_size:
                                        print(
                                            f"Warning: Content-Length mismatch. Expected: {expected_size}, Got: {content_length}"
                                        )
                                        raise Exception(
                                            f"Content-Length mismatch. Expected: {expected_size}, Got: {content_length}"
                                        )
                                print(f"Reading chunk {chunk}...")
                                # Note: let's see how it works just buffering the whole thing in memory so I don't have
                                # to deal with the fancy streaming and buffering to save some memory. One page is typically
                                # 200-500MB, and the cost of that is pretty much negligible compared to GPU (my guess).
                                return await resp.read()

                    chunk_data = await retry_with_backoff_async(
                        _make_async_request, STREAMING_RETRY_CONFIG
                    )

                    print(f"Uploading chunk {chunk}...")
                    loop = asyncio.get_event_loop()
                    upload_response = await loop.run_in_executor(
                        thread_pool_executor,
                        lambda: r2.upload_part(
                            Bucket=bucket,
                            Key=audio_object_key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=chunk_data,
                            ContentLength=len(chunk_data),
                        ),
                    )

                    print(f"Uploaded chunk {chunk}...")
                    return {"ETag": upload_response["ETag"], "PartNumber": part_number}

                with ThreadPoolExecutor(max_workers=5) as thread_pool_executor:
                    tasks = [
                        upload_single_chunk(i + 1, chunk, thread_pool_executor)
                        for i, chunk in enumerate(chunks)
                    ]
                    results = await asyncio.gather(*tasks)
                # Complete multipart upload
                r2.complete_multipart_upload(
                    Bucket=bucket,
                    Key=audio_object_key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": results,
                    },
                )

                print(
                    f"Completed multipart upload for {audio_object_key} with upload ID {upload_id}"
                )
                break
            except Exception as e:
                print(
                    f"Failed to complete multipart upload: {e}, try another chunk size..."
                )
                if tasks:
                    for task in tasks:
                        task.cancel()

                if upload_id:
                    r2.abort_multipart_upload(
                        Bucket=bucket, Key=audio_object_key, UploadId=upload_id
                    )
                time.sleep(10)

        object_keys.append(audio_object_key)

    return object_keys


def object_exists(
    r2: botocore.client.BaseClient,
    bucket: str,
    object_key: str,
) -> bool:
    """
    Check if the audio object key already exists.
    """
    try:
        r2.head_object(Bucket=bucket, Key=object_key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise e


def chunk_audio(
    content_length: int, chunk_size: int | None = 20 * 1024 * 1024
) -> list[tuple[int, int]]:
    """
    Chunk the audio into chunks of the given size.
    """

    if chunk_size is None:
        return [(0, -1)]

    chunks = []
    for i in range(0, content_length, chunk_size + 1):
        if i + chunk_size >= content_length:
            chunks.append((i, -1))
        else:
            chunks.append((i, i + chunk_size))
    return chunks


def get_audio_object_key(recording: dict, page: int) -> str:
    """
    Get the audio object keys from the video info.

    Args:
        recording: A dictionary with the following keys:
            - id: The recording ID.
            - title: The title of the recording.
            - bvid: The Bilibili video ID.
            - mid: The vtuber mid of the recording.
            - pubdate: The publication date of the video, unix epoch timestamp, meant to be consumed
        page: The page number of the video,

    Returns:
        The audio object key.
    """

    bvid = recording["bvid"]
    mid = recording["mid"]
    pubdate = recording["pubdate"]
    # Extract year, month, day from pudate in tz Asia/Shanghai
    pubdate = datetime.fromtimestamp(pubdate, tz=pytz.timezone("Asia/Shanghai"))
    year = pubdate.year
    month = pubdate.month
    day = pubdate.day

    # We use dash, which gives mp4 format audios
    return f"audio/{mid}/{year}/{month:02d}/{day:02d}/{bvid}/{page}.mp4"
