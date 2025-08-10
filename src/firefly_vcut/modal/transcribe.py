from datetime import datetime
import json
import modal
import tempfile
import os

import pathlib

from .app import app, cache_volume, bucket_volume, CACHE_DIR, BUCKET_DIR, secret, image

@app.cls(
    gpu="T4",
    image=image,
    timeout=10 * 60,
    volumes={
        CACHE_DIR: cache_volume,
    },
)
class Whisper:
    """Serverless Whisper model running on a single GPU"""

    @modal.enter()
    def setup(self):
        print("🔄 Loading Whisper model …")
        import whisper

        self.model = whisper.load_model("turbo", download_root=CACHE_DIR)
        print("✅ Model ready!")

    @modal.method()
    def transcribe(self, data: bytes, format: str) -> str:
        print(f"🔄 Transcribing audio in format {format} …")

        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as f:
            f.write(data)
            temp_audio_path = f.name

        try:
            result = self.model.transcribe(temp_audio_path, language="zh", verbose=True)
            segments = [
                {"start": segment["start"], "text": segment["text"]}
                for segment in result["segments"]
            ]

            print(f"✅ Transcribed {len(segments)} segments")
            return segments
        finally:
            os.unlink(temp_audio_path)


@app.function(
    timeout=30 * 60,  # 30 minutes
    secrets=[secret],
    volumes={
        BUCKET_DIR: bucket_volume,
        CACHE_DIR: cache_volume,
    },
)
def transcribe_recordings():
    import firefly_vcut.db as db

    with db.connection(os.getenv("DATABASE_URL")) as conn:
        recordings = db.recording.list_recordings_to_transcribe(conn)

    modal = Whisper()

    for recording in recordings:
        print(f"🔄 Transcribing recording {recording['title']} …")

        # Check if the transcript already exists in the cache
        transcript_key = transcript_key_from_recording(recording)
        print(f"🔄 Transcript key: {transcript_key}")

        bucket_path = pathlib.Path(BUCKET_DIR, transcript_key)
        cache_path = pathlib.Path(CACHE_DIR, transcript_key)

        if bucket_path.exists():
            print(
                f"✅ Transcript of {recording['title']} exists in bucket, updating database …"
            )
        else:
            if cache_path.exists():
                with open(cache_path, "r") as f:
                    segments = json.load(f)
                print(f"✅ Transcribed recording {recording['title']} exists in cache")
            else:
                # the ith audio is the ith page of the recording
                # we merge all the pages into one transcript
                segments = []
                for audio_key in recording["audioObjectKeys"]:
                    print(f"🔄 Transcribing page from {audio_key} …")

                    audio_path = pathlib.Path(BUCKET_DIR, audio_key)
                    with open(audio_path, "rb") as f:
                        data = f.read()

                    page_segments = modal.transcribe.remote(data, "mp4")
                    segments.append(page_segments)

                # Write the transcript to cache first.
                print("🔄 Writing transcript to cache …")
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, "w") as f:
                    json.dump(segments, f)
                cache_volume.commit()
                print(
                    f"✅ Transcribed recording {recording['title']} and wrote to cache"
                )

            # Then upload the transcript to the object store
            object_path = pathlib.Path(BUCKET_DIR, transcript_key)
            object_path.parent.mkdir(parents=True, exist_ok=True)
            with open(object_path, "w") as f:
                json.dump(segments, f)

        with db.connection(os.getenv("DATABASE_URL")) as conn:
            db.recording.update_recording_transcript(
                conn, recording["id"], transcript_key
            )
        print(f"✅ Transcribed recording {recording['title']}")

        # Delete the cache
        os.unlink(cache_path)
        cache_volume.commit()
        # Delete the audio files
        for audio_key in recording["audioObjectKeys"]:
            audio_path = pathlib.Path(BUCKET_DIR, audio_key)
            os.unlink(audio_path)


def transcript_key_from_recording(recording: dict) -> str:
    """
    Generate a transcript object key from a recording.
    """
    import pytz

    bvid = recording["bvid"]
    mid = recording["mid"]
    pubdate = recording["pubdate"]
    # Extract year, month, day from pudate in tz Asia/Shanghai
    pubdate = datetime.fromtimestamp(pubdate, tz=pytz.timezone("Asia/Shanghai"))
    year = pubdate.year
    month = pubdate.month
    day = pubdate.day

    return f"transcripts/{mid}/{year}/{month:02d}/{day:02d}/{bvid}.json"
