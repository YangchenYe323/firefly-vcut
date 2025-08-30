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
    secrets=[secret],
)
class WhisperX:
    @modal.enter()
    def setup(self):
        print("🔄 Loading WhisperX model …")
        import whisperx

        self.model = whisperx.load_model(
            "turbo", device="cuda", download_root=pathlib.Path(CACHE_DIR, "whisperx")
        )
        print("✅ WhisperX model ready!")
        print("🔄 Loading WhisperX align model …")
        align_model, metadata = whisperx.load_align_model(
            language_code="zh",
            device="cuda",
            model_dir=pathlib.Path(CACHE_DIR, "whisperx-align"),
        )
        self.align_model = align_model
        self.metadata = metadata
        print("✅ WhisperX align model ready!")
        print("🔄 Loading WhisperX diarization model …")

        # Set up environment variables for using the cache for huggingface models
        os.environ["HF_HOME"] = str(pathlib.Path(CACHE_DIR, "/huggingface/cache"))
        os.environ["TRANSFORMERS_CACHE"] = str(pathlib.Path(CACHE_DIR, "/transformers/cache"))
        os.environ["TORCH_HOME"] = str(pathlib.Path(CACHE_DIR, "/torch/cache"))
        self.diarize_model = whisperx.diarize.DiarizationPipeline(
            use_auth_token=os.getenv("HUGGINGFACE_ACCESS_TOKEN"),
            device="cuda",
        )
        print("✅ WhisperX diarization model ready!")

    @modal.method()
    def transcribe(self, data: bytes, format: str) -> str:
        import whisperx

        print(f"🔄 Transcribing audio in format {format} …")
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as f:
            f.write(data)
            temp_audio_path = f.name
        try:
            audio = whisperx.load_audio(temp_audio_path)
            print("🔄 Transcribing audio …")
            result = self.model.transcribe(audio, language="zh", verbose=True)
            print("🔄 Aligning audio …")
            result = whisperx.align(
                result["segments"],
                self.align_model,
                self.metadata,
                audio,
                "cuda",
                return_char_alignments=False,
            )
            print("🔄 Diarizing audio …")
            diarization = self.diarize_model(audio)
            print("🔄 Aligning transcript and diarization…")
            result = whisperx.assign_word_speakers(diarization, result)
            print("✅ Transcribed audio")

            print(f"✅ Transcribed {len(result['segments'])} segments")
            print(result)
            segments = [
                {
                    "start": segment["start"],
                    "text": segment["text"],
                    "speaker": segment["speaker"],
                }
                for segment in result["segments"]
            ]
            return segments
        finally:
            os.unlink(temp_audio_path)

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

@app.function()
def display_system_info():
    import os
    import sys
    
    # Check CUDA availability
    try:
        import torch
        print(f"PyTorch version: {torch.__version__}")
        print(f"CUDA available: {torch.cuda.is_available()}")
        if torch.cuda.is_available():
            print(f"CUDA version: {torch.version.cuda}")
            print(f"cuDNN version: {torch.backends.cudnn.version()}")
            print(f"GPU count: {torch.cuda.device_count()}")
            print(f"Current GPU: {torch.cuda.current_device()}")
            print(f"GPU name: {torch.cuda.get_device_name()}")
    except ImportError:
        print("PyTorch not available")
    
    # Check NVIDIA libraries
    try:
        import nvidia.cublas.lib
        print(f"cuBLAS lib path: {os.path.dirname(nvidia.cublas.lib.__file__)}")
    except (ImportError, AttributeError) as e:
        print(f"cuBLAS not available: {e}")
    
    try:
        import nvidia.cudnn.lib
        if hasattr(nvidia.cudnn.lib, '__path__') and nvidia.cudnn.lib.__path__:
            print(f"cuDNN lib path: {os.path.dirname(nvidia.cudnn.lib.__path__[0])}")
        else:
            print("cuDNN lib path: None (library not properly installed)")
    except (ImportError, AttributeError) as e:
        print(f"cuDNN not available: {e}")
    
    # Check system CUDA
    print(f"CUDA_HOME: {os.environ.get('CUDA_HOME', 'Not set')}")
    print(f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH', 'Not set')}")
    # Inspect cudnn object files
    for file in os.listdir(nvidia.cudnn.lib.__path__[0]):
        print(file)


@app.function()
def diagnose_cudnn():
    try:
        import nvidia.cudnn
        print(f"nvidia.cudnn module: {nvidia.cudnn}")
        print(f"nvidia.cudnn.__file__: {getattr(nvidia.cudnn, '__file__', 'No __file__ attribute')}")
        
        # Check what's available in the module
        print(f"nvidia.cudnn dir: {dir(nvidia.cudnn)}")
        
        # Try different import paths
        try:
            import nvidia.cudnn.lib
            print(f"nvidia.cudnn.lib found: {nvidia.cudnn.lib}")
        except Exception:
            print("nvidia.cudnn.lib not found")
            
        try:
            from nvidia.cudnn import lib
            print(f"from nvidia.cudnn import lib: {lib}")
        except Exception:
            print("from nvidia.cudnn import lib failed")
            
    except ImportError as e:
        print(f"nvidia.cudnn not available: {e}")

@app.function(
    timeout=30 * 60,  # 30 minutes
    secrets=[secret],
    volumes={
        CACHE_DIR: cache_volume,
        BUCKET_DIR: bucket_volume,
    },
)
def test_whisperx(audio_object_key: str):
    audio_path = pathlib.Path(BUCKET_DIR, audio_object_key)
    with open(audio_path, "rb") as f:
        data = f.read()
    modal = WhisperX()
    segments = modal.transcribe.remote(data, "mp4")
    print(segments)


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
