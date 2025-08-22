"""
This module contains the resource definitions of the firefly-vcut application on modal.
"""

import modal
import dotenv
import os

dotenv.load_dotenv()

CACHE_DIR = "/firefly-vcut-cache"
BUCKET_DIR = "/firefly-vcut-bucket"

image = (
    modal.Image.from_registry(
        "nvidia/cuda:12.8.0-cudnn-devel-ubuntu22.04",
        add_python="3.12",
    )
    .apt_install("ffmpeg")
    .uv_pip_install(
        "python-dotenv>=1.1.1",
        "openai-whisper>=20250625",
        "psycopg[binary]>=3.2.9",
        "pytz>=2025.2",
        "requests>=2.32.4",
        "boto3>=1.40.6",    
        "botocore>=1.40.6",
        "aiohttp>=3.11.12",
        "rapidfuzz>=3.13.0",
    )
)

secret = modal.Secret.from_name("firefly-secret")

app = modal.App("firefly-vcut-app", image=image)

cache_volume = modal.Volume.from_name("firefly-vcut-cache", create_if_missing=True)

bucket_volume = modal.CloudBucketMount(
    bucket_name=os.getenv("R2_BUCKET"),
    bucket_endpoint_url=os.getenv("R2_ENDPOINT"),
    secret=secret,
    read_only=False,
)
