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
        "nvidia/cuda:11.8.0-cudnn8-devel-ubuntu22.04",
        add_python="3.12",
    )
    .apt_install("ffmpeg")
    .uv_pip_install(
        "python-dotenv>=1.1.1",
        # "openai-whisper>=20250625",
        "psycopg[binary]>=3.2.9",
        "pytz>=2025.2",
        "requests>=2.32.4",
        "boto3>=1.40.6",
        "botocore>=1.40.6",
        "aiohttp>=3.11.12",
        "rapidfuzz>=3.13.0",
        "ctranslate2<4.4.0",
        "whisperx>=3.4.2",
    )
    # .env(
        # {
            # "LD_LIBRARY_PATH": " /usr/local/lib/python3.12/site-packages/nvidia/cublas/lib:/usr/local/lib/python3.12/site-packages/nvidia/cudnn:/usr/local/nvidia/lib:/usr/local/nvidia/lib64",
        # }
    # )
)

secret = modal.Secret.from_name("firefly-secret")

app = modal.App("firefly-vcut-app", image=image)

cache_volume = modal.Volume.from_name("firefly-vcut-cache", create_if_missing=True)

# bucket_name and bucket_volume are retreived from .env when running/deploying modal locally,
# and retrieved from GitHub secrets when deploying modal using GitHub Actions.
bucket_volume = modal.CloudBucketMount(
    bucket_name=os.getenv("R2_BUCKET"),
    bucket_endpoint_url=os.getenv("R2_ENDPOINT"),
    secret=secret,
    read_only=False,
)
