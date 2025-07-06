import logging
import subprocess
import os
import json
import whisper

from .types import Archive

logger = logging.getLogger(__name__)

def download_audio(recording: Archive):
    """
    Download the audio files of a recording.

    Args:
        recording: The recording information

    Returns:
        None
    
    Side Effects:
        - Download the audio files of the recording to the current directory
        - The audio files are named like <bvid>-<pageNumberWithZero>.m4a
    """

    # Try download with multi thread and fallback to single thread if failed
    logger.info(f"Downloading audio files for {recording.bvid}...")
    try:
        cmd = subprocess.run(
            ["BBDown", recording.bvid, "-tv", "--audio-only", "-M", "<bvid>-<pageNumberWithZero>", "-F", "<bvid>"],
        )
        cmd.check_returncode()
    except subprocess.CalledProcessError:
        logger.info(f"Downloading audio files for {recording.bvid} with single thread...")
        cmd = subprocess.run(
            ["BBDown", recording.bvid, "-tv", "--audio-only", "-M", "<bvid>-<pageNumberWithZero>", "-F", "<bvid>", "--multi-thread", "false"],
        )
        cmd.check_returncode()

def download_and_transcribe(recording: Archive, transcription_file: str, model: whisper.Whisper):
    """
    Download the audio files of a recording and transcribe them. Write the segments to the given file

    Args:
        recording: The recording information
        transcription_file: The file to write the transcription to
        model: The model to use for transcription

    Returns:
        None
    
    Side Effects:
        - Write the transcription of the recording to the given file, JSON format:
        [
            [
                {"start": 0, "text": "Hello, world!"}
            ],
            [
                {"start": 0, "text": "Hello, world!"}
            ],
            ...
        ]
    """

    logger.info(f"Downloading audio files for {recording.bvid}...")

    # 使用 BBDown 下载录播音频
    # https://github.com/nilaoda/BBDown
    download_audio(recording)

    # 获取录播文件
    # segments: An array of segment array for each page
    audio_files = []
    for file in os.listdir("."):
        if file.startswith(recording.bvid):
            audio_files.append(file)
    # 按照分p排序
    audio_files.sort()

    logger.info(f"Downloaded {len(audio_files)} audio files for {recording.bvid}")

    segments = []
    for audio_file in audio_files:
        logger.info(f"Transcribing {audio_file}...")

        result = model.transcribe(audio_file, language="zh", verbose=True)

        # 我们只需要 start 和 text，省一点存储空间
        segments.append([
            {
                "start": segment["start"],
                "text": segment["text"]
            }
            for segment in result["segments"]
        ])

    with open(transcription_file, "w") as f:
        json.dump(segments, f)
    
    for audio_file in audio_files:
        os.remove(audio_file)