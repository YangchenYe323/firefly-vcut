import os
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import logging
import pytz
import whisper
import tqdm
import boto3
import botocore
import click
import sys

from .fuzz import search_text_in_transcript
from .dblocal import (
    get_all_archives_from_db,
    get_all_occurrences_from_db,
    get_all_vtuber_songs_from_db,
    get_archives_by_bvid,
    get_db_connection,
    get_latest_archives_from_db,
    get_vtuber_song_by_title,
    insert_archives_to_db,
    insert_song_occurrences_to_db,
    update_recording_transcript_and_mark_scanned,
)
from .bilibililocal import get_live_recording_series, get_archives_from_series
from .types import Archive, SongOccurrence
from .transcribe import download_and_transcribe

logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--root",
    type=click.Path(exists=True),
    default="data",
    help="Root directory for vcut operations",
)
@click.option("-v", "--verbose", count=True)
@click.pass_context
def vcut(ctx: click.Context, root: str, verbose: int):
    # Load environment variables from .env file
    load_dotenv()

    ctx.ensure_object(dict)
    ctx.obj["root"] = root

    match verbose:
        case 0:
            logging.basicConfig(level=logging.ERROR)
        case 1:
            logging.basicConfig(level=logging.WARNING)
        case 2:
            logging.basicConfig(level=logging.DEBUG)
        case _:
            logging.basicConfig(level=logging.DEBUG)

@vcut.command()
@click.pass_context
def upload_transcripts_to_r2(ctx: click.Context):
    """
    Upload all the locally stored transcripts to r2, and mark all the recordings as scanned.
    """
    root = ctx.obj["root"]

    r2 = boto3.client(
        service_name="s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    updates = []
    for mid in os.listdir(root):
        for year in os.listdir(f"{root}/{mid}"):
            for month in os.listdir(f"{root}/{mid}/{year}"):
                for recording_dir_name in os.listdir(f"{root}/{mid}/{year}/{month}"):
                    meta_file = f"{root}/{mid}/{year}/{month}/{recording_dir_name}/meta.json"
                    transcript_file = f"{root}/{mid}/{year}/{month}/{recording_dir_name}/segments.json"

                    if not os.path.exists(meta_file) or not os.path.exists(transcript_file):
                        continue
                    
                    with open(meta_file, "r") as f:
                        meta = json.load(f)
                    
                    tkey = transcript_key(mid, meta)

                    try:
                        r2.head_object(
                            Bucket=os.getenv("R2_BUCKET"),
                            Key=tkey,
                        )
                    except botocore.exceptions.ClientError as e:
                        if e.response["Error"]["Code"] == "404":
                            r2.upload_file(
                                transcript_file,
                                os.getenv("R2_BUCKET"),
                                tkey,
                            )
                            print(f"Uploaded {tkey} to r2")
                        else:
                            raise e
                    else:
                        print(f"Object {tkey} already exists in r2, skipping")

                    updates.append({
                        "bvid": meta["bvid"],
                        "transcript_object_key": tkey,
                    })

    with get_db_connection(os.getenv("DATABASE_URL")) as conn:
        update_recording_transcript_and_mark_scanned(conn, updates)

@vcut.command()
@click.option(
    "--mid", type=int, help="Mid of the user to sync archives from", required=True
)
@click.option(
    "--db-url",
    type=str,
    help="URL of the database to sync with, if not provided, will try to read from environment variable DATABASE_URL sourced from .env file",
)
@click.pass_context
def sync_archives(ctx: click.Context, mid: int, db_url: str | None):
    """
    Syncs locally stored live recording archives with the database.

    This command is solely used for the firefly project and works with firefly's database. It might not be useful for other project.

    What the command does:
      - Read all the existing archives from the database
      - Read all the archives we have from local storage following the directory structure described in the help of transcriber command
      - Insert a record into the database for each archive we have locally but not in the database.

    It does NOT upload the transcript, just the metadata of the archive.

    Args:
        mid: Mid of the user to sync archives from
        db_url: URL of the database to sync with, if not provided, will try to read from environment variable DATABASE_URL sourced from .env file
    """
    root = ctx.obj["root"]

    if db_url is None:
        db_url = os.getenv("DATABASE_URL")

    if db_url is None:
        click.echo(
            "没有提供数据库URL, 请使用 --db-url 参数提供或设置环境变量 DATABASE_URL 在 .env 文件中",
            err=True,
        )
        sys.exit(1)

    archives_local = get_all_archives_local(root, mid)
    click.echo(f"从本地存储中获取到 {len(archives_local)} 个直播回放档案")

    with get_db_connection(db_url) as conn:
        archives = get_all_archives_from_db(conn, mid)

        click.echo(f"从数据库中获取到 {len(archives)} 个直播回放档案")

        archive_bvid_set = set([archive.bvid for archive in archives])

        archives_to_insert = [
            archive
            for archive in archives_local
            if archive.bvid not in archive_bvid_set
        ]
        click.echo(f"需要插入 {len(archives_to_insert)} 个直播回放档案")

        if not archives_to_insert:
            click.echo("没有需要插入的直播回放档案")
            return

        insert_archives_to_db(conn, archives_to_insert, mid)

        click.echo("同步完成")


@vcut.command()
@click.option(
    "--mid",
    type=int,
    help="Mid of the user to sync song occurrence from",
    required=True,
)
@click.option(
    "--db-url",
    type=str,
    help="URL of the database to sync with, if not provided, will try to read from environment variable DATABASE_URL sourced from .env file",
)
@click.option(
    "--song",
    type=str,
    help="Title of the song to sync song occurrence from. If not provided, it syncs all the songs in the database.",
)
@click.option(
    "--bvid",
    type=str,
    help="Bilibili video ID of the live recording to sync song occurrence from. If not provided, it syncs all the live recordings in the database.",
)
@click.option(
    "--threshold",
    type=int,
    default=40,
    help="Threshold for the similarity score, the higher the threshold, the more similar the text must be to be considered a match. 40 is a good threshold for my own testing",
)
@click.option(
    "-n",
    "--dry-run",
    is_flag=True,
    help="If set, it will not insert the song occurrence relation into the database, but only print the relation to be inserted.",
)
@click.option(
    "--latest",
    type=int,
    help="If set, it will only search the latest number of archives from the database.",
)
@click.pass_context
def sync_occurrences(
    ctx: click.Context,
    mid: int,
    db_url: str | None,
    song: str | None,
    bvid: str | None,
    threshold: int,
    dry_run: bool,
    latest: int | None,
):
    """
    Syncs song occurrence in live recordings with the database.

    This command is solely used for the firefly project and works with firefly's database. It might not be useful for other project.
    Use the `search` command to do ad-hoc fuzzy search for given text in the transcript.

    What the command does:
        - Get the necessary song data from database, which contains the song title and lyrics_fragment used for fuzzy search.
        - Get the live recordings from the database, which we just need the ID for updating the relation.
        - Retrieve transcript segments for each live recording, and for each song, find the segment where the song is being played
          in the transcript.
        - Insert the song occurrence relation into the database.

    Note:
        - If it is invoked without any arguments, it does not compute occurrences that already exist in the database.
        - If it is invoked with a specific song/bvid, it updates the relation for that song/bvid, overriding the existing entries.

    Args:
        mid: Mid of the user to sync song occurrence from
        db_url: URL of the database to sync with, if not provided, will try to read from environment variable DATABASE_URL sourced from .env file
        song: Title of the song to sync song occurrence from. If not provided, it syncs all the songs in the database.
        bvid: Bilibili video ID of the live recording to sync song occurrence from. If not provided, it syncs all the live recordings in the database.
        dry_run: If set, it will not insert the song occurrence relation into the database, but only print the relation to be inserted.
    """
    root = ctx.obj["root"]

    if db_url is None:
        db_url = os.getenv("DATABASE_URL")

    if db_url is None:
        click.echo(
            "没有提供数据库URL, 请使用 --db-url 参数提供或设置环境变量 DATABASE_URL 在 .env 文件中",
            err=True,
        )
        sys.exit(1)

    force_update = song is not None or bvid is not None

    vtuber_songs = None
    archives = None
    # A hash set of (song_id, archive_id)  where the occurrence exists in the database
    occurrence_set = set()

    with get_db_connection(db_url) as conn:
        if song is not None:
            vtuber_songs = get_vtuber_song_by_title(conn, song, mid)
        else:
            vtuber_songs = get_all_vtuber_songs_from_db(conn, mid)

        if bvid is not None:
            archives = get_archives_by_bvid(conn, bvid)  # noqa: F821

        elif latest is not None:
            archives = get_latest_archives_from_db(conn, mid, latest)
        else:
            archives = get_all_archives_from_db(conn, mid)

        if not force_update:
            occurrences = get_all_occurrences_from_db(conn, mid)
            occurrence_set = set(
                [
                    (occurrence.vtuber_song_id, occurrence.archive_id)
                    for occurrence in occurrences
                ]
            )

    if not vtuber_songs:
        click.echo("没有找到歌曲", err=True)
        sys.exit(1)

    if not archives:
        click.echo("没有找到直播回放", err=True)
        sys.exit(1)

    with get_db_connection(db_url) as conn:
        new_occurrences = []
        for archive in archives:
            transcript = find_transcript_from_bvid(root, archive.bvid)
            if transcript is None:
                logger.debug(f"没有找到 {archive.bvid} 的转写结果, 跳过")
                continue

            if len(transcript) == 0:
                raise ValueError(f"没有找到 {archive.bvid} 的转写结果")

            for song in vtuber_songs:
                if (song.vtuber_song_id, archive.id) in occurrence_set:
                    logger.debug(f"已存在 {song.title} 在 {archive.bvid} 的记录, 跳过")
                    continue

                result = search_text_in_transcript(transcript, song.lyrics_fragment)
                if result is None:
                    logger.debug(
                        f"没有找到 {song.title} 在 {archive.bvid} 的记录, 跳过"
                    )
                    continue
                start, page, score, text = result

                if score < threshold:
                    logger.debug(
                        f"歌曲 {song.title} 在 {archive.bvid} 的相似度 {score} 低于阈值 {threshold}, 跳过"
                    )
                    logger.debug(f"片段: {text}")
                    continue

                click.echo(
                    f"{song.title} 在 {archive.bvid} 的记录: P{page}, {normalize_seconds(start)}, similarity: {score}"
                )
                click.echo(f"{text}")

                new_occurrences.append(
                    SongOccurrence(
                        song_id=song.song_id,
                        vtuber_song_id=song.vtuber_song_id,
                        archive_id=archive.id,
                        start=start,
                        page=page,
                    )
                )

                if len(new_occurrences) > 100 and not dry_run:
                    click.echo(f"需要插入 {len(new_occurrences)} 个歌曲出现记录")
                    with get_db_connection(db_url) as conn:
                        insert_song_occurrences_to_db(conn, new_occurrences)
                    click.echo(f"插入 {len(new_occurrences)} 个歌曲出现记录完成")
                    new_occurrences = []

        if dry_run:
            return

        click.echo(f"需要插入 {len(new_occurrences)} 个歌曲出现记录")

        with get_db_connection(db_url) as conn:
            insert_song_occurrences_to_db(conn, new_occurrences)

        click.echo("插入完成")


@vcut.command()
@click.option(
    "--mid",
    type=int,
    help="Mid of the user to download and transcribe recordings from",
    required=True,
)
@click.option(
    "--model",
    type=click.Choice(["tiny", "base", "small", "medium", "large", "turbo"]),
    default="turbo",
    help="Model to use for transcription",
)
@click.pass_context
def transcriber(ctx: click.Context, mid: int, model: str):
    """
    Download and transcribe **ALL** live recordings of a given user.

    This script will download and transcribe all the recordings of the specified user. Only the transcript is saved
    on disk and the audio files are deleted to save space. It stores the transcripts using the following directory structure:

    <root>/<mid>/<year>/<month>/<year>-<month>-<day>_<hour>-<minute>-<second>_<bvid>/[meta.json, segments.json]

    where:
        - meta.json: Metadata about the recording archive
        - segments.json: Tanscript of all the pages of the recording, which is a JSON array of arrays of segments,
          each segment is a JSON object with the following fields:
             - start: The start time of the segment in seconds
             - text: The text of the segment

    The directory is assumed to be persistent across runs, the script skips downloading and processing if the segment file
    already exists. It can be safely interrupted and rerun to make sure all the recordings are continuously processed.

    It depends on BBDown for downloading the audio files, which can be installed from https://github.com/nilaoda/BBDown. Make sure
    the executable is in the PATH when running the script.

    It uses openai-whisper (https://github.com/openai/whisper) for transcribing the audio files, and will work best on machines with
    GPU and CUDA support. See https://github.com/openai/whisper about specific VRAM requirements of different models.

    Args:
        root: Root directory for vcut operations
        mid: Mid of the user to download and transcribe recordings from
    """
    root = ctx.obj["root"]

    os.makedirs(f"{root}/{mid}", exist_ok=True)

    logger.info(f"Loading model {model}...")
    model = whisper.load_model(model)

    china_tz = timezone(timedelta(hours=8))
    series = get_live_recording_series(mid)
    if series is None:
        click.echo(f"没有找到 {mid} 的 直播回放 系列", err=True)
        sys.exit(1)

    archives = get_archives_from_series(mid, series.series_id)

    if not archives:
        click.echo(f"没有获取到 {mid} 的 直播回放 系列的回放视频", err=True)
        sys.exit(1)

    total_gpu_time = 0
    total_archive_duration = 0

    for archive in tqdm.tqdm(archives):
        pubdate = datetime.fromtimestamp(archive.pubdate, tz=china_tz)
        pubdate_name = pubdate.strftime("%Y-%m-%d_%H-%M-%S")

        recording_dir_name = f"{pubdate_name}_{archive.bvid}"
        os.makedirs(
            f"{root}/{mid}/{pubdate.year}/{pubdate.month:02d}/{recording_dir_name}",
            exist_ok=True,
        )

        meta_file = f"{root}/{mid}/{pubdate.year}/{pubdate.month:02d}/{recording_dir_name}/meta.json"
        if not os.path.exists(meta_file):
            with open(meta_file, "w") as f:
                json.dump(
                    {
                        "bvid": archive.bvid,
                        "title": archive.title,
                        "pubdate": archive.pubdate,
                        "cover": archive.cover,
                        "duration": archive.duration,
                    },
                    f,
                )

        transcription_file = f"{root}/{mid}/{pubdate.year}/{pubdate.month:02d}/{recording_dir_name}/segments.json"
        if not os.path.exists(transcription_file):
            try:
                gpu_time = download_and_transcribe(archive, transcription_file, model)
                total_gpu_time += gpu_time
                total_archive_duration += archive.duration
            except Exception as e:
                logger.error(f"下载和转写 {archive.bvid} 失败, 跳过: {e}")
                continue
        else:
            logger.info(f"已存在 {recording_dir_name} 的转写结果, 跳过")

    click.echo(f"总GPU时间: {total_gpu_time} 秒")
    click.echo(f"总时长: {total_archive_duration} 秒")
    if total_gpu_time > 0:
        click.echo(f"每秒GPU时间可以处理 {total_archive_duration / total_gpu_time} 秒录播时长")


@vcut.command()
@click.option("--bvid", type=str, help="Bilibili video ID", required=True)
@click.option(
    "--text", type=str, help="Text to fuzzy search in the transcript", required=True
)
@click.pass_context
def search(ctx: click.Context, bvid: str, text: str):
    """
    Fuzzy search for the given lyric in the transcript.

    Args:
        bvid: Bilibili video ID
        lyric: Lyric to fuzzy search in the transcript

    Returns:
        None
    """
    root = ctx.obj["root"]
    transcript = find_transcript_from_bvid(root, bvid)
    if transcript is None:
        click.echo(f"没有找到 {bvid} 的转写结果", err=True)
        sys.exit(1)

    results = search_text_in_transcript(transcript, text)

    start, page, score, text = results
    click.echo(f"P{page} 最相似的片段: {text}")
    click.echo(f"相似度: {score}")
    click.echo(f"开始时间: {start}")


def get_all_archives_local(root: str, mid: int) -> list[Archive]:
    archives = []
    for year in os.listdir(f"{root}/{mid}"):
        for month in os.listdir(f"{root}/{mid}/{year}"):
            for recording_dir_name in os.listdir(f"{root}/{mid}/{year}/{month}"):
                meta_file = (
                    f"{root}/{mid}/{year}/{month}/{recording_dir_name}/meta.json"
                )
                if os.path.exists(meta_file):
                    with open(meta_file, "r") as f:
                        meta = json.load(f)
                        archives.append(
                            Archive(
                                id=None,
                                bvid=meta["bvid"],
                                title=meta["title"],
                                pubdate=meta["pubdate"],
                                duration=meta["duration"],
                                cover=meta["cover"],
                            )
                        )
    return archives


def find_transcript_from_bvid(root: str, bvid: str) -> dict | None:
    for mid in os.listdir(root):
        for year in os.listdir(f"{root}/{mid}"):
            for month in os.listdir(f"{root}/{mid}/{year}"):
                for recording_dir_name in os.listdir(f"{root}/{mid}/{year}/{month}"):
                    if bvid in recording_dir_name:
                        transcript_file = f"{root}/{mid}/{year}/{month}/{recording_dir_name}/segments.json"
                        if not os.path.exists(transcript_file):
                            return None
                        with open(
                            f"{root}/{mid}/{year}/{month}/{recording_dir_name}/segments.json",
                            "r",
                        ) as f:
                            return json.load(f)
    return None


def normalize_seconds(seconds):
    """
    Convert seconds to hh:mm:ss format.

    Args:
        seconds (int or float): Number of seconds to convert

    Returns:
        str: Time in hh:mm:ss format
    """
    # Convert to integer to handle potential float inputs
    seconds = int(seconds)

    # Calculate hours, minutes, and remaining seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    remaining_seconds = seconds % 60

    # Format with leading zeros
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"

def transcript_key(mid: int, recording: dict) -> str:
    """
    Generate a transcript object key from a recording.

    Args:
        mid: Mid of the user
        recording: Recording metadata, which matches the meta.json file in the data directory populated by
        the transcriber command.

    Returns:
        str: Transcript object key
    """

    bvid = recording["bvid"]
    pubdate = recording["pubdate"]
    # Extract year, month, day from pudate in tz Asia/Shanghai
    pubdate = datetime.fromtimestamp(pubdate, tz=pytz.timezone("Asia/Shanghai"))
    year = pubdate.year
    month = pubdate.month
    day = pubdate.day

    return f"transcripts/{mid}/{year}/{month:02d}/{day:02d}/{bvid}.json"