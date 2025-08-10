from datetime import datetime
import re
import psycopg
from psycopg.rows import dict_row
import pytz

def create_recordings(conn: psycopg.Connection, recordings: list[dict], mid: int) -> int:
    """
    Create new recordings in the database.

    Args:
        conn: A database connection.
        mid: The vtuber mid of the recordings.
        recording: A dictionary with the following keys:
            - title: The title of the recording.
            - bvid: The Bilibili video ID.
            - pubdate: The publication date of the video, unix epoch timestamp, meant to be consumed
            - duration: The duration of the recording in seconds.
            - cover: The cover image of the recording.
    """

    def extract_datetime_from_title(title: str) -> datetime | None:
        """
        Bilibili live recordings are named like "2025年8月10日1点场 <title>", which is the only way of knowing
        when the live happened instead of when the recording was published, so we try to extract the datetime from the title.
        """
        tz = pytz.timezone("Asia/Shanghai")
        pattern = r"(\d{4})年(\d{1,2})月(\d{1,2})日(\d{1,2})点场"
        match = re.search(pattern, title)
        if match:
            year, month, day, hour = map(int, match.groups())
            return tz.localize(datetime(year, month, day, hour, 0, 0))
        return None

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT "id" FROM "VtuberProfile" WHERE "mid" = %s;
        """,
            (mid,),
        )
        vtuber_profile_id = cursor.fetchone()[0]

    with conn.cursor() as cursor:
        cursor.executemany(
            """
                INSERT INTO "LiveRecordingArchive" ("title", "bvid", "vtuberProfileId", "pubdate", "date", "duration", "cover")
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("bvid") DO NOTHING;
            """,
            [
                (
                    recording["title"],
                    recording["bvid"],
                    vtuber_profile_id,
                    recording["pubdate"],
                    extract_datetime_from_title(recording["title"]),
                    recording["duration"],
                    recording["cover"],
                )
                for recording in recordings
            ],
        )
        conn.commit()
        return cursor.rowcount

def list_latest_and_oldest_recordings(conn: psycopg.Connection) -> list[dict]:
    """
    List each vtuber's latest and oldest recording recorded in the database.

    Args:
        conn: A database connection.

    Returns:
        A list of entries.
        Each entry is a dictionary of the following keys:
        - mid: The vtuber mid.
        - latest_pubdate: The publication date of the video, unix epoch timestamp, meant to be consumed
            as timezone aware datetime in timezone Asia/Shanghai, or None if no recording exists.
        - oldest_pubdate: The publication date of the video, unix epoch timestamp, meant to be consumed
            as timezone aware datetime in timezone Asia/Shanghai, or None if no recording exists.
    """
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                v."mid" as "mid",
                MAX(l."pubdate") as "latest_pubdate",
                MIN(l."pubdate") as "oldest_pubdate"
            FROM "VtuberProfile" v
            LEFT JOIN "LiveRecordingArchive" l ON l."vtuberProfileId" = v."id"
            GROUP BY v."id";
        """)
        return cursor.fetchall()

def list_recordings_to_stream(conn: psycopg.Connection, limit: int = 10) -> list[dict]:
    """
    List recordings that need to be streamed.

    A recording needs to be streamed if:
    - it's "transcriptObjectKey" is null
    - it's "audioObjectKeys" is null
    - it's "lastSongOccurrenceScan" is null

    Args:
        conn: A database connection.
        limit: The maximum number of recordings to return.

    Returns:
        A list of recordings that need to be streamed.
        Each recording is a dictionary with the following keys:
        - id: The recording ID.
        - title: The title of the recording.
        - bvid: The Bilibili video ID.
        - mid: The vtuber mid of the recording.
        - pubdate: The publication date of the video, unix epoch timestamp, meant to be consumed
            as timezone aware datetime in timezone Asia/Shanghai.
    """
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                l."id" as "id",
                l."title" as "title",
                l."bvid" as "bvid",
                v."mid" as "mid",
                l."pubdate" as "pubdate"
            FROM "LiveRecordingArchive" l
            JOIN "VtuberProfile" v ON l."vtuberProfileId" = v."id"
            WHERE l."transcriptObjectKey" IS NULL
            AND (
                l."audioObjectKeys" IS NULL
                OR array_length(l."audioObjectKeys", 1) = 0
            )
            AND l."lastSongOccurrenceScan" IS NULL
            ORDER BY l."pubdate" DESC
            LIMIT %s;
        """, (limit,))
        return cursor.fetchall()


def list_recordings_to_transcribe(conn: psycopg.Connection) -> list[dict]:
    """
    List recordings that need to be transcribed.

    A recording needs to be transcribed if:
    - it's "transcriptObjectKey" is null
    - it's "audioObjectKeys" is not null and not empty

    Args:
        conn: A database connection.

    Returns:
        A list of recordings that need to be transcribed.
        Each recording is a dictionary with the following keys:
        - id: The recording ID.
        - title: The title of the recording.
        - bvid: The Bilibili video ID.
        - mid: The vtuber mid of the recording.
        - pubdate: The publication date of the video, unix epoch timestamp, meant to be consumed
            as timezone aware datetime in timezone Asia/Shanghai.
        - audioObjectKeys: A list of audio object keys (path in object store bucket for the audio files, one for each page)
    """
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                l."id" as "id",
                l."title" as "title",
                l."bvid" as "bvid",
                v."mid" as "mid",
                l."pubdate" as "pubdate",
                l."audioObjectKeys" as "audioObjectKeys"
            FROM "LiveRecordingArchive" l
            JOIN "VtuberProfile" v ON l."vtuberProfileId" = v."id"
            WHERE l."transcriptObjectKey" IS NULL
            AND l."audioObjectKeys" IS NOT NULL
            AND array_length(l."audioObjectKeys", 1) > 0;
        """)
        return cursor.fetchall()

def list_recordings_to_populate_occurrences(conn: psycopg.Connection) -> list[dict]:
    """
    List recordings that need to be populated with occurrences.

    A recording needs to be populated with occurrences if:
    - it's "lastSongOccurrenceScan" is null
    - it's "transcriptObjectKey" is not null

    Args:
        conn: A database connection.

    Returns:
        A list of recordings that need to be populated with occurrences.
        Each recording is a dictionary with the following keys:
        - id: The recording ID.
        - bvid: The Bilibili video ID.
        - transcriptObjectKey: The object key of the transcript in the object store.
    """
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT 
                l."id" as "id",
                l."bvid" as "bvid",
                l."transcriptObjectKey" as "transcriptObjectKey"
            FROM "LiveRecordingArchive" l
            WHERE l."lastSongOccurrenceScan" IS NULL
            AND l."transcriptObjectKey" IS NOT NULL;
        """)
        return cursor.fetchall()

def update_recording_audio_object_keys(conn: psycopg.Connection, recording_id: int, audio_object_keys: list[str]) -> int:
    """
    Update a recording's audio object keys.

    Args:
        conn: A database connection.
        recording_id: The ID of the recording to update.
        audio_object_keys: A list of audio object keys (path in object store bucket for the audio files, one for each page)

    Returns:
        The number of rows updated.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE "LiveRecordingArchive" SET "audioObjectKeys" = %s WHERE "id" = %s;
        """, (audio_object_keys, recording_id))
        conn.commit()
        return cursor.rowcount

def update_recording_transcript(
    conn: psycopg.Connection,
    recording_id: int,
    transcript_object_key: str,
):
    """
    Update a recording's transcript, and also remove the audio object keys
    as we're going to remove the files from the object store.

    Args:
        conn: A database connection.
        recording_id: The ID of the recording to update.
        transcript_object_key: The object key of the transcript in the object store.

    Returns:
        The number of rows updated.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE "LiveRecordingArchive" SET "transcriptObjectKey" = %s, "audioObjectKeys" = NULL WHERE "id" = %s;
        """,
            (transcript_object_key, recording_id),
        )
        conn.commit()
        return cursor.rowcount

def mark_recording_scanned(conn: psycopg.Connection, recording_id: int) -> int:
    """
    Mark a recording as scanned.

    Args:
        conn: A database connection.
        recording_id: The ID of the recording to mark as scanned.
    """
    with conn.cursor() as cursor:  
        cursor.execute("""
            UPDATE "LiveRecordingArchive" SET "lastSongOccurrenceScan" = NOW() WHERE "id" = %s;
        """, (recording_id,))
        conn.commit()
        return cursor.rowcount

