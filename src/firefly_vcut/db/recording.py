import psycopg
from psycopg.rows import dict_row

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
        cursor.execute("""
            UPDATE "LiveRecordingArchive" SET "transcriptObjectKey" = %s, "audioObjectKeys" = NULL WHERE "id" = %s;
        """, (transcript_object_key, recording_id))
        conn.commit()
        return cursor.rowcount
