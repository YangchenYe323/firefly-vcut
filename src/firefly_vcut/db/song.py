import psycopg
from psycopg.rows import dict_row

def list_latest_bvid_by_vtuber(conn: psycopg.Connection) -> list[dict]:
    """
    List the latest bvid known for each vtuber. If a vtuber has no known bvid, return None.

    Args:
        conn: A database connection.

    Returns:
        A list of entries.
        Each entry is a dictionary of the following keys:
        - vtuber_profile_id: The vtuber profile ID.
        - mid: The vtuber mid.
        - latest_video_pubdate: The publication date of the latest video, unix epoch timestamp.
    """
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                vp."id" as "vtuber_profile_id",
                vp."mid" as "mid",
                MAX(vs."pubdate") as "latest_video_pubdate"
            FROM "VtuberProfile" vp
            LEFT JOIN "VtuberSong" vs ON vs."vtuberProfileId" = vp."id"
            GROUP BY vp."id";
        """)
        return cursor.fetchall()

def list_vtuber_songs_by_vtuber_profile_id(conn: psycopg.Connection) -> list[dict]:
    """
    List a flattened list of vtuber songs

    Args:
        conn: A database connection.
        vtuber_profile_id: The vtuber profile ID.

    Returns:
        A list of vtuber songs.
        Each vtuber song is a dictionary with the following keys:
        - vtuber_profile_id: The vtuber profile ID.
        - id: The song ID.
        - vtuber_song_id: The vtuber song ID.
        - title: The title of the song.
    """

    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                vs."vtuberProfileId" as "vtuber_profile_id",
                s."id" as "id",
                vs."id" as "vtuber_song_id",
                s."title" as "title"
            FROM "VtuberSong" vs
            JOIN "Song" s ON vs."songId" = s."id";
        """)
        return cursor.fetchall()

def update_bvid(conn: psycopg.Connection, entries: list[dict]) -> int:
    """
    Update the bvid and update of the given vtuber songs to reflect
    new uploaded song videos.

    Args:
        conn: A database connection.
        entries: A list of entries.
        Each entry is a dictionary with the following keys:
        - vtuber_song_id: The vtuber song ID.
        - bvid: The Bilibili video ID.
        - pubdate: The publication date of the video, unix epoch timestamp.

    Returns:
        The number of updated entries.
    """
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            UPDATE "VtuberSong" vs
            SET "bvid" = %s, "pubdate" = %s
            WHERE vs."id" = %s;
            """,
            [(entry["bvid"], entry["pubdate"], entry["vtuber_song_id"]) for entry in entries]
        )
        conn.commit()
        return cursor.rowcount

def list_all_songs_with_vtuber_song_ids(conn: psycopg.Connection) -> list[dict]:
    """
    List all songs in the database. 

    Args:
        conn: A database connection.

    Returns:
        A list of songs.
        Each song is a dictionary with the following keys:
        - id: The song ID.
        - vtuber_song_ids: A list of VtuberSong ids associated with this song.
        - vtuber_profile_ids: A list of VtuberProfile ids associated with this song.
        - title: The title of the song.
        - lyrics_fragment: The lyrics of the song.
    """

    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                s."id" as "id",
                s."title" as "title",
                s."lyricsFragment" as "lyrics_fragment",
                ARRAY_AGG(vs."id") as "vtuber_song_ids",
                ARRAY_AGG(vs."vtuberProfileId") as "vtuber_profile_ids"
            FROM "Song" s
            LEFT JOIN "VtuberSong" vs ON s."id" = vs."songId"
            GROUP BY s."id";
        """)
        return cursor.fetchall()

def list_songs_by_title(conn: psycopg.Connection, title: str) -> list[dict] | None:
    """
    List songs by title.

    Args:
        conn: A database connection.
        title: The title of the song.

    Returns:
        A song.
        A dictionary with the following keys:
        - id: The song ID.
        - vtuber_song_ids: A list of VtuberSong ids associated with this song.
        - vtuber_profile_ids: A list of VtuberProfile ids associated with this song.
        - title: The title of the song.
        - lyrics_fragment: The lyrics of the song.
    """
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                s."id" as "id",
                s."title" as "title",
                s."lyricsFragment" as "lyrics_fragment",
                ARRAY_AGG(vs."id") as "vtuber_song_ids",
                ARRAY_AGG(vs."vtuberProfileId") as "vtuber_profile_ids"
            FROM "Song" s
            LEFT JOIN "VtuberSong" vs ON s."id" = vs."songId"
            WHERE s."title" = %s
            GROUP BY s."id";
        """, (title,))
        return cursor.fetchall()