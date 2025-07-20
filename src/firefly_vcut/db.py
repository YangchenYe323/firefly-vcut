import psycopg2
from contextlib import contextmanager
from psycopg2.extras import execute_values
from .types import Archive, VtuberSong, SongOccurrence


@contextmanager
def get_db_connection(db_url: str):
    conn = psycopg2.connect(db_url)
    try:
        yield conn
    finally:
        conn.close()


def get_all_archives_from_db(conn: psycopg2.extensions.connection, mid: int) -> list[Archive]:
    mid = str(mid)

    archives = []
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT a.id, a.bvid, a.title, a.pubdate, a.duration, a.cover FROM "LiveRecordingArchive" a JOIN "VtuberProfile" v ON a."vtuberProfileId" = v."id" WHERE v."mid" = %s;',
            (mid,)
        )
        for id, bvid, title, pubdate, duration, cover in cursor:
            archives.append(
                Archive(
                    id=id,
                    bvid=bvid,
                    title=title,
                    pubdate=pubdate,
                    duration=duration,
                    cover=cover,
                )
            )
    return archives

def get_latest_archives_from_db(conn: psycopg2.extensions.connection, mid: int, count: int) -> list[Archive]:
    mid = str(mid)

    archives = []
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT a.id, a.bvid, a.title, a.pubdate, a.duration, a.cover FROM "LiveRecordingArchive" a JOIN "VtuberProfile" v ON a."vtuberProfileId" = v."id" WHERE v."mid" = %s ORDER BY id DESC LIMIT %s', (mid, count))
        for id, bvid, title, pubdate, duration, cover in cursor:
            archives.append(Archive(id=id, bvid=bvid, title=title, pubdate=pubdate, duration=duration, cover=cover))
    return archives

def get_archives_by_bvid(
    conn: psycopg2.extensions.connection, bvid: str
) -> list[Archive]:
    archives = []
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT id, bvid, title, pubdate, duration, cover FROM "LiveRecordingArchive" WHERE bvid = %s',
            (bvid,),
        )
        for id, bvid, title, pubdate, duration, cover in cursor:
            archives.append(
                Archive(
                    id=id,
                    bvid=bvid,
                    title=title,
                    pubdate=pubdate,
                    duration=duration,
                    cover=cover,
                )
            )
    return archives


def get_all_vtuber_songs_from_db(conn: psycopg2.extensions.connection, mid: int) -> list[VtuberSong]:
    mid = str(mid)

    stmt = """
    SELECT s1."id", s2."id", s1."title", s1."lyricsFragment"
    FROM "Song" s1 JOIN "VtuberSong" s2 ON s1."id" = s2."songId" JOIN "VtuberProfile" v ON s2."vtuberProfileId" = v."id"
    WHERE v."mid" = %s AND s1."lyricsFragment" IS NOT NULL AND s1."lyricsFragment" != ''
    """

    songs = []
    with conn.cursor() as cursor:
        cursor.execute(stmt, (mid,))
        for song_id, vtuber_song_id, title, lyrics_fragment in cursor:
            songs.append(VtuberSong(song_id=song_id, vtuber_song_id=vtuber_song_id, title=title, lyrics_fragment=lyrics_fragment))
    return songs


def get_vtuber_song_by_title(conn: psycopg2.extensions.connection, title: str, mid: int) -> list[VtuberSong]:
    mid = str(mid)

    stmt = """
    SELECT s1."id", s2."id", s1."title", s1."lyricsFragment"
    FROM "Song" s1 JOIN "VtuberSong" s2 ON s1."id" = s2."songId" JOIN "VtuberProfile" v ON s2."vtuberProfileId" = v."id"
    WHERE v."mid" = %s AND s1."title" = %s AND s1."lyricsFragment" IS NOT NULL AND s1."lyricsFragment" != ''
    """
    songs = []
    with conn.cursor() as cursor:
        cursor.execute(stmt, (mid, title))
        for song_id, vtuber_song_id, title, lyrics_fragment in cursor:
            songs.append(VtuberSong(song_id=song_id, vtuber_song_id=vtuber_song_id, title=title, lyrics_fragment=lyrics_fragment))
    return songs


def get_all_occurrences_from_db(
    conn: psycopg2.extensions.connection,
    mid: int,
) -> list[SongOccurrence]:
    """
    Retrieve all song occurrences from the database.

    Note: Double quotes around column names are required because PostgreSQL treats unquoted
    identifiers as lowercase by default. Since our table uses camelCase column names
    ("songId", "liveRecordingArchiveId"), we must quote them to preserve the exact case.
    """

    mid = str(mid)

    stmt = """
    SELECT s1."songId", s1."vtuberSongId", s1."liveRecordingArchiveId", s1."start", s1."page"
    FROM "SongOccurrenceInLive" s1 JOIN "VtuberSong" s2 ON s1."vtuberSongId" = s2."id" JOIN "VtuberProfile" v ON s2."vtuberProfileId" = v."id"
    WHERE v."mid" = %s
    """

    occurrences = []
    with conn.cursor() as cursor:
        cursor.execute(stmt, (mid,))
        for song_id, vtuber_song_id, archive_id, start, page in cursor:
            occurrences.append(
                SongOccurrence(
                    song_id=song_id,
                    vtuber_song_id=vtuber_song_id,
                    archive_id=archive_id,
                    start=start,
                    page=page,
                )
            )
    return occurrences


def insert_archives_to_db(
    conn: psycopg2.extensions.connection,
    archives: list[Archive],
    mid: int,
):
    mid = str(mid)

    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT "id" FROM "VtuberProfile" WHERE "mid" = %s', (mid,)
        )
        vtuber_profile_id = cursor.fetchone()[0]

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO "LiveRecordingArchive" ("vtuberProfileId", "bvid", "title", "pubdate", "duration", "cover") VALUES %s
            ON CONFLICT (bvid) DO NOTHING;
            """,
            [
                (
                    vtuber_profile_id,
                    archive.bvid,
                    archive.title,
                    archive.pubdate,
                    archive.duration,
                    archive.cover,
                )
                for archive in archives
            ],
        )
        conn.commit()


def insert_song_occurrences_to_db(
    conn: psycopg2.extensions.connection, occurrences: list[SongOccurrence]
):
    """
    Insert song occurrences into the database with upsert functionality.

    Note: Double quotes around column names are required because PostgreSQL treats unquoted
    identifiers as lowercase by default. Since our table uses camelCase column names
    ("songId", "liveRecordingArchiveId"), we must quote them to preserve the exact case.
    """
    with conn.cursor() as cursor:
        # Split the occurrences into chunks of 50 to avoid db error
        for chunk in [occurrences[i : i + 50] for i in range(0, len(occurrences), 50)]:
            execute_values(
                cursor,
                """
            INSERT INTO "SongOccurrenceInLive" ("songId", "vtuberSongId", "liveRecordingArchiveId", "start", "page") VALUES %s
            ON CONFLICT ("vtuberSongId", "liveRecordingArchiveId") DO UPDATE SET
                "start" = EXCLUDED."start",
                "page" = EXCLUDED."page";
            """,
                [
                    (
                        occurrence.song_id,
                        occurrence.vtuber_song_id,
                        occurrence.archive_id,
                        occurrence.start,
                        occurrence.page,
                    )
                    for occurrence in chunk
                ],
            )
            conn.commit()
