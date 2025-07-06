# Relevant SQL migration files:
#
# -- CreateTable
# CREATE TABLE "Song" (
#     "id" SERIAL NOT NULL,
#     "title" TEXT NOT NULL,
#     "artist" TEXT NOT NULL,
#     "remark" TEXT NOT NULL,
#     "extra" JSONB NOT NULL,
#     "created_on" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
#     "lang" TEXT[],
#     "tag" TEXT[],
#     "url" TEXT,

#     CONSTRAINT "Song_pkey" PRIMARY KEY ("id")
# );
#
# ALTER TABLE "Song" ADD COLUMN     "lyrics_fragment" TEXT;
#
# -- CreateTable
# CREATE TABLE "LiveRecordingArchive" (
#     "id" SERIAL NOT NULL,
#     "bvid" TEXT NOT NULL,
#     "title" TEXT NOT NULL,
#     "pubdate" INTEGER NOT NULL,
#     "duration" INTEGER NOT NULL,
#     "cover" TEXT NOT NULL,

#     CONSTRAINT "LiveRecordingArchive_pkey" PRIMARY KEY ("id")
# );

# -- CreateTable
# CREATE TABLE "SongOccurrenceInLive" (
#     "songId" INTEGER NOT NULL,
#     "liveRecordingArchiveId" INTEGER NOT NULL,
#     "start" INTEGER NOT NULL,
#     "page" INTEGER NOT NULL,

#     CONSTRAINT "SongOccurrenceInLive_pkey" PRIMARY KEY ("songId","liveRecordingArchiveId")
# );

# -- CreateIndex
# CREATE UNIQUE INDEX "LiveRecordingArchive_bvid_key" ON "LiveRecordingArchive"("bvid");

# -- CreateIndex
# CREATE INDEX "LiveRecordingArchive_bvid_pubdate_idx" ON "LiveRecordingArchive"("bvid", "pubdate" DESC);

# -- AddForeignKey
# ALTER TABLE "SongOccurrenceInLive" ADD CONSTRAINT "SongOccurrenceInLive_songId_fkey" FOREIGN KEY ("songId") REFERENCES "Song"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

# -- AddForeignKey
# ALTER TABLE "SongOccurrenceInLive" ADD CONSTRAINT "SongOccurrenceInLive_liveRecordingArchiveId_fkey" FOREIGN KEY ("liveRecordingArchiveId") REFERENCES "LiveRecordingArchive"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

import psycopg2
from contextlib import contextmanager
from psycopg2.extras import execute_values
from .types import Archive, Song, SongOccurrence


@contextmanager
def get_db_connection(db_url: str):
    conn = psycopg2.connect(db_url)
    try:
        yield conn
    finally:
        conn.close()


def get_all_archives_from_db(conn: psycopg2.extensions.connection) -> list[Archive]:
    archives = []
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT id, bvid, title, pubdate, duration, cover FROM "LiveRecordingArchive";'
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


def get_all_songs_from_db(conn: psycopg2.extensions.connection) -> list[Song]:
    songs = []
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT id, title, lyrics_fragment FROM \"Song\" WHERE lyrics_fragment IS NOT NULL AND lyrics_fragment != ''"
        )
        for id, title, lyrics_fragment in cursor:
            songs.append(Song(id=id, title=title, lyrics_fragment=lyrics_fragment))
    return songs


def get_song_by_title(conn: psycopg2.extensions.connection, title: str) -> list[Song]:
    songs = []
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT id, title, lyrics_fragment FROM "Song" WHERE title = %s', (title,)
        )
        for id, title, lyrics_fragment in cursor:
            songs.append(Song(id=id, title=title, lyrics_fragment=lyrics_fragment))
    return songs


def get_all_occurrences_from_db(
    conn: psycopg2.extensions.connection,
) -> list[SongOccurrence]:
    """
    Retrieve all song occurrences from the database.

    Note: Double quotes around column names are required because PostgreSQL treats unquoted
    identifiers as lowercase by default. Since our table uses camelCase column names
    ("songId", "liveRecordingArchiveId"), we must quote them to preserve the exact case.
    """
    occurrences = []
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT s."songId", s."liveRecordingArchiveId", s."start", s."page" FROM "SongOccurrenceInLive" as s;'
        )
        for songId, liveRecordingArchiveId, start, page in cursor:
            occurrences.append(
                SongOccurrence(
                    song_id=songId,
                    archive_id=liveRecordingArchiveId,
                    start=start,
                    page=page,
                )
            )
    return occurrences


def insert_archives_to_db(
    conn: psycopg2.extensions.connection, archives: list[Archive]
):
    with conn.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO "LiveRecordingArchive" (bvid, title, pubdate, duration, cover) VALUES %s
            ON CONFLICT (bvid) DO NOTHING;
            """,
            [
                (
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
            INSERT INTO "SongOccurrenceInLive" ("songId", "liveRecordingArchiveId", "start", "page") VALUES %s
            ON CONFLICT ("songId", "liveRecordingArchiveId") DO UPDATE SET
                "start" = EXCLUDED."start",
                "page" = EXCLUDED."page";
            """,
                [
                    (
                        occurrence.song_id,
                        occurrence.archive_id,
                        occurrence.start,
                        occurrence.page,
                    )
                    for occurrence in chunk
                ],
            )
            conn.commit()
