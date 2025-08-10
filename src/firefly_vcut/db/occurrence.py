import psycopg


def create_occurrences(conn: psycopg.Connection, occurrences: list[dict]) -> int:
    """
    Create occurrences in the database.

    Args:
        conn: A database connection.
        occurrences: A list of occurrences.
            Each occurrence is a dictionary with the following keys:
            - song_id: The song ID.
            - vtuber_song_id: The vtuber song ID.
            - archive_id: The liver recording archive ID.
            - start: The start time of the occurrence, in seconds.
            - page: The page of the occurrence.

    Returns:
        The number of rows created.
    """

    with conn.cursor() as cursor:
        # Split the occurrences into chunks of 50 to avoid db error
        for chunk in [occurrences[i : i + 50] for i in range(0, len(occurrences), 50)]:
            cursor.executemany(
                """
            INSERT INTO "SongOccurrenceInLive" (
                "songId",
                "vtuberSongId",
                "liveRecordingArchiveId",
                "start",
                "page"
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ("vtuberSongId", "liveRecordingArchiveId") DO UPDATE SET
                "start" = EXCLUDED."start",
                "page" = EXCLUDED."page";
            """,
                [
                    (
                        occurrence["song_id"],
                        occurrence["vtuber_song_id"],
                        occurrence["archive_id"],
                        occurrence["start"],
                        occurrence["page"],
                    )
                    for occurrence in chunk
                ],
            )
            conn.commit()
        return cursor.rowcount
