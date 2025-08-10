import psycopg
from psycopg.rows import dict_row

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
        - title: The title of the song.
        - lyrics_fragment: The lyrics of the song.
    """

    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT
                s."id" as "id",
                s."title" as "title",
                s."lyricsFragment" as "lyrics_fragment",
                ARRAY_AGG(vs."id") as "vtuber_song_ids"
            FROM "Song" s
            LEFT JOIN "VtuberSong" vs ON s."id" = vs."songId"
            GROUP BY s."id";
        """)
        return cursor.fetchall()
