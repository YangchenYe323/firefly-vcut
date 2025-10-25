import os
import json

from .app import app, bucket_volume, BUCKET_DIR, secret

@app.function(
    timeout=30 * 60,  # 30 minutes
    secrets=[secret],
    volumes={
        BUCKET_DIR: bucket_volume,
    },
)
def backfill_occurrences(song_title: str, backfill_limit: int = 10):
    import firefly_vcut.db as db
    from firefly_vcut.fuzz import search_text_in_transcript

    with db.connection(os.getenv("DATABASE_URL")) as conn:
        songs = db.song.list_songs_by_title(conn, song_title)
        if not songs:
            print(f"No songs found for {song_title}")
            return
        recordings = db.recording.list_latest_recordings(conn, backfill_limit)
        if not recordings:
            print("No recordings found to backfill")
            return

    occurrences = []
    for recording in recordings:
        print(f"Backfilling occurrences for {recording['bvid']}")
        transcript_object_key = recording["transcriptObjectKey"]
        transcript_path = os.path.join(BUCKET_DIR, transcript_object_key)
        with open(transcript_path, "r") as f:
            transcript = json.load(f)

        for song in songs:
            entry = search_text_in_transcript(transcript, song["lyrics_fragment"])
            if entry is None:
                continue

            start, page, score, text = entry
            if score < 40:
                continue
            print(
                f"Found {song['title']} in {recording['bvid']} at page {page} at {start} with score {score}"
            )
            print(f"Matched text: {text}")

            for i, vtuber_song_id in enumerate(song["vtuber_song_ids"]):
                vtuber_profile_id = song["vtuber_profile_ids"][i]
                if vtuber_profile_id != recording["vtuber_profile_id"]:
                    continue

                occurrences.append(
                    {
                        "song_id": song["id"],
                        "vtuber_song_id": vtuber_song_id,
                        "archive_id": recording["id"],
                        "start": start,
                        "page": page,
                    }
                )

        print(f"Found {len(occurrences)} occurrences for {recording['bvid']}")

    with db.connection(os.getenv("DATABASE_URL")) as conn:
        db.occurrence.create_occurrences(conn, occurrences)
        for recording in recordings:
            db.recording.mark_recording_scanned(conn, recording["id"])


@app.function(
    timeout=30 * 60,  # 30 minutes
    secrets=[secret],
    volumes={
        BUCKET_DIR: bucket_volume,
    },
)
def populate_occurrences():
    import firefly_vcut.db as db
    from firefly_vcut.fuzz import search_text_in_transcript

    with db.connection(os.getenv("DATABASE_URL")) as conn:
        recordings = db.recording.list_recordings_to_populate_occurrences(conn)
        if len(recordings) == 0:
            print("No recordings to populate occurrences")
            return

        songs = db.song.list_all_songs_with_vtuber_song_ids(conn)

    for recording in recordings:
        print(f"Populating occurrences for {recording['bvid']}")

        occurrences = []
        transcript_object_key = recording["transcriptObjectKey"]
        transcript_path = os.path.join(BUCKET_DIR, transcript_object_key)
        with open(transcript_path, "r") as f:
            transcript = json.load(f)

        for song in songs:
            entry = search_text_in_transcript(transcript, song["lyrics_fragment"])
            if entry is None:
                continue

            start, page, score, text = entry
            if score < 40:
                continue
            print(
                f"Found {song['title']} in {recording['bvid']} at page {page} at {start} with score {score}"
            )
            print(f"Matched text: {text}")

            for i, vtuber_song_id in enumerate(song["vtuber_song_ids"]):
                vtuber_profile_id = song["vtuber_profile_ids"][i]
                if vtuber_profile_id != recording["vtuber_profile_id"]:
                    continue

                occurrences.append(
                    {
                        "song_id": song["id"],
                        "vtuber_song_id": vtuber_song_id,
                        "archive_id": recording["id"],
                        "start": start,
                        "page": page,
                    }
                )

        print(f"Found {len(occurrences)} occurrences for {recording['bvid']}")
        with db.connection(os.getenv("DATABASE_URL")) as conn:
            db.occurrence.create_occurrences(conn, occurrences)
            db.recording.mark_recording_scanned(conn, recording["id"])
