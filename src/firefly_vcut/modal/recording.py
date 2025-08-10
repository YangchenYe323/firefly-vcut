import os

from .app import app, secret

@app.function(
    timeout=10 * 60, # 10 minutes
    secrets=[secret],
)
def discover_new_recordings() -> int:
    import firefly_vcut.db as db
    import firefly_vcut.bilibili as bilibili

    with db.connection(os.getenv("DATABASE_URL")) as conn:
        entries = db.recording.list_latest_and_oldest_recordings(conn)
    
    # For each vtuber, fetch recordings from bilibili which are either
    # - older than the oldest recording
    # - newer than the latest recording
    # subject to the limit of number of recordings for each vtuber

    new_recordings = 0
    for entry in entries:
        mid = entry["mid"]
        last_pubdate = entry["latest_pubdate"]
        first_pubdate = entry["oldest_pubdate"]

        print(f"Vtuber {mid} has latest recording at {last_pubdate} and oldest recording at {first_pubdate}")

        series = bilibili.series.get_live_recording_series(mid)
        if series is None:
            print(f"Vtuber {mid} has no series named '直播回放'")
            continue
        
        archives = bilibili.series.get_archives_from_series(
            mid,
            series["series_id"],
            pubdate_after=last_pubdate,
            pubdate_before=first_pubdate,
            limit=10,
        )

        if len(archives) == 0:
            print(f"Vtuber {mid} has no new recordings")
            continue

        # Hack: bilibili returns archive with a "pic" field, we need it to be cover
        for archive in archives:
            print(f"Inserting new archive {archive['bvid']} {archive['title']}, pubdate {archive['pubdate']}")
            archive["cover"] = archive.pop("pic")
        
        with db.connection(os.getenv("DATABASE_URL")) as conn:
            db.recording.create_recordings(conn, archives, mid)
        
        print(f"Created {len(archives)} new recordings for vtuber {mid}")
        new_recordings += len(archives)
    
    return new_recordings




        

        


