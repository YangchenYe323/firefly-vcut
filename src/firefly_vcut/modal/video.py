import os
import modal
import re

from .app import app, secret


import firefly_vcut.db as db
import firefly_vcut.bilibili as bilibili

# Regex patterns to extract song title from video title
SONG_VIDEO_PATTERNS = [
    r"《(.*)》"  # e.g., '「我来不及道声不安，有点混乱有点缓慢」痛彻心扉翻唱《离开我的依赖》'
]


@app.function(
    # Run every Monday, Thursday, and Saturday at 3:00AM Asia/Shanghai.
    schedule=modal.Cron(timezone="Asia/Shanghai", cron_string="0 3 * * 1,4,6"),
    timeout=10 * 60,  # 10 minutes
    secrets=[secret],
)
def discover_new_song_videos():
    """
    Workflow to discover new song videos uploaded by vtubers and update the vtuber songs
    in the database to link to the new videos.
    """

    with db.connection(os.getenv("DATABASE_URL")) as conn:
        # entries contains an anchor point for each vtuber,
        # which is the latest video pubdate we know about for the vtuber.
        # We will only search videos published after this anchor point.
        entries = db.song.list_latest_bvid_by_vtuber(conn)
        # Pre-populate a mapping from
        # title -> vtuber_profile_id -> vtuber_song_id
        # so we know which vtuber song to update given a new song title
        # we discovered
        vtuber_songs = db.song.list_vtuber_songs_by_vtuber_profile_id(conn)
        by_title = {}
        for s in vtuber_songs:
            if s["title"] not in by_title:
                by_title[s["title"]] = {}
            by_title[s["title"]][s["vtuber_profile_id"]] = s["vtuber_song_id"]

    sessdata = os.getenv("SESSDATA")
    wbi_key = bilibili.wbi.getWbiKeys(sessdata)

    # List of update entries where each is a dictionary with the following keys:
    # - vtuber_song_id: The vtuber song ID.
    # - bvid: The Bilibili video ID.
    # - pubdate: The publication date of the video, unix epoch timestamp.
    update_entries = []
    # Then for each vtuber, fetch new songs
    for entry in entries:
        vtuber_profile_id = entry["vtuber_profile_id"]
        mid = entry["mid"]
        latest_video_pubdate = entry["latest_video_pubdate"]

        videos = bilibili.video.list_user_videos(
            mid,
            sessdata,
            wbi_key,
            pubdate_after=latest_video_pubdate,
        )

        for video in videos:
            title = extract_title_from_video_title(video["title"])
            if title is None:
                continue

            if title not in by_title:
                print(f"Vtuber {mid} uploaded an unknown song: {title}")
                continue

            if vtuber_profile_id not in by_title[title]:
                print(
                    f"Vtuber {mid} uploaded a song that is not in their profile: {title}"
                )
                continue

            vtuber_song_id = by_title[title][vtuber_profile_id]

            print(f"Vtuber {mid} uploaded a new song video: {title} ({video['bvid']})")

            update_entries.append(
                {
                    "vtuber_song_id": vtuber_song_id,
                    "bvid": video["bvid"],
                    "pubdate": video["created"],
                }
            )

    if len(update_entries) == 0:
        print("No new song videos found")
        return

    with db.connection(os.getenv("DATABASE_URL")) as conn:
        updated = db.song.update_bvid(conn, update_entries)
        print(f"Updated {updated} new song videos")


def extract_title_from_video_title(title: str) -> str | None:
    for pattern in SONG_VIDEO_PATTERNS:
        m = re.search(pattern, title)
        if m:
            return m.group(1)
    return None
