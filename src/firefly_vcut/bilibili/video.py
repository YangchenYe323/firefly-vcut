import requests
import time

from . import wbi
from ..retry import retry_with_backoff
from ..config import BILIBILI_RETRY_CONFIG


def list_user_videos(
    mid: int,
    sessdata: str,
    wbi_key: tuple[str, str],
    pubdate_after: int,
) -> list[dict]:
    """
    List all the uploaded videos of a given user published after a given pubdate.

    Args:
        mid: The Bilibili user ID.
        sessdata: The sessdata of the login user.
        wbi_key: The wbi key of the login user.
        buvid3: The buvid3 of the login user.
        pubdate_after: The publication date after which to list videos.

    Returns:
        A list of videos.
        Each video is a dictionary with the following keys:
        - bvid: The Bilibili video ID.
        - title: The title of the video.
        - created: The publication date of the video, unix epoch timestamp.
    """

    # See comments in https://github.com/SocialSisterYi/bilibili-API-collect/blob/e5fbfed42807605115c6a9b96447f6328ca263c5/docs/user/space.md
    # Don't bother with https://api.bilibili.com/x/space/wbi/arc/search
    base_url = "https://api.bilibili.com/x/series/recArchivesByKeywords"

    # List of videos published after the given pubdate
    videos = []

    pn = 1
    while True:
        print(f"Listing user {mid} videos, page {pn}")

        params = {
            "mid": mid,
            "keywords": "",
            "orderby": "pubdate",
            "type": 0,  # 不筛选分区
            "ps": 30,
            "pn": pn,
        }

        img_key, sub_key = wbi_key
        encoded_params = wbi.encWbi(params=params, img_key=img_key, sub_key=sub_key)

        def _make_request():
            resp = requests.get(
                base_url,
                params=encoded_params,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                    "Cookie": f"SESSDATA={sessdata}",
                },
            )
            return resp

        resp = retry_with_backoff(_make_request, BILIBILI_RETRY_CONFIG)

        if resp.status_code != 200:
            body = resp.text
            raise Exception(
                f"Failed to list user videos: {resp.status_code}, Body: {body}"
            )

        data = resp.json()
        if data["code"] != 0:
            raise Exception(
                f"Failed to list user videos: {data['code']}, {data['message']}"
            )

        stop = False
        # It's a double for loop because each upload can contain multiple
        # videos.
        for video in data["data"]["archives"]:
            bvid = video["bvid"]
            title = video["title"]
            pubdate = video["pubdate"]
            if pubdate <= pubdate_after:
                stop = True
                break
            videos.append(
                {
                    "bvid": bvid,
                    "title": title,
                    "created": pubdate,
                }
            )

        if stop:
            break

        # Sleep for 1.5 seconds to avoid rate limiting
        time.sleep(1.5)
        pn += 1

    return videos


def get_video_info(bvid: str, sessdata: str) -> dict:
    """
    Get video info from Bilibili.

    Args:
        bvid: The Bilibili video ID.
    """

    def _make_request():
        resp = requests.get(
            "https://api.bilibili.com/x/web-interface/view",
            params={
                "bvid": bvid,
            },
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "Cookie": f"SESSDATA={sessdata}",
            },
        )
        return resp

    resp = retry_with_backoff(_make_request, BILIBILI_RETRY_CONFIG)

    if resp.status_code != 200:
        body = resp.text
        raise Exception(f"Failed to get video info: {resp.status_code}, Body: {body}")

    return resp.json()


def get_video_stream_url(
    bvid: str, cid: int, fnval: int, sessdata: str, wbi_key: tuple[str, str]
) -> dict:
    """
    Get the stream url info for a video.

    Args:
        bvid: The Bilibili video ID.
        cid: The cid of the video.
        fnval: The fnval of the video.
        sessdata: The sessdata of the login user.
        wbi_key: The wbi key of the login user.
    """

    base_url = "https://api.bilibili.com/x/player/wbi/playurl"

    params = {
        "bvid": bvid,
        "cid": str(cid),
        "fnval": str(fnval),
    }

    img_key, sub_key = wbi_key
    encoded_params = wbi.encWbi(params=params, img_key=img_key, sub_key=sub_key)

    def _make_request():
        resp = requests.get(
            base_url,
            params=encoded_params,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "Cookie": f"SESSDATA={sessdata}",
            },
        )
        return resp

    resp = retry_with_backoff(_make_request, BILIBILI_RETRY_CONFIG)

    if resp.status_code != 200:
        body = resp.text
        raise Exception(
            f"Failed to get video stream url: {resp.status_code}, Body: {body}"
        )

    return resp.json()
