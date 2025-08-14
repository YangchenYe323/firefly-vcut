import requests
from . import wbi
from ..retry import retry_with_backoff, BILIBILI_RETRY_CONFIG

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

def get_video_stream_url(bvid: str, cid: int, fnval: int, sessdata: str, wbi_key: tuple[str, str]) -> dict:
    """
    Get the stream url info for a video.

    Args:
        bvid: The Bilibili video ID.
        cid: The cid of the video.
        fnval: The fnval of the video. 
        sessdata: The sessdata of the user.
        wbi_key: The wbi key of the user.
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
        raise Exception(f"Failed to get video stream url: {resp.status_code}, Body: {body}")
    
    return resp.json()