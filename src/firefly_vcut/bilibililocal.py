import logging
import requests
from .retry import retry_with_backoff, BILIBILI_RETRY_CONFIG

from .types import Archive, Series

logger = logging.getLogger(__name__)


def get_live_recording_series(mid: int) -> Series | None:
    """
    Get the live recording series of a given user.

    Args:
        mid: User mid

    Returns:
        Series: The live recording series of the user
    """
    URL = "https://api.bilibili.com/x/polymer/web-space/home/seasons_series"

    def _make_request():
        response = requests.get(
            URL,
            params={"mid": mid, "page_num": 1000, "page_size": 10},
            headers={
                "User-Agent": "Mozilla/5.0 BiliDroid/6.73.1 (bbcallen@gmail.com) os/android model/Mi 10 Pro mobi_app/android build/6731100 channel/xiaomi innerVer/6731110 osVer/12 network/2"
            },
        )
        return response

    response = retry_with_backoff(_make_request, BILIBILI_RETRY_CONFIG)
    response.raise_for_status()

    series_list = response.json()["data"]["items_lists"]["series_list"]

    for series in series_list:
        meta = series["meta"]
        if meta["name"] == "直播回放":
            return Series(
                series_id=meta["series_id"],
                name=meta["name"],
            )
    return None


def get_archives_from_series(
    mid: int, series_id: int, page_size: int = 5000
) -> list[Archive]:
    """
    Get all the recordings from a given series.

    Args:
        mid: User mid
        series_id: Series id
        page_size: Number of recordings to get per page

    Returns:
        list[Archive]: The recordings from the series
    """
    URL = "https://api.bilibili.com/x/series/archives"

    recordings = []
    pn = 1

    while True:
        params = {
            "mid": mid,
            "series_id": series_id,
            "pn": pn,
            "ps": page_size,
        }
        
        def _make_request():
            response = requests.get(
                URL,
                params=params,
                headers={
                    "User-Agent": "Mozilla/5.0 BiliDroid/6.73.1 (bbcallen@gmail.com) os/android model/Mi 10 Pro mobi_app/android build/6731100 channel/xiaomi innerVer/6731110 osVer/12 network/2",
                },
            )
            return response
            
        response = retry_with_backoff(_make_request, BILIBILI_RETRY_CONFIG)
        response.raise_for_status()
        response = response.json()

        if response["code"] != 0:
            logger.error(f"获取回放视频失败: {response}")
            break

        recordings.extend([
            Archive(
                id=None,
                bvid=archive["bvid"],
                title=archive["title"],
                pubdate=archive["pubdate"],
                cover=archive["pic"],
                duration=archive["duration"],
            )
            for archive in response["data"]["archives"]
        ])

        if len(response["data"]["archives"]) < page_size:
            break
        pn += 1

    return recordings