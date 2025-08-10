import requests

def get_live_recording_series(mid: int) -> dict | None:
    """
    Get the live recording series of a given user.

    Args:
        mid: The vtuber mid.

    Returns:
        A dictionary containing the series information, or None if no series exists.
        The dictionary has the following keys:
        - series_id: The series ID.
        - name: The name of the series.
    """
    URL = "https://api.bilibili.com/x/polymer/web-space/home/seasons_series"

    # You're not going to have 10000 series, are you?
    response = requests.get(
        URL,
        params={"mid": mid, "page_num": 1000, "page_size": 10},
        headers={
            "User-Agent": "Mozilla/5.0 BiliDroid/6.73.1 (bbcallen@gmail.com) os/android model/Mi 10 Pro mobi_app/android build/6731100 channel/xiaomi innerVer/6731110 osVer/12 network/2",
        },
    )

    response.raise_for_status()

    series_list = response.json()["data"]["items_lists"]["series_list"]

    for series in series_list:
        meta = series["meta"]
        if meta["name"] == "直播回放":
            return {
                "series_id": meta["series_id"],
                "name": meta["name"],
            }
    # vtuber has no series named "直播回放"
    return None

def get_archives_from_series(
    mid: int,
    series_id: int,
    pubdate_after: int | None = None,
    pubdate_before: int | None = None,
    limit: int = 5,
) -> list[dict]:
    """
    Get the archives from a given series subject to the below constraints:
        pubdate > pubdate_after or pubdate < pubdate_before

    Args:
        mid: The vtuber mid.
        series_id: The series ID.
        last_pubdate: The last publication date of the existing archives
        first_pubdate: The first publication date of the existing archives
        limit: The limit of the number of archives to get.
    """

    URL = "https://api.bilibili.com/x/series/archives"

    archives = []

    # fetch 5000 in a row, so probably we get all of them in one request.
    # because of the way the filtering work, it's highly likely that we will need to look
    # at the entire archive series to find the ones we wanted to add.
    pn = 1
    page_size = 5000

    while len(archives) < limit:
        params = {
            "mid": mid,
            "series_id": series_id,
            "pn": pn,
            "ps": page_size,
        }

        response = requests.get(
            URL,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0 BiliDroid/6.73.1 (bbcallen@gmail.com) os/android model/Mi 10 Pro mobi_app/android build/6731100 channel/xiaomi innerVer/6731110 osVer/12 network/2",
            },
        )
        response.raise_for_status()
        response = response.json()

        if response["code"] != 0:
            raise ValueError(f"Failed to get archives from series: {response}")
        
        for archive in response["data"]["archives"]:
            if pubdate_after is not None and archive["pubdate"] <= pubdate_after:
                continue
            if pubdate_before is not None and archive["pubdate"] >= pubdate_before:
                continue
            archives.append(archive)
            if len(archives) >= limit:
                break
        
        # This is the last page
        if len(response["data"]["archives"]) < page_size:
            break
        
        pn += 1

    return archives
