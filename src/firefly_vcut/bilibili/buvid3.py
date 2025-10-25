import requests

def get_buvid3(sessdata: str) -> str:
    base_url = "https://api.bilibili.com/x/frontend/finger/spi"

    resp = requests.get(
        base_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Cookie": f"SESSDATA={sessdata}",
        },
    )

    if resp.status_code != 200:
        raise Exception(f"Failed to get buvid3: {resp.status_code}, Body: {resp.text}")

    data = resp.json()
    if data["code"] != 0:
        raise Exception(f"Failed to get buvid3: {data['code']}, {data['message']}")

    return data["data"]["b_3"]

