#### game_ids에 패치 정보를 붙이기 위한 코드. 한번쓰고말듯?

import pandas as pd
import numpy as np
from tqdm import tqdm
import requests
import time

# get_game_detail.py에서 2024년 5월 31일에 복사, 이거 이러지말고 모듈화해서 import 하고 싶은데 get_game_detail.py에 main()부분이 없어야 하는 건가?
def requestWithHandlingHttperr(url):
    RETRY_COUNT = 12                # 기본 반복 12회
    RETRY_DELAY_SEC = 10            # 대기 10초
    ERRNO_10054 = 10054
    ERRNO_500 = 500
    ERRNO_503 = 503
    ERRNO_504 = 504

    REQUEST_INTERVAL_SEC = 0.1

    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}  # 서버에 내 신분을 속이기 위한 유저에이전트... 자세히는 잘 모릅니다

    time.sleep(REQUEST_INTERVAL_SEC)    # 먼저 0.1초 쉬고

    for i in range(RETRY_COUNT):
        try:
            result = requests.get(url, headers = headers)       # API에 request 요청
            result.raise_for_status()                           # http에러가 나오면 예외를 발생시킴 -> except로 점프
            return result
        except requests.exceptions.ConnectionError as e:
            if isinstance(e.args[0], ConnectionResetError) and e.args[0].winerror == ERRNO_10054:
                print(f"Attempt {i + 1} failed with error 10054. Retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
            else:                           # 다른 http 에러면
                raise
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == ERRNO_500:
                print(f"Attempt {i + 1} failed with 500 Internal Server Error. Retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
            elif e.response.status_code == ERRNO_503:
                print(f"Attempt {i + 1} failed with 503 Service Unavailable. Retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
            elif e.response.status_code == ERRNO_504:
                print(f"Attempt {i + 1} failed with 504 Gateway Timeout. Retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
            else:  # 다른 HTTPError 예외 처리
                raise
    game_id = url[url.rfind("/") + 1:]
    print(f"Failed to fetch data from game ID : {game_id} after {RETRY_COUNT} attempts.")
    raise Exception(f"Failed to fetch data from game ID : {game_id} after {RETRY_COUNT} attempts")

result = []
game_ids = pd.read_excel("../data/game_ids.xlsx")
for idx, row in tqdm(game_ids.iterrows(), total = 9293):
    window_url = f"https://feed.lolesports.com/livestats/v1/window/{row["gameId"]}"
    apiResult = requestWithHandlingHttperr(window_url)
    if apiResult.status_code == 200:
        json_data = apiResult.json()
        if 'gameMetadata' in json_data and len(json_data['gameMetadata']) > 0:
            game = json_data["gameMetadata"]
            if game.get("patchVersion") is not None:
                patch_ver = game["patchVersion"]
                where = patch_ver.find(".")
                patch_ver = patch_ver[:patch_ver[where+1:].find(".")+where+1] # [where+1:]부터 "."의 위치를 찾았으니까 인덱스가 예상한것보다 한 칸 앞으로 당겨져 있을것이므로 +1을 넣어 보정.
                row["patch"] = patch_ver
            else:
                row["patch"] = np.nan
            result.append(row)

result_df = pd.DataFrame(result)
result_df = result_df.astype({"matchId":"str", "gameId":"str"})
result_df.to_excel("../data/game_ids_with_patch.xlsx")