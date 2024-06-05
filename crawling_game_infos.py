import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import pandas as pd
import numpy as np
import json

NUMBER_OF_PLAYERS_OF_A_TEAM = 5

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
    base = url[:url.rfind("/")]
    tournament_name = base[base.rfind("/")+1:]
    print(f"Failed to fetch data from tournament : {tournament_name} after {RETRY_COUNT} attempts.")
    raise Exception(f"Failed to fetch data from tournament : {tournament_name} after {RETRY_COUNT} attempts")

def scrapInAPage(tournament_link):
    result = []
    if tournament_link is not np.nan:
        match_history = tournament_link + "/Match_History"

        response = requestWithHandlingHttperr(match_history)
        soup = BeautifulSoup(response.text, "html.parser")

        tournament_heading = soup.find("h1", attrs={"id":"firstHeading"}).text
        hyphen_where = tournament_heading.find("-")
        tournament_title = tournament_heading[:hyphen_where-1]
        for match in soup.find_all("tr", class_=["mhgame-red multirow-highlighter", "mhgame-blue multirow-highlighter"]):
            data = match.find_all("td")
            if data[1].find("a") is not None:
                href = data[1].find("a").attrs["href"]
                patch = href[href.rfind("_")+1:]
            else:
                patch = "Unknown"
            blueteam = data[2].find("img").attrs["alt"][:data[2].find("img").attrs["alt"].find("logo std")]
            redteam = data[3].find("img").attrs["alt"][:data[3].find("img").attrs["alt"].find("logo std")]
            winner = data[4].find("img").attrs["alt"][:data[4].find("img").attrs["alt"].find("logo std")]
            if winner == blueteam: winner_side = "Blue"
            elif winner == redteam: winner_side = "Red"
            row = {
                "date" : data[0].text,
                "tournament_title" : tournament_title,
                "patch" : patch,
                "blueteam" : blueteam,
                "redteam" : redteam,
                "winner_side" : winner_side
            }
            if blueteam == "Beşiktaş Esports": row["blueteam"] = "Besiktas Esports"                     # 터키팀 특수문자를 알파벳으로 변경해보자.
            if blueteam == "Beşiktaş.Oyun Hizmetleri": row["blueteam"] = "Besiktas.Oyun Hizmetleri"
            if blueteam == "Fenerbahçe Esports": row["blueteam"] = "Fenerbahce Esports"
            if redteam == "Beşiktaş Esports": row["redteam"] = "Besiktas Esports"
            if redteam == "Beşiktaş.Oyun Hizmetleri": row["redteam"] = "Besiktas.Oyun Hizmetleri"
            if redteam == "Fenerbahçe Esports": row["redteam"] = "Fenerbahce Esports"

            for idx, ban in enumerate(data[5].find_all("span")):
                row[f"ban_{idx}"] = ban.attrs["title"]
            for idx, ban in enumerate(data[6].find_all("span")):
                row[f"ban_{idx + NUMBER_OF_PLAYERS_OF_A_TEAM}"] = ban.attrs["title"]
            for idx, pick in enumerate(data[7].find_all("span")):
                row[f"pick_{idx}"] = pick.attrs["title"]
            for idx, pick in enumerate(data[8].find_all("span")):
                row[f"pick_{idx + NUMBER_OF_PLAYERS_OF_A_TEAM}"] = pick.attrs["title"]
            result.append(row)
    return pd.DataFrame(result)

###### 실행되는 부분 ######
result = pd.DataFrame()
df = pd.read_excel("../data/pentakill 경기 상세데이터 수집기록.xlsx", sheet_name="경기 세부 링크")

for idx, row in tqdm(df.iterrows(), total = df.shape[0]):
    links = []
    links.append(row["링크"])
    if row["플옵링크_1"] is not np.nan: links.append(row["플옵링크_1"])
    if row["플옵링크_2"] is not np.nan: links.append(row["플옵링크_2"])
    info_fromapi = {
        "tournament_name" : row["이름"],
        "tournament_id" : str(row["id"]),
        "tournament_slug" : row["slug"]
    }
    for link in links:
        info_fromapi_list = []
        scrapped_df = scrapInAPage(link)
        for j in range(scrapped_df.shape[0]):                  # concat을 위해 api에서 나온 토너먼트 정보를 아래로 복제해줌
            info_fromapi_list.append(info_fromapi)
        info_fromapi_df = pd.DataFrame(info_fromapi_list)
        scrapped_df = pd.concat([scrapped_df, info_fromapi_df], axis = 1)
        result = pd.concat([result, scrapped_df], axis = 0, ignore_index=True)

with open('../data/team_code.json', 'r') as f:
    team_code_map = json.load(f)
result['blueteam_code'] = result['blueteam'].apply(lambda x : team_code_map.get(x, np.nan))
result['redteam_code'] = result['redteam'].apply(lambda x : team_code_map.get(x, np.nan))
result.to_excel("../data/crawling_result_with_codenames.xlsx", index=False)