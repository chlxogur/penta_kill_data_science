import pandas as pd
import requests
import json
import time
from tqdm import tqdm

def getGameIds():
    headers = {
        "x-api-key" : "0TvQnueqKa5mxJntVWt0w4LpLfEkrV1Ta8rQBb9Z"
    }
    resultlist = []
    tournaments = pd.read_excel("../data/target_tournament.xlsx")["id"]
    for tournament in tqdm(tournaments):
        apiResult = requests.get(f"https://prod-relapi.ewp.gg/persisted/gw/getCompletedEvents?hl=en-US&tournamentId={tournament}", headers=headers)
        if apiResult.status_code == 200:
            json = apiResult.json()
            if 'data' in json and len(json['data']) > 0:
                events = json["data"]['schedule']['events']
                for event in events:
                    match = event["match"]
                    games = event["games"]
                    blueteam = match["teams"][0]
                    redteam = match["teams"][1]
                    for idx, game in enumerate(games):
                        if "vods" in game and len(game["vods"]) > 0:
                            game_data = {
                                "startTime(match)" : event["startTime"][:19]+"Z",
                                "blockName" : event["blockName"],
                                "leagueName": event["league"]["name"],
                                "matchId": match["id"],
                                "bestof": match["strategy"]["count"],
                                "blueteam_name" : blueteam["name"],
                                "blueteam_code" : blueteam["code"],
                                "blueteam_win" : blueteam["result"]["gameWins"],
                                "redteam_name" : redteam["name"],
                                "redteam_code" : redteam["code"],
                                "redteam_win" : redteam["result"]["gameWins"],
                                "gameNumberInAMatch": idx,
                                "gameId": game["id"]
                            }
                            resultlist.append(game_data)
        time.sleep(1)
    game_ids_df = pd.DataFrame(resultlist)
    return game_ids_df

######## 아래부턴 실행되는 부분 #######

game_ids = getGameIds() # 게임아이디가 들어간 리스트
game_ids.to_excel("../data/game_ids.xlsx", index=None)