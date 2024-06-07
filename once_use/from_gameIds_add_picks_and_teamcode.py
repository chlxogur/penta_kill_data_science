import pandas as pd
from tqdm import tqdm, tqdm_pandas
import numpy as np
import os

game_ids = pd.read_excel("../data/game_ids.xlsx")
game_ids = game_ids.astype({"tournamentId":"str","matchId":"str", "gameId":"str"})
PARTICIPANT_NUMBER_OF_A_TEAM = 5

def addPicksAndTeamCode(row):
    #print(type(row))
    gameId = row["gameId"]
    
    DETAIL_PATH = "../data/collected_data/"
    file_list = os.listdir(DETAIL_PATH)
    if gameId + ".xlsx" in file_list:
        detail_data = pd.read_excel(DETAIL_PATH + gameId + ".xlsx")
        for i in range(PARTICIPANT_NUMBER_OF_A_TEAM * 2):
            row[f"pick_{i}"] = detail_data.at[0, f"championName_{i}"]
        for j in range(PARTICIPANT_NUMBER_OF_A_TEAM * 2):
            row[f"summonerName_{j}"] = detail_data.at[0, f"summonerName_{j}"]
        row["teamcode_blue_on_detail"] = detail_data.at[0, "teamCode_0"]
        row["teamcode_red_on_detail"] = detail_data.at[0, "teamCode_5"]
    else:
        for i in range(PARTICIPANT_NUMBER_OF_A_TEAM * 2):
            row[f"pick_{i}"] = np.nan
        for j in range(PARTICIPANT_NUMBER_OF_A_TEAM * 2):
            row[f"summonerName_{j}"] = np.nan
        row["teamcode_blue_on_detail"] = np.nan
        row["teamcode_red_on_detail"] = np.nan
    if len(row) != 37:
        print(f"gameId: {gameId}, row length: {len(row)}")
    return row

for i in range(PARTICIPANT_NUMBER_OF_A_TEAM * 2):
    game_ids[f"pick_{i}"] = np.nan
for j in range(PARTICIPANT_NUMBER_OF_A_TEAM * 2):
    game_ids[f"summonerName_{j}"] = np.nan
game_ids["teamcode_blue_on_detail"] = np.nan
game_ids["teamcode_red_on_detail"] = np.nan

tqdm.pandas()
game_ids = game_ids.progress_apply(lambda row : addPicksAndTeamCode(row), axis = 1)
game_ids.to_excel("../data/game_ids.xlsx", index=None)