import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
from tqdm import tqdm
from calculateColumnsForModel import calculateColumnsForModel, getAvgOfCollectedData, numberToRoleName

PARTICIPANT_NUMBER_OF_A_TEAM = 5
participant_ids_by_role = []
team_Ids_list = []
DETAIL_PATH = "../data/collected_data/"

strcolumn_dict = {"tournamentId":"str", "matchId":"str", "gameId":"str", "patch":"str", "esportsTeamId_Blue":"str", "esportsTeamId_Red":"str"}
temp1 = [f"esportsPlayerId_{i}" for i in range(10)]
temp2 = ["str" for i in range(10)]
esportsPlayerId_type_dict = dict(zip(temp1, temp2))
strcolumn_dict.update(esportsPlayerId_type_dict)
game_ids_df = pd.read_excel("../data/game_ids.xlsx", dtype=strcolumn_dict, index_col = 0)

#이거 넣어야 됨. id에 널값이 있는 게임은 누군지도 모르니까 걍 제외.
included_all_ids_df = game_ids_df[game_ids_df["esportsPlayerId_0"].notna() & game_ids_df["esportsPlayerId_1"].notna() & game_ids_df["esportsPlayerId_2"].notna() &\
                                  game_ids_df["esportsPlayerId_3"].notna() & game_ids_df["esportsPlayerId_4"].notna() & game_ids_df["esportsPlayerId_5"].notna() &\
                                    game_ids_df["esportsPlayerId_6"].notna() & game_ids_df["esportsPlayerId_7"].notna() & game_ids_df["esportsPlayerId_8"].notna() &\
                                        game_ids_df["esportsPlayerId_9"].notna()]

included_all_ids_df_sorted = included_all_ids_df.sort_values(by=["startTime(match)", "gameNumberInAMatch"], ascending=[False, False]).reset_index() # 최근 경기가 위로 오도록 내림차순 정렬

average_playerstat = getAvgOfCollectedData()    # 한번돌리는시간이 너무 오래결려서 테스트용 임시코드
last_game_time = included_all_ids_df_sorted.iloc[0, :]["startTime(match)"]
strcolumn_dict = {"gameId":"str", "esportsTeamId_Blue":"str", "esportsTeamId_Red":"str"}
temp1 = [f"esportsPlayerId_{i}" for i in range(10)]
temp2 = ["str" for i in range(10)]
esportsPlayerId_type_dict = dict(zip(temp1, temp2))
strcolumn_dict.update(esportsPlayerId_type_dict)
last_row_of_collected_datas_df = pd.read_excel("../data/last_row_of_collected_datas.xlsx", dtype = strcolumn_dict)
RANGE_OF_RECENT_GAME = 5

teamId_blue_ser = game_ids_df[pd.isna(game_ids_df["esportsTeamId_Blue"]) == False]["esportsTeamId_Blue"]
teamId_red_ser = game_ids_df[pd.isna(game_ids_df["esportsTeamId_Red"]) == False]["esportsTeamId_Red"]
team_Ids_list.append(list(set(pd.concat([teamId_blue_ser, teamId_red_ser], ignore_index=True))))

for i in range(PARTICIPANT_NUMBER_OF_A_TEAM):       # 탑 정글 미드 원딜 서폿 이렇게.. 포지션별
    playerId_blue_ser = game_ids_df[pd.isna(game_ids_df[f"esportsPlayerId_{i}"]) == False][f"esportsPlayerId_{i}"]  # 블루팀 플레이어id 시리즈
    playerId_red_ser = game_ids_df[pd.isna(game_ids_df[f"esportsPlayerId_{i + PARTICIPANT_NUMBER_OF_A_TEAM}"]) == False][f"esportsPlayerId_{i + PARTICIPANT_NUMBER_OF_A_TEAM}"]    # 레드팀 플레이어id 시리즈
    participant_ids_by_role.append(list(set(pd.concat([playerId_blue_ser,playerId_red_ser], ignore_index=True))))
    
past_games_df = included_all_ids_df_sorted.iloc[1:,:]
team_winrate_dict = {}
headtohead_winrate_dict = {}
for team_id in team_Ids_list:
    team_wincount = 0
    target_game = past_games_df[past_games_df["esportsTeamId_Blue"] == team_id | past_games_df["esportsTeamId_Red"] == team_id]
    for idx, row in target_game.iterrows():
        if last_game_time - row["startTime(match)"] > timedelta(days=365):      # 최근 1년 경기가 아니면
            break
        else:
            if ((row["esportsTeamId_Blue"] == team_id) & (row["winner_side"] == "Blue")) | ((row["esportsTeamId_Red"] == team_id) & (row["winner_side"] == "Red")):
                team_wincount += 1
    team_wincount += 1
    team_winrate = team_wincount / (target_game.shape[0] + 2)
    team_winrate_dict.update({team_id : team_winrate})
    for opposite_team_id in team_Ids_list:
        particular_winrate_dict = {}
        if opposite_team_id == team_id:
            continue
        else:
            target_games = past_games_df[((past_games_df["esportsTeamId_Blue"] == team_id) & (past_games_df["esportsTeamId_Red"] == opposite_team_id)) |
                                        ((past_games_df["esportsTeamId_Red"] == team_id) & (past_games_df["esportsTeamId_Blue"] == opposite_team_id))
                                        ]
            particular_wincount = 0
            for idx, row in target_games.iterrow():
                if last_game_time - row["startTime(match)"] > timedelta(days=365):
                    break
                else:
                    if ((row["esportsTeamId_Blue"] == team_id) & (row["esportsTeamId_Red"] == opposite_team_id)) & (row["winner_side"] == "Blue"):
                        particular_wincount += 1
                    elif ((row["esportsTeamId_Red"] == team_id) & (row["esportsTeamId_Blue"] == opposite_team_id)) & (row["winner_side"] == "Red"):
                        particular_wincount += 1
                        