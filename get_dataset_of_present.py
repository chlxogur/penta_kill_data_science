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
RANGE_OF_RECENT_GAME = 10

teamId_blue_ser = game_ids_df[pd.isna(game_ids_df["esportsTeamId_Blue"]) == False]["esportsTeamId_Blue"]
teamId_red_ser = game_ids_df[pd.isna(game_ids_df["esportsTeamId_Red"]) == False]["esportsTeamId_Red"]
team_Ids_list = list(set(pd.concat([teamId_blue_ser, teamId_red_ser], ignore_index=True)))

for i in range(PARTICIPANT_NUMBER_OF_A_TEAM):       # 탑 정글 미드 원딜 서폿 이렇게.. 포지션별
    playerId_blue_ser = game_ids_df[pd.isna(game_ids_df[f"esportsPlayerId_{i}"]) == False][f"esportsPlayerId_{i}"]  # 블루팀 플레이어id 시리즈
    playerId_red_ser = game_ids_df[pd.isna(game_ids_df[f"esportsPlayerId_{i + PARTICIPANT_NUMBER_OF_A_TEAM}"]) == False][f"esportsPlayerId_{i + PARTICIPANT_NUMBER_OF_A_TEAM}"]    # 레드팀 플레이어id 시리즈
    participant_ids_by_role.append(list(set(pd.concat([playerId_blue_ser,playerId_red_ser], ignore_index=True))))
    
past_games_df = included_all_ids_df_sorted.iloc[1:,:]
team_winrate_dict = {}
player_form_dict = {}
for team_id in tqdm(team_Ids_list):
    team_wincount = 1
    subrow_count = 0
    target_game = past_games_df[(past_games_df["esportsTeamId_Blue"] == team_id) | (past_games_df["esportsTeamId_Red"] == team_id)]
    for idx, row in target_game.iterrows():
        if last_game_time - row["startTime(match)"] > timedelta(days=365):      # 최근 1년 경기가 아니면
            break
        else:
            subrow_count += 1
            if ((row["esportsTeamId_Blue"] == team_id) & (row["winner_side"] == "Blue")) | ((row["esportsTeamId_Red"] == team_id) & (row["winner_side"] == "Red")):
                team_wincount += 1
    team_winrate = team_wincount / (subrow_count + 2)
    team_winrate_dict.update({team_id: {"self": team_winrate}})
    for opposite_team_id in team_Ids_list:
        particular_winrate_dict = {}
        if opposite_team_id == team_id:
            continue
        else:
            target_games = past_games_df[((past_games_df["esportsTeamId_Blue"] == team_id) & (past_games_df["esportsTeamId_Red"] == opposite_team_id)) |
                                        ((past_games_df["esportsTeamId_Red"] == team_id) & (past_games_df["esportsTeamId_Blue"] == opposite_team_id))
                                        ]
            particular_wincount = 1
            particular_subrow_count = 0
            for idx, row in target_games.iterrows():
                if last_game_time - row["startTime(match)"] > timedelta(days=365):
                    break
                else:
                    particular_subrow_count += 1
                    if ((row["esportsTeamId_Blue"] == team_id) & (row["esportsTeamId_Red"] == opposite_team_id)) & (row["winner_side"] == "Blue"):
                        particular_wincount += 1
                    elif ((row["esportsTeamId_Red"] == team_id) & (row["esportsTeamId_Blue"] == opposite_team_id)) & (row["winner_side"] == "Red"):
                        particular_wincount += 1
            particular_winrate = particular_wincount / (particular_subrow_count + 2)
            team_winrate_dict.update({team_id: {opposite_team_id : particular_winrate}})
        
##### 아래는 선수별 최근 n경기 스탯을 통해 폼을 뽑아내는 코드 #####
desired_labels = [f'esportsPlayerId_{j}' for j in range(PARTICIPANT_NUMBER_OF_A_TEAM * 2)]
for role in tqdm(range(PARTICIPANT_NUMBER_OF_A_TEAM)):
    for player_id in tqdm(participant_ids_by_role[role], leave=False):
        playerstat_list = []    # 선수별 최근 n경기의 기록을 저장할 곳
        subdata_playerstat = past_games_df[(past_games_df[f"esportsPlayerId_{role}"] == player_id) |
                                           (past_games_df[f"esportsPlayerId_{role + PARTICIPANT_NUMBER_OF_A_TEAM}"] == player_id)].head(RANGE_OF_RECENT_GAME)
        number_of_past_games_of_player = subdata_playerstat.shape[0]
        for sub_idx, sub_row in subdata_playerstat.iterrows():
            #딕셔너리로 만들어서 playerstat_list에 append하자. 그 다음에 형태를 보자.
            playerstat_dict_of_the_game = {}    # 한 경기에 대한 플레이어 활약상을 넣을 딕셔너리
            game_ids_of_sub_row = sub_row[desired_labels]
            matched_column = game_ids_of_sub_row[game_ids_of_sub_row.values == player_id].index[0]      # ID가 서로 맞는 컬럼이 어디인지(블루팀인지레드팀인지 모르니까)
            matched_column_number = matched_column[matched_column.find("_")+1:]     # 번호 추출
            target_game = calculateColumnsForModel(last_row_of_collected_datas_df[last_row_of_collected_datas_df["gameId"] == sub_row["gameId"]].T.squeeze())
            #print(target_game["gameId"])
            playerstat_dict_of_the_game["kda"] = target_game[f"kda_{matched_column_number}"]
            playerstat_dict_of_the_game["championDamageShare"] = target_game[f"championDamageShare_{matched_column_number}"]
            playerstat_dict_of_the_game["creepScorePerTime"] = target_game[f"creepScorePerTime_{matched_column_number}"]
            playerstat_dict_of_the_game["wardsScorePerTime"] = target_game[f"wardsScorePerTime_{matched_column_number}"]
            playerstat_dict_of_the_game["goldEarnedPerTime"] = target_game[f"goldEarnedPerTime_{matched_column_number}"]
            #print(playerstat_dict_of_the_game)
            playerstat_list.append(playerstat_dict_of_the_game)
            # 넣을거 : kda, championDamageShare, creepScore, wardsScore, goldEarned
        if number_of_past_games_of_player <= 2:
            playerstat_dict_of_the_game = {         # 빈 자리에 일반적인 평균을 채워 넣고
                "kda" : average_playerstat[f"kdaof{numberToRoleName(role)}"],
                "championDamageShare" : average_playerstat[f"championDamageShareof{numberToRoleName(role)}"],
                "creepScorePerTime" : average_playerstat[f"creepScorePerTimeof{numberToRoleName(role)}"],
                "wardsScorePerTime" : average_playerstat[f"wardsScorePerTimeof{numberToRoleName(role)}"],
                "goldEarnedPerTime" : average_playerstat[f"goldEarnedPerTimeof{numberToRoleName(role)}"]
            }
        else:
            playerstat_keys = playerstat_list[0].keys()
            playerstat_values = {key: [] for key in playerstat_keys}
            for entry in playerstat_list:
                for key in playerstat_keys:
                    playerstat_values[key].append(entry[key])
            playerstat_dict_of_the_game = {key : np.mean(playerstat_values[key]) for key in playerstat_keys}
            
        player_form = pd.DataFrame(playerstat_dict_of_the_game, index=[0]).T
        player_form.reset_index(inplace = True)
        player_form.columns = ["elements", "formvalue"]
        player_form_dict.update({numberToRoleName(role): {player_id: player_form}})
data_dict = {
    "team_winrate" : team_winrate_dict,
    "player_form" : player_form_dict
}


            