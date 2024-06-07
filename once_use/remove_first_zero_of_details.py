import os
import pandas as pd
import datetime
from tqdm import tqdm

DETAIL_PATH = "../data/collected_data/"     # 처음 다 0으로 나오는줄들 지우고 끝에 10초단위로 안끊기는 한 줄 지우고
file_list = os.listdir(DETAIL_PATH)

for file_name in tqdm(file_list):
    if file_name.find("_invalid") == -1:
        df = pd.read_excel(DETAIL_PATH + file_name)
        df = df.astype({"esportsTeamId_Blue":"str", "esportsTeamId_Red":"str", "esportsPlayerId_0":"str", "esportsPlayerId_1":"str", "esportsPlayerId_2":"str", "esportsPlayerId_3":"str", "esportsPlayerId_4":"str", "esportsPlayerId_5":"str", "esportsPlayerId_6":"str", "esportsPlayerId_7":"str", "esportsPlayerId_8":"str", "esportsPlayerId_9":"str"})
        df["timestamp"] = df["timestamp"].apply(lambda x : datetime.datetime.strptime(x[:19], "%Y-%m-%dT%H:%M:%S"))
        non_zero_df = df[df["totalGoldEarned_0"] != 0]
        drop_tail_df = non_zero_df.iloc[:-1, :]
        start_time = drop_tail_df.iat[0,0]
        drop_tail_df = drop_tail_df.copy()
        drop_tail_df["game_duration"] = (drop_tail_df["timestamp"] - start_time).dt.total_seconds().astype(int)
        drop_tail_df.to_excel(DETAIL_PATH + file_name, index=False)