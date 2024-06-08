 # 처음 다 0으로 나오는줄들 지우고 끝에 10초단위로 안끊기는 한 줄 지우고 하는 코드입니다.
import os
import pandas as pd
import datetime
from tqdm import tqdm
import ast

DETAIL_PATH = "../data/collected_data/"
file_list = os.listdir(DETAIL_PATH)

for file_name in tqdm(file_list):
    if file_name.find("_invalid") == -1 and file_name.find(".xlsx") != -1:
        df = pd.read_excel(DETAIL_PATH + file_name)
        df = df.astype({"esportsTeamId_Blue":"str", "esportsTeamId_Red":"str", "esportsPlayerId_0":"str", "esportsPlayerId_1":"str", "esportsPlayerId_2":"str", "esportsPlayerId_3":"str", "esportsPlayerId_4":"str", "esportsPlayerId_5":"str", "esportsPlayerId_6":"str", "esportsPlayerId_7":"str", "esportsPlayerId_8":"str", "esportsPlayerId_9":"str"})
        df["timestamp"] = df["timestamp"].apply(lambda x : datetime.datetime.strptime(x[:19], "%Y-%m-%dT%H:%M:%S"))
        blue_dragons_count = df["blue_dragons"].apply(lambda x : len(ast.literal_eval(x)))
        df = pd.concat([df, blue_dragons_count.rename("blue_dragons_count")], axis=1)
        red_dragons_count = df["red_dragons"].apply(lambda x : len(ast.literal_eval(x)))
        df = pd.concat([df, red_dragons_count.rename("red_dragons_count")], axis=1)
        df = df.copy()
        non_zero_df = df[df["totalGoldEarned_0"] != 0]
        drop_weird_second_df = non_zero_df[non_zero_df["timestamp"].dt.second % 10 == 0].reset_index(drop = True)
        drop_weird_second_df["duration"] = drop_weird_second_df.index * 10
        if drop_weird_second_df.iloc[-1]["duration"] > 600: # 10분이내 끝난 경기는 제외함.
            new_file_name = file_name
        else:
            base_name = os.path.splitext(file_name)[0]  
            new_file_name = f"{base_name}_tooshort.xlsx"
        full_path = os.path.join(DETAIL_PATH, new_file_name)
        drop_weird_second_df.to_excel(full_path)