import pandas as pd
import numpy as np
import json

def map_team_name_to_codename(team_name):
    return team_code_map.get(team_name, np.nan)

df = pd.read_excel("../data/crawling_result.xlsx")
with open('../data/team_code.json', 'r') as f:
    team_code_map = json.load(f)

df['blueteam_code'] = df['blueteam'].apply(map_team_name_to_codename)
df['redteam_code'] = df['redteam'].apply(map_team_name_to_codename)

df.to_excel("../data/crawling_result_with_codenames_nan.xlsx", index=False)