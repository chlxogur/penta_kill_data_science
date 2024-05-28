import pandas as pd


# 팀명과 소환사명이 공백 없이 붙어서 나와 문제가 생긴 데이터들이 일부 있습니다. 그걸 팀명과 소환사명으로 분리하기 위한 코드입니다.
# 각 팀 5명의 선수에 대해 공통적으로 앞에 붙은 문자열은 팀명으로 간주합니다.
# 일치하는 부분이 없으면 "", 0을 리턴합니다. 파라미터가 문자열이 아니면 "", -1을 리턴합니다.

def find_common_prefix(strings):
    if not strings:
        return "", -1

    # Find the shortest string in the list
    shortest = min(strings, key=len)
    
    # Initialize the end index of the common prefix
    end_index = len(shortest)
    
    for i in range(len(shortest)):
        # Check if this character is the same in all strings
        current_char = shortest[i]
        for string in strings:
            if string[i] != current_char:
                end_index = i
                break
        if end_index != len(shortest):
            break
    
    # The common prefix
    common_prefix = shortest[:end_index]
    return common_prefix, end_index

##### 아래는 실행되는 부분 #######

PARTICIPANTS_NUMBER_OF_A_TEAM = 5
blueSummonerNames = []
redSummonerNames = []
needed = True

df = pd.read_excel("../data/112274705999884336.xlsx")

for i in range(PARTICIPANTS_NUMBER_OF_A_TEAM):
    blueteam = df.at[0, f"teamCode_{i}"]
    name = df.at[0, f"summonerName_{i}"]
    if blueteam != name[:-1]: needed = False
    blueSummonerNames.append(name)
    redteam = df.at[0, f"teamCode_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]
    name = df.at[0, f"summonerName_{i+PARTICIPANTS_NUMBER_OF_A_TEAM}"]
    if redteam != name[:-1]: needed = False
    redSummonerNames.append(name)

if needed == True:
    blueteamName, blueteamWhere = find_common_prefix(blueSummonerNames)
    redteamName, redteamWhere = find_common_prefix(redSummonerNames)

    for i in range(PARTICIPANTS_NUMBER_OF_A_TEAM):
        print(i, blueteamName, blueteamWhere, redteamName, redteamWhere)
        if blueteamWhere > 0:
            df[f"teamCode_{i}"] = blueteamName
            df[f"summonerName_{i}"] = df[f"summonerName_{i}"].apply(lambda x : x[redteamWhere:])
        if redteamWhere > 0:
            df[f"teamCode_{i+PARTICIPANTS_NUMBER_OF_A_TEAM}"] = redteamName
            df[f"summonerName_{i+PARTICIPANTS_NUMBER_OF_A_TEAM}"] = df[f"summonerName_{i+PARTICIPANTS_NUMBER_OF_A_TEAM}"].apply(lambda x : x[redteamWhere:])