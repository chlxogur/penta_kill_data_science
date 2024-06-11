def numberToRoleName(number):
    if number == 0 or number == 5:
        return "Top"
    elif number == 1 or number == 6:
        return "Jgl"
    elif number == 2 or number == 7:
        return "Mid"
    elif number == 3 or number == 8:
        return "Adc"
    else:
        return "Spt"

def calculateColumnsForModel(row):
    PARTICIPANTS_NUMBER_OF_A_TEAM = 5
    for i in range(PARTICIPANTS_NUMBER_OF_A_TEAM * 2):
        kills = row[f"kills_{i}"]
        deaths = row[f"deaths_{i}"]
        assists = row[f"assists_{i}"]
        if deaths == 0:                         
            kda = (kills + assists) * 1.2   # LCK에서는 노데스로 끝난 선수에게는 1데스시 kda의 1.2배를 곱한 값을 kda로 표시해주고 있다.
        else:
            kda = (kills + assists) / deaths
        row[f"kda_{i}"] = kda
        
        #row[f"championDamageShareByRoleof{numberToRoleName(i)}"] = (row[f"championDamageShare_{i}"] + row[f"championDamageShare_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        # DamageShare는 이미 있어서 안 넣음!!!!!!
        #creepScore = (row[f"creepScore_{i}"] + row[f"creepScore_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"creepScorePerTime_{i}"] = row[f"creepScore_{i}"] / row["duration"]
        #wardsPlaced = (row[f"wardsPlaced_{i}"] + row[f"wardsPlaced_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        wardsPlacedScore = row[f"wardsPlaced_{i}"] * 1.5        # 공식 시야점수를 계산하는 계산식에서는 와드를 놓은 게 와드를 지운 것보다 더 중요하다고 한다.
        #wardsDestroyed = (row[f"wardsDestroyed_{i}"] + row[f"wardsDestroyed_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"wardsScorePerTime_{i}"] = (wardsPlacedScore + row[f"wardsDestroyed_{i}"]) / row["duration"]
        #goldEarned = (row[f"totalGoldEarned_{i}"] + row[f"totalGoldEarned_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"goldEarnedPerTime_{i}"] = row[f"totalGoldEarned_{i}"] / row["duration"]
    for i in range(PARTICIPANTS_NUMBER_OF_A_TEAM):
        row[f"kdaof{numberToRoleName(i)}"] = (row[f"kda_{i}"] + row[f"kda_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"championDamageShareByRoleof{numberToRoleName(i)}"] = (row[f"championDamageShare_{i}"] + row[f"championDamageShare_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"creepScorePerTimeof{numberToRoleName(i)}"] = (row[f"creepScorePerTime_{i}"] + row[f"creepScorePerTime_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"wardsScorePerTimeof{numberToRoleName(i)}"] = (row[f"wardsScorePerTime_{i}"] + row[f"wardsScorePerTime_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"goldEarnedPerTimeof{numberToRoleName(i)}"] = (row[f"goldEarnedPerTime_{i}"] + row[f"goldEarnedPerTime_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
    return row

"""   
def calculateColumnsForModel(row, flag):
    PARTICIPANTS_NUMBER_OF_A_TEAM = 5
    for i in range(PARTICIPANTS_NUMBER_OF_A_TEAM):
        kills = row[f"kills_{i}"]
        deaths = row[f"deaths_{i}"]
        assists = row[f"assists_{i}"]
        if deaths == 0:                         
            kda_blue = (kills + assists) * 1.2   # LCK에서는 노데스로 끝난 선수에게는 1데스시 kda의 1.2배를 곱한 값을 kda로 표시해주고 있다.
        else:
            kda_blue = (kills + assists) / deaths
        kills = row[f"kills_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]
        deaths = row[f"deaths_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]
        assists = row[f"assists_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]
        if deaths == 0:                         
            kda_red = (kills + assists) * 1.2   # LCK에서는 노데스로 끝난 선수에게는 1데스시 kda의 1.2배를 곱한 값을 kda로 표시해주고 있다.
        else:
            kda_red = (kills + assists) / deaths
        row[f"kdaof{numberToRoleName(i)}"] = (kda_blue + kda_red) / 2

        row[f"championDamageShareByRoleof{numberToRoleName(i)}"] = (row[f"championDamageShare_{i}"] + row[f"championDamageShare_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        creepScore = (row[f"creepScore_{i}"] + row[f"creepScore_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"creepScorePerTimeof{numberToRoleName(i)}"] = creepScore / row["duration"]
        wardsPlaced = (row[f"wardsPlaced_{i}"] + row[f"wardsPlaced_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        wardsPlacedScore = wardsPlaced * 1.5        # 공식 시야점수를 계산하는 계산식에서는 와드를 놓은 게 와드를 지운 것보다 더 중요하다고 한다.
        wardsDestroyed = (row[f"wardsDestroyed_{i}"] + row[f"wardsDestroyed_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"wardsScorePerTimeof{numberToRoleName(i)}"] = (wardsPlacedScore + wardsDestroyed) / row["duration"]
        goldEarned = (row[f"totalGoldEarned_{i}"] + row[f"totalGoldEarned_{i + PARTICIPANTS_NUMBER_OF_A_TEAM}"]) / 2
        row[f"goldEarnedPerTimeof{numberToRoleName(i)}"] = goldEarned / row["duration"]
    return row
"""