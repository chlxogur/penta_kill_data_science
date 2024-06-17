import pickle

def getMedian(role):
    role = role % 5
    if role == 0:
        median = {
            "kda" : 3.93333333333333,
            "killsPerTime" : 0.0012396694214876,
            "deathsPerTime" : 0.00136986301369863,
            "assistsPerTime" : 0.0023972602739726,
            "championDamageShare" : 0.228523154923377,
            "creepScorePerTime" : 0.133996402877698,
            "wardsScorePerTime" : 0.0128080095122445,
            "goldEarnedPerTime" : 6.4429331869941
        }
    elif role == 1:
        median = {
            "kda" : 5.0,
            "killsPerTime" : 0.00122448979591837,
            "deathsPerTime" : 0.00145348837209302,
            "assistsPerTime" : 0.00348360655737705,
            "championDamageShare" : 0.144554837154089,
            "creepScorePerTime" : 0.0917910447761194,
            "wardsScorePerTime" : 0.0148372890472669,
            "goldEarnedPerTime" : 5.60251262626263
        }
    elif role == 2:
        median = {
            "kda" : 5.3,
            "killsPerTime" : 0.00161290322580645,
            "deathsPerTime" : 0.00124223602484472,
            "assistsPerTime" : 0.0028125,
            "championDamageShare" : 0.261571385007903,
            "creepScorePerTime" : 0.143388523991972,
            "wardsScorePerTime" : 0.0131656804733728,
            "goldEarnedPerTime" : 6.70846681695934
        }
    elif role == 3:
        median = {
            "kda" : 5.79285714285714,
            "killsPerTime" : 0.00202312138728324,
            "deathsPerTime" : 0.00112107623318386,
            "assistsPerTime" : 0.00251396648044693,
            "championDamageShare" : 0.272831173686407,
            "creepScorePerTime" : 0.155673758865248,
            "wardsScorePerTime" : 0.016043778434033,
            "goldEarnedPerTime" : 7.26228078187797
        }
    elif role == 4:
        median = {
            "kda" : 4.83333333333333,
            "killsPerTime" : 0.000297619047619048,
            "deathsPerTime" : 0.0014792899408284,
            "assistsPerTime" : 0.0044793720078851,
            "championDamageShare" : 0.0746235333980816,
            "creepScorePerTime" : 0.0183673469387755,
            "wardsScorePerTime" : 0.0441531378196736,
            "goldEarnedPerTime" : 4.00327956989247
        }
    return median

def getPredictData(match):
    with open('../data/present_data.pkl', 'rb') as f:
        present_data = pickle.load(f)
    teams = match["teams"]
    blueteam = teams[0]
    redteam = teams[1]
    blueteam_id = blueteam["esportsTeamId"]
    redteam_id = redteam["esportsTeamId"]
    blueteam_players = blueteam["participantMetadata"]
    redteam_players = redteam["participantMetadata"]
    if present_data["team_history"][blueteam_id] is not None:
        blue_winrate = present_data['team_history'][blueteam_id]["self"]["winrate"]
        blue_golddiff = present_data['team_history'][blueteam_id]["self"]["golddiff"]
        blue_killdiff = present_data['team_history'][blueteam_id]["self"]["killdiff"]
    else:
        blue_winrate = 0.5
        blue_golddiff = 0
        blue_killdiff = 0
        if present_data["team_history"][blueteam_id][redteam_id] is not None:
            headtohead_winrate = present_data['team_history'][blueteam_id][redteam_id]["winrate"]
            headtohead_golddiff = present_data['team_history'][blueteam_id][redteam_id]["golddiff"]
            headtohead_killdiff = present_data['team_history'][blueteam_id][redteam_id]["killdiff"]
        else:
            headtohead_winrate = 0.5
            headtohead_golddiff = 0
            headtohead_killdiff = 0
    if present_data["team_history"][redteam_id] is not None:
        red_winrate = present_data['team_history'][redteam_id]["self"]["winrate"]
        red_golddiff = present_data['team_history'][redteam_id]["self"]["golddiff"]
        red_killdiff = present_data['team_history'][redteam_id]["self"]["killdiff"]
    else:
        red_winrate = 0.5
        red_golddiff = 0
        red_killdiff = 0
    team_winrate_diff = blue_winrate - red_winrate
    team_golddiff = blue_golddiff - red_golddiff
    team_killdiff = blue_killdiff - red_killdiff
    
    