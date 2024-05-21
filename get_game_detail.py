import os
import requests
import json
import datetime
import time
import pandas as pd
from tqdm import tqdm

# 응답을 컬럼에맞춰 넣어 리스트형태로 반환하는 함수입니다.
# data 에는 request를 통해 얻은 데이터가 들어있고, flag에는 무슨 목적으로 이 함수를 호출했는지를 구분할 목적의 정수가 들어갑니다.
# flag      0 : getDetails를 요청해 받은 데이터일 경우
#           1 : getWindow를 통해 받은 데이터일 경우
#           2 : getWindow에서 플레이어 정보만 얻고 싶을 때
def jsonParser(data, flag):
    result_data = []                                                                # 결과데이터가 들어갈 리스트 초기화
    # JSON 데이터 파싱
    json = data.json()
    if flag == 0:                                                                   # getDetails을 통해 받은 응답일 경우 flag : 0
        # 필요한 데이터 추출 (여기서는 참가자 데이터만 추출하여 리스트에 추가)
        if 'frames' in json and len(json['frames']) > 0:                            # 데이터 덩어리가 들어 있는지 검사
            frame = json['frames'][0]                                               # 커서를 frame쪽으로 옮겨주고
            for participant in frame['participants']:                               # 모든 participant(여기서는 블루팀5명, 레드팀 5명 총 10명)에 대해
                participant_data = {
                    "timestamp": frame['rfc460Timestamp'][:19] + "Z",               # 시간
                    "participantId": participant['participantId'],                  # 1~5 : 블루팀 탑~서폿, 6~10 : 레드팀 탑~서폿
                    "level": participant['level'],                                  # 챔피언 레벨
                    "kills": participant['kills'],                                  # 킬수
                    "deaths": participant['deaths'],                                # 데스 수
                    "assists": participant['assists'],                              # 어시 수
                    "totalGoldEarned": participant['totalGoldEarned'],              # 벌어들인 골드
                    "creepScore": participant['creepScore'],                        # CS
                    "killParticipation": participant['killParticipation'],          # 킬 관여율
                    "championDamageShare": participant['championDamageShare'],      # 데미지 기여율
                    "wardsPlaced": participant['wardsPlaced'],                      # 와드 놓은 횟수
                    "wardsDestroyed": participant['wardsDestroyed'],                # 상대편의 와드를 부순 횟수
                    "attackDamage": participant['attackDamage'],                    # 일반 공격력
                    "abilityPower": participant['abilityPower'],                    # ap 공격력
                    "criticalChance": participant['criticalChance'],                # 치명타율
                    "attackSpeed": participant['attackSpeed'],                      # 공격 속도
                    "lifeSteal": participant['lifeSteal'],                          # 피 흡수
                    "armor": participant['armor'],                                  # 물리 방어력
                    "magicResistance": participant['magicResistance'],              # 마법 방어력
                    "tenacity": participant['tenacity'],                            # cc 저항력? 이라는데 맞나요?
                    "items": participant['items'],                                  # 보유한 아이템
                    "perkMetadata": participant['perkMetadata'],                    # 어... 특성 같은건가?
                    "abilities": participant['abilities']                           # 스킬 찍은 순서
                }
                result_data.append(participant_data)                                # 끼워맞춘 데이터를 리스트에 하나씩 넣어줌
                
    elif flag == 1:                                                                 # getWindow를 통해 받은 응답일 경우 flag : 1
         # 필요한 데이터 추출(팀 데이터)
        if 'frames' in json and len(json['frames']) > 0:                            # 데이터 덩어리가 들어 있는지 검사
            frame = json['frames'][0]                                               # 커서를 frame으로 옮겨줌
            blueteam = frame["blueTeam"]
            redteam = frame["redTeam"]
            for participant in blueteam['participants']:
                participant_data = {
                    "timestamp": frame['rfc460Timestamp'][:19] + "Z",               # 시간 데이터, 중복검사를 위해 넣음
                    "currentHealth": participant["currentHealth"],                  # 선수의 현재 체력
                    "maxHealth": participant["maxHealth"],                          # 선수의 최대 체력(풀피)
                    "blue_totalGold": blueteam["totalGold"],                        # 블루팀의 골드 획득량
                    "blue_inhibitors": blueteam["inhibitors"],                      # 블루팀이 부순 억제기 갯수
                    "blue_towers": blueteam["towers"],                              # 블루팀이 부순 타워 갯수
                    "blue_barons": blueteam["barons"],                              # 블루팀이 먹은 바론 갯수
                    "blue_totalKills": blueteam["totalKills"],                      # 블루팀 총 킬 수
                    "blue_dragons": blueteam["dragons"],                            # 블루팀이 먹은 드래곤 (리스트)
                    "red_totalGold": redteam["totalGold"],                          # 레드팀의 골드 획득량
                    "red_inhibitors": redteam["inhibitors"],                        # 레드팀이 부순 억제기 갯수
                    "red_towers": redteam["towers"],                                # 레드팀이 부순 타워 갯수
                    "red_barons": redteam["barons"],                                # 레드팀이 먹은 바론 갯수
                    "red_totalKills": redteam["totalKills"],                        # 레드팀 총 킬 수
                    "red_dragons": redteam["dragons"],                              # 레드팀이 먹은 드래곤(리스트)
                    "participantId": participant['participantId'],                  # 1~5 : 블루팀 탑~서폿, 6~10 : 레드팀 탑~서폿
                    "gameState": frame["gameState"]
                }
                result_data.append(participant_data)                                # 리스트에 넣어줌
            for participant in redteam['participants']:
                participant_data = {
                    "timestamp": frame['rfc460Timestamp'][:19] + "Z",               # 시간 데이터, 중복검사를 위해 넣음
                    "currentHealth": participant["currentHealth"],                  # 선수의 현재 체력
                    "maxHealth": participant["maxHealth"],                          # 선수의 최대 체력(풀피)
                    "blue_totalGold": blueteam["totalGold"],                        # 블루팀의 골드 획득량
                    "blue_inhibitors": blueteam["inhibitors"],                      # 블루팀이 부순 억제기 갯수
                    "blue_towers": blueteam["towers"],                              # 블루팀이 부순 타워 갯수
                    "blue_barons": blueteam["barons"],                              # 블루팀이 먹은 바론 갯수
                    "blue_totalKills": blueteam["totalKills"],                      # 블루팀 총 킬 수
                    "blue_dragons": blueteam["dragons"],                            # 블루팀이 먹은 드래곤 (리스트)
                    "red_totalGold": redteam["totalGold"],                          # 레드팀의 골드 획득량
                    "red_inhibitors": redteam["inhibitors"],                        # 레드팀이 부순 억제기 갯수
                    "red_towers": redteam["towers"],                                # 레드팀이 부순 타워 갯수
                    "red_barons": redteam["barons"],                                # 레드팀이 먹은 바론 갯수
                    "red_totalKills": redteam["totalKills"],                        # 레드팀 총 킬 수
                    "red_dragons": redteam["dragons"],                              # 레드팀이 먹은 드래곤(리스트)
                    "participantId": participant['participantId'],                  # 1~5 : 블루팀 탑~서폿, 6~10 : 레드팀 탑~서폿
                    "gameState": frame["gameState"]
                }
                result_data.append(participant_data)                                # 리스트에 넣어줌
                
    elif flag == 2:                                                                 # getWindow에서 플레이어 정보를 얻고 싶을때는 flag : 2
        # 필요한 데이터 추출
        if 'gameMetadata' in json and len(json['gameMetadata']) > 0:                # gameMetadata 데이터 덩어리가 있으면
            game = json['gameMetadata']
            
            blueteam = game['blueTeamMetadata']
            blueteamplayers = blueteam['participantMetadata']
            for player in blueteamplayers:                                          # 모든 블루팀 선수에 대해
                summonerName = player["summonerName"]
                blank_where = summonerName.find(" ")
                team = summonerName[:blank_where]                                   # 팀 코드
                name = summonerName[blank_where + 1:]                               # 소환사명
                player_info = {
                    "esportsTeamId" : blueteam["esportsTeamId"],                    # 팀 고유 ID
                    "esportsPlayerId" : player["esportsPlayerId"],                  # 선수의 고유 ID
                    "participantId" : player["participantId"],                      # 1~5 : 블루팀 탑~서폿, 6~10 : 레드팀 탑~서폿
                    "teamCode" : team,                                              # 팀 코드
                    "summonerName" : name,                                          # 소환사명
                    "championId" : player["championId"],                            # 챔피언 이름
                    "role" : player["role"],                                        # 포지션(탑, 정글 등)
                }
                result_data.append(player_info)                                     # 리스트에 넣어줌
                
            redteam = game['redTeamMetadata']
            redteamplayers = redteam['participantMetadata']
            for player in redteamplayers:
                summonerName = player["summonerName"]
                blank_where = summonerName.find(" ")
                team = summonerName[:blank_where]                                   # 팀 코드
                name = summonerName[blank_where + 1:]                               # 소환사명
                player_info = {
                    "esportsTeamId" : redteam["esportsTeamId"],                     # 팀 고유 ID
                    "participantId" : player["participantId"],                      # 1~5 : 블루팀 탑~서폿, 6~10 : 레드팀 탑~서폿
                    "esportsPlayerId" : player["esportsPlayerId"],                  # 선수의 고유 ID
                    "teamCode" : team,                                              # 팀 코드
                    "summonerName" : name,                                          # 소환사명
                    "championId" : player["championId"],                            # 챔피언 이름
                    "role" : player["role"],                                        # 포지션(탑, 정글 등)
                }
                result_data.append(player_info)                                     # 리스트에 넣어줌
    return result_data

# gameID를 받아 request 요청을 보내고, api로부터 데이터를 받아 중복검사 이후 데이터프레임으로 리턴하는 함수입니다.
def getGameStatusOrderedbyTime(game_id):
    # URL 설정
    getDetails_url = f"https://feed.lolesports.com/livestats/v1/details/{game_id}"  # getDetails 요청 cf. https://vickz84259.github.io/lolesports-api-docs/#operation/getDetails
    getWindow_url = f"https://feed.lolesports.com/livestats/v1/window/{game_id}"    # getWindow 요청 cf. https://vickz84259.github.io/lolesports-api-docs/#operation/getWindow
    
    # 결과 저장 리스트
    participants_data = []
    window_data = []
    
    # 중복검사용 변수 세팅 및 초기화
    previous_data = None
    repetition_count = 0
    INITIAL_COLLECTION_PERIOD = 60  # 중복 검사를 시작하기 전에 수집할 초기 데이터 수
    MAX_REPETITION_COUNT = 6

    HTTP_OK = 200
    REQUEST_INTERVAL = 0.1
    
    # 경기시작 시간 알아내기
    apiResult = requestWithHandlingHttperr(f"{getDetails_url}")
        
    if apiResult.status_code == HTTP_OK:
        starting_time = datetime.datetime.strptime(jsonParser(apiResult, 0)[0]["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")  # Zulu time 표시를 빼고 앞부분만 쓰기 위해 19까지 슬라이스.
        starting_time = starting_time - datetime.timedelta(seconds = starting_time.second % 10)                         # 이유는 모르겠는데 10초 단위로만 요청할 수 있는 것 같다. 
                                                                                                                        # 34초 데이터 주세요 이런거 요청하면 400 에러 뜸. 그래서 초를 10초단위로 끊음.
    else:
        print(f"Failed to fetch data from game ID : {game_id}, Status code {apiResult.status_code}")
        return [str(game_id), apiResult.status_code] # 비정상 응답이 온 game id를 리턴
    
    # 요청 간 0.1초 지연
    time.sleep(REQUEST_INTERVAL)
    
    # 최대 2시간 동안 10초 단위로 데이터 수집
    for i in tqdm(range(720), desc=f"Collecting Details Of GameID : {game_id}", leave = False):
        # 요청할 시간 계산
        query_time = starting_time + datetime.timedelta(seconds=i * 10)
        query_time_str = query_time.strftime("%Y-%m-%dT%H:%M:%S") + "Z" # 형식 맞춰주기
        
        apiResult = requestWithHandlingHttperr(f"{getDetails_url}?startingTime={query_time_str}") # API에서 getDetails 요청
        
        if apiResult.status_code == HTTP_OK:
            detail = jsonParser(apiResult, 0)       # get으로 받은 결과를 넘겨주고 리스트를 받는 함수. 2번째 인수로 무슨 타입인지 구분(0 : getDetails, 1 : getWindow)
            if detail[0]["attackDamage"] != 0:      # 게임 시작 시 잠깐동안 전부 0으로 나오는 데이터는 포함하지 않음.
                if i < INITIAL_COLLECTION_PERIOD:   # 처음 조금동안은 중복검사를 하지 않습니다.
                    participants_data.extend(detail)
                else:                               # 중복 데이터 확인 및 반복 카운트
                    if previous_data is not None and (previous_data[0]["timestamp"] == detail[0]["timestamp"]): # == 만으로는 중복데이터 확인이 안 되어 시간을 비교
                        repetition_count += 1
                        if repetition_count >= MAX_REPETITION_COUNT:
                            break
                    else:
                        repetition_count = 0
                        previous_data = detail
                        participants_data.extend(detail)
        else:                                       # 응답에 뭔가 문제가 생겼을 때
            print(f"Failed to fetch data for {query_time_str} from game ID : {game_id}, Status code {apiResult.status_code}")
            return [str(game_id), apiResult.status_code, query_time_str] # 비정상 응답이 온 game id를 리턴
        
        time.sleep(REQUEST_INTERVAL)                             # 요청 간 0.1초 지연
        
    for i in tqdm(range(720), desc=f"Collecting Windows Of GameID : {game_id}", leave = False):
        # 요청할 시간 계산
        query_time = starting_time + datetime.timedelta(seconds=i * 10)
        query_time_str = query_time.strftime("%Y-%m-%dT%H:%M:%S") + "Z" # 형식 맞춰주기
        
        apiResult = requestWithHandlingHttperr(f"{getWindow_url}?startingTime={query_time_str}")  # API에서 getWindow 요청
        
        if apiResult.status_code == HTTP_OK:
            window = jsonParser(apiResult, 1)                                               # get으로 받은 결과를 넘겨주고 리스트를 받는 함수. 2번째 인수로 무슨 타입인지 구분(0 : getDetails, 1 : getWindow)
            if window[0]["maxHealth"] != 0:                                                 # 게임 시작 시 잠깐동안 전부 0으로 나오는 데이터는 포함하지 않음.
                if i < INITIAL_COLLECTION_PERIOD:                                           # 처음 조금동안은 중복검사를 하지 않습니다.
                    window_data.extend(window)
                else:                                                                       # 중복 데이터 확인 및 반복 카운트
                    if previous_data is not None and (previous_data[0]["timestamp"] == window[0]["timestamp"]): # == 만으로는 중복데이터 확인이 안 되어 시간을 비교
                        repetition_count += 1
                        if repetition_count >= MAX_REPETITION_COUNT:
                            break
                    else:
                        repetition_count = 0
                        previous_data = window
                        window_data.extend(window)
        else:                                       # 응답에 뭔가 문제가 생겼을 때
            print(f"Failed to fetch data for {query_time_str} from game ID : {game_id}, Status code {apiResult.status_code}")
            return [str(game_id), apiResult.status_code, query_time_str] # 비정상 응답이 온 game id를 리턴
            
        time.sleep(REQUEST_INTERVAL)                             # 요청 간 0.1초 지연
        
    participants_df = pd.DataFrame(participants_data)
    window_df = pd.DataFrame(window_data)
    df = pd.merge(participants_df, window_df, how = "left", on = ["timestamp", "participantId"])
    return df

# getWindow 요청을 보내 api로부터 선수 정보를 받아옵니다.
def getParticipantInfo(game_id):
    HTTP_OK = 200
    
    # URL 설정
    window_url = f"https://feed.lolesports.com/livestats/v1/window/{game_id}" # getWindow 요청 cf. https://vickz84259.github.io/lolesports-api-docs/#operation/getWindow)

    # 요청 날리기
    apiResult = requestWithHandlingHttperr(f"{window_url}")
    
    # 응답이 정상이면
    if apiResult.status_code == HTTP_OK:
        return pd.DataFrame(jsonParser(apiResult, 2))
    else:
        print(f"Failed to fetch data from game ID : {game_id},  Status code {apiResult.status_code}")
        return [str(game_id), apiResult.status_code]                # 비정상 응답이 온 game id를 리턴
    
# try-except를 통해 서버로부터 10054에러가 떴을 때 잠시 기다렸다 재시도하는 루틴입니다.
def requestWithHandlingHttperr(url):
    RETRY_COUNT = 12            # 기본 반복 12회
    SLEEP_DELAY = 10            # 대기 10초
    ERRNO_10054 = 10054

    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}  # 서버에 내 신분을 속이기 위한 유저에이전트... 자세히는 잘 모릅니다

    for i in range(RETRY_COUNT):
        try:
            result = requests.get(url, headers = headers)       # API에 request 요청
            result.raise_for_status()                           # http에러가 나오면 예외를 발생시킴 -> except로 점프
            return result
        except ConnectionError as e:
            if isinstance(e.args[0], ConnectionResetError) and e.args[0].winerror == ERRNO_10054:
        #except requests.exceptions.RequestException as e:
            #if e.errno == ERRNO_10054:            # http에러 10054일때
                print(f"Attempt {i + 1} failed with error 10054. Retrying in {SLEEP_DELAY} seconds...")
                time.sleep(SLEEP_DELAY)
            else:                           # 다른 http 에러면
                raise
    game_id = url[url.rfind("/") + 1:]
    print(f"Failed to fetch data from game ID : {game_id} after {RETRY_COUNT} attempts.")
    # raise Exception(f"Failed to fetch data from game ID : {game_id} after {RETRY_COUNT} attempts.")

###### 아래부턴 실행되는 부분 ######
id_list = [28, 29, 33] # <- 요 부분에 원하는 숫자 넣고 돌리시면 됩니다.
for i in id_list:
    game_ids = pd.read_excel(f"data/game_ids/game_id_{i}.xlsx") 

    for idx, row in tqdm(game_ids.iterrows(), desc="Entire Progress", total = len(game_ids)):  # 게임 번호를 하나씩 row에 넣어 분기를 돌립니다.
        game_id = row["ID"]
        playerinfo = getParticipantInfo(game_id)                        # 플레이어 정보를 뽑아냄
        playerStatus = getGameStatusOrderedbyTime(game_id)              # 게임 상세 데이터를 뽑아냄
        
        # data폴더 내에 xlsx 파일로 저장
        os.makedirs('data', exist_ok=True)

        if type(playerinfo) is list:                                # 정상적인 데이터가 모이지 않았을 때 getParticipantInfo()와 getGameStatusOrderedbyTime()은 List를 반환합니다.
            df = pd.Series(playerinfo).to_frame().T
            if df.shape[1] == 2:                                    # 데이터 요청 처음부터 망했을 때
                df.columns = ["game_id", "status_code"]
            elif df.shape[1] == 3:                                  # 데이터를 한참 시간순서대로 받는 도중에 망했을 때
                df.columns = ["game_id", "status_code", "timestamp"]
            df.to_excel(f'data/collected_data/{game_id}_invalid.xlsx', index=False)
        elif type(playerStatus) is list:
            df = pd.Series(playerinfo).to_frame().T
            if df.shape[1] == 2:                                    # 데이터 요청 처음부터 망했을 때
                df.columns = ["game_id", "status_code"]
            elif df.shape[1] == 3:                                  # 데이터를 한참 시간순서대로 받는 도중에 망했을 때
                df.columns = ["game_id", "status_code", "timestamp"]
            df.to_excel(f'data/collected_data/{game_id}_invalid.xlsx', index=False)
        else:                                                       # 정상이면
            df = pd.merge(playerinfo, playerStatus, how = "right", left_on = "participantId", right_on = "participantId") # 플레이어 정보랑 게임 데이터를 합쳐줍니다.
            df.to_excel(f'data/collected_data/{game_id}.xlsx', index=False)
    print(f"Collecting data from game_id_{i} is completed!")