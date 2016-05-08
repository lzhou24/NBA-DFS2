import json
import requests
import urllib2

HEADER = {'User-agent':'Mozilla/5.0', 'referer': 'http://stats.nba.com/scores/'}


def get_data_probe(url, index):
    response = requests.get(url)
    data = json.loads(response.text)
    return data    

def get_data_from_url(url, index):
    response = requests.get(url, headers=HEADER)
    data = json.loads(response.text)
    headers = data['resultSets'][index]['headers']
    rows = data['resultSets'][index]['rowSet']
    return [dict(zip(headers, row)) for row in rows]

def get_data_from_url_add_player_id(url, player_id, player_name, index):
    response = urllib2.urlopen(url)
    data = json.loads(response.read())
    headers = data['resultSets'][index]['headers']
    headers = ["PLAYER_ID", "PLAYER_NAME"] + headers
    rows = data['resultSets'][index]['rowSet']
    return [dict(zip(headers, [player_id, player_name] + row)) for row in rows]

def get_data_from_url_add_game_id(url, game_id, index):
    response = urllib2.urlopen(url)
    data = json.loads(response.read())
    headers = data['resultSets'][index]['headers']
    headers = ["GAME_ID"] + headers
    rows = data['resultSets'][index]['rowSet']
    return [dict(zip(headers, [game_id] + row)) for row in rows]

def get_data_from_url_rename_columns(url, renamed_columns, index):
    response = urllib2.urlopen(url)
    data = json.loads(response.read())
    headers = data['resultSets'][index]['headers']
    for key in renamed_columns.keys():
        headers = [renamed_columns[key] if x==key else x for x in headers]
    rows = data['resultSets'][index]['rowSet']
    return [dict(zip(headers, row)) for row in rows]


def get_game_ids_for_date(date):
    # date format YYYY-MM-DD (string)
    game_ids = []
    split = date.split("-")
    url = "http://stats.nba.com/stats/scoreboardV2?DayOffset=0&LeagueID=00&gameDate="+split[1]+"%2F"+split[2]+"%2F"+split[0]
    games = get_data_from_url(url, 1)
    for game in games:
        game_ids.append(game['GAME_ID'])
    
    return list(set(game_ids))

def get_data_from_url_params(base_url, parameters, index):
    response = requests.get(base_url, headers={'User-agent':'Mozilla/5.0'}, params=parameters)
    
    data = response.json()
    headers = data['resultSets'][index]['headers']
    rows = data['resultSets'][index]['rowSet']
    return [dict(zip(headers, row)) for row in rows]
    
def boxscore_fix_min(dataframe):
    minutes_split = dataframe['MIN'].str.split(":")
    minutes_split= [[0, 0] if x is None else x for x in minutes_split]
    dataframe = dataframe[:].fillna(0)
    dataframe[['MIN']] = [minutes_split[i][0] for i in range(len(dataframe['MIN']))]
    return dataframe
    
def get_game_schedule_for_date(date):
    # date format YYYY-MM-DD (string)
    teams = []
    split = date.split("-")
    url = "http://stats.nba.com/stats/scoreboardV2?DayOffset=0&LeagueID=00&gameDate="+split[1]+"%2F"+split[2]+"%2F"+split[0]
    games = get_data_from_url(url, 1)    
    for game in games:
        teams.append(game['TEAM_ABBREVIATION'])
    return teams