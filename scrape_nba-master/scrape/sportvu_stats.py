import json
import urllib2

import helper

def get_sportvu_data_for_stat(season, season_type, player_or_team, measure_type, start_date="", end_date=""):
    
    start = start_date.split("-")[1] + "%2F" + start_date.split("-")[2] + "%2F" + start_date.split("-")[0]
    end = end_date.split("-")[1] + "%2F" + end_date.split("-")[2] + "%2F" + end_date.split("-")[0]
    url = "http://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom="+start+"&DateTo="+end+"&Division=&DraftPick=&DraftYear=&GameScope=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam="+player_or_team+"&PlayerPosition=&PtMeasureType="+measure_type+"&Season="+season+"&SeasonSegment=&SeasonType="+season_type+"&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
    return helper.get_data_from_url(url, 0)

def add_game_id_to_game_log_for_player(daily_data, date, storage, player_game_map):
    games = storage.query('select GAME_ID from game_summary where GAME_DATE_EST = "' + date + 'T00:00:00"')
    to_return = []
    for row in daily_data:
        player_id = str(row["PLAYER_ID"])
        for game in games:
            game_id = game[0]
            if game_id in player_game_map[player_id].keys():
                row["GAME_ID"] = game_id
                row["TEAM_ID"] = player_game_map[player_id][game_id]
                to_return.append(row)
                break
    return to_return

def add_game_id_to_game_log_for_team(daily_data, date, storage, team_game_map):
    games = storage.query('select GAME_ID from game_summary where GAME_DATE_EST = "' + date + 'T00:00:00"')
    to_return = []
    for row in daily_data:
        team_id = str(row["TEAM_ID"])
        for game in games:
            game_id = game[0]
            if game_id in team_game_map[team_id].keys():
                row["GAME_ID"] = game_id
                to_return.append(row)
                break
    return to_return

def get_sportvu_data_game_logs(season, date, season_type, player_or_team, measure_type):
    
    player_game_map = {}        
    game_ids = helper.get_game_ids_for_date(date.strftime("%Y-%m-%d")) #gets the selected dates
    
    boxscore_base_url = "http://stats.nba.com/stats/boxscoretraditionalv2?"
    for game_id in game_ids:
        boxscore_parameters = {
            "GameId" : game_id,
            "StartPeriod": 0,
            "EndPeriod": 10,
            "RangeType": 2,
            "StartRange": 0,
            "EndRange": 55800
        }
        
        player_boxscore_data = helper.get_data_from_url_params(boxscore_base_url, boxscore_parameters, 0)
        
        #create map of PLAYER_ID:GAME_ID pairs for every player
        for player_data in player_boxscore_data:
            player_game_map[player_data['PLAYER_ID']] = player_data['GAME_ID']

    player_tracking_data = get_sportvu_data_for_stat(season, season_type, player_or_team, measure_type, date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d"))

    for i in range(len(player_tracking_data)):
        player_tracking_data[i]['GAME_ID'] = player_game_map[player_tracking_data[i]['PLAYER_ID']]
        
    return player_tracking_data
