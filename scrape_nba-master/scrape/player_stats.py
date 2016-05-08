import json
import urllib2

import scrape.helper

class PlayerData:
    def __init__(self, player_id, player_name, season, season_type):
        self.player_id = player_id
        self.player_name = player_name

        self.player_tracking_shot_logs_url = "http://stats.nba.com/stats/playerdashptshotlog?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&PlayerID="+player_id+"&Season="+season+"&SeasonSegment=&SeasonType="+season_type+"&TeamID=0&VsConference=&VsDivision="
        self.player_tracking_rebound_logs_url = "http://stats.nba.com/stats/playerdashptreboundlogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&PlayerID="+player_id+"&Season="+season+"&SeasonSegment=&SeasonType="+season_type+"&TeamID=0&VsConference=&VsDivision="
        self.player_tracking_passes_url = "http://stats.nba.com/stats/playerdashptpass?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID="+player_id+"&Season="+season+"&SeasonSegment=&SeasonType="+season_type+"&TeamID=0&VsConference=&VsDivision="

    def shot_logs(self):
        return scrape.helper.get_data_from_url_add_player_id(self.player_tracking_shot_logs_url, self.player_id, self.player_name, 0)

    def rebound_logs(self):
        return scrape.helper.get_data_from_url_add_player_id(self.player_tracking_rebound_logs_url, self.player_id, self.player_name, 0)

    def passes_made(self):
        return scrape.helper.get_data_from_url(self.player_tracking_passes_url, 0)

    def passes_received(self):
        return scrape.helper.get_data_from_url(self.player_tracking_passes_url, 1)
