import json
import urllib2

import helper

class GameData:
    def __init__(self, game_id, season):
        self.game_id = game_id
        self.season = season

        self.pbp_url = "http://stats.nba.com/stats/playbyplayv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=55800"
        self.player_tracking_boxscore_url = "http://stats.nba.com/stats/boxscoreplayertrackv2?GameId="+game_id
        self.traditional_boxscore_url = "http://stats.nba.com/stats/boxscoretraditionalv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=55800"
        self.traditional_boxscore_url_q1 = "http://stats.nba.com/stats/boxscoretraditionalv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=7200"
        self.traditional_boxscore_url_q2 = "http://stats.nba.com/stats/boxscoretraditionalv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=7200&EndRange=14400"
        self.traditional_boxscore_url_q3 = "http://stats.nba.com/stats/boxscoretraditionalv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=14400&EndRange=21600"
        self.traditional_boxscore_url_q4 = "http://stats.nba.com/stats/boxscoretraditionalv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=21600&EndRange=28800"
        self.advanced_boxscore_url = "http://stats.nba.com/stats/boxscoreadvancedv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=55800"
        self.scoring_boxscore_url = "http://stats.nba.com/stats/boxscorescoringv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=55800"
        self.misc_boxscore_url = "http://stats.nba.com/stats/boxscoremiscv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=55800"
        self.usage_boxscore_url = "http://stats.nba.com/stats/boxscoreusagev2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=55800"
        self.four_factors_boxscore_url = "http://stats.nba.com/stats/boxscorefourfactorsv2?GameId="+game_id+"&StartPeriod=0&EndPeriod=10&RangeType=2&StartRange=0&EndRange=55800"
        self.summary_url = "http://stats.nba.com/stats/boxscoresummaryv2?GameId="+game_id
        self.teams = [self.player_tracking_boxscore_team()[0]['TEAM_ID'], self.player_tracking_boxscore_team()[1]['TEAM_ID']]


    def pbp(self):
        return helper.get_data_from_url(self.pbp_url, 0)

    def player_tracking_boxscore(self):
        return helper.get_data_from_url(self.player_tracking_boxscore_url, 0)

    def traditional_boxscore_q1(self):
        return helper.get_data_from_url(self.traditional_boxscore_url_q1, 0)

    def traditional_boxscore_q2(self):
        return helper.get_data_from_url(self.traditional_boxscore_url_q2, 0)

    def traditional_boxscore_q3(self):
        return helper.get_data_from_url(self.traditional_boxscore_url_q3, 0)

    def traditional_boxscore_q4(self):
        return helper.get_data_from_url(self.traditional_boxscore_url_q4, 0)        

    def player_tracking_boxscore_team(self):
        return helper.get_data_from_url(self.player_tracking_boxscore_url, 1)

    def traditional_boxscore(self):
        return helper.get_data_from_url(self.traditional_boxscore_url, 0)

    def traditional_boxscore_team(self):
        return helper.get_data_from_url(self.traditional_boxscore_url, 1)

    def advanced_boxscore(self):
        return helper.get_data_from_url(self.advanced_boxscore_url, 0)

    def advanced_boxscore_team(self):
        return helper.get_data_from_url(self.advanced_boxscore_url, 1)

    def scoring_boxscore(self):
        return helper.get_data_from_url(self.scoring_boxscore_url, 0)

    def scoring_boxscore_team(self):
        return helper.get_data_from_url(self.scoring_boxscore_url, 1)

    def misc_boxscore(self):
        return helper.get_data_from_url(self.misc_boxscore_url, 0)

    def misc_boxscore_team(self):
        return helper.get_data_from_url(self.misc_boxscore_url, 1)

    def usage_boxscore(self):
        return helper.get_data_from_url(self.usage_boxscore_url, 0)

    def four_factors_boxscore(self):
        return helper.get_data_from_url(self.four_factors_boxscore_url, 0)

    def four_factors_boxscore_team(self):
        return helper.get_data_from_url(self.four_factors_boxscore_url, 1)

    def shots(self):
        game_shots = []
        for team in self.teams:
            shots_url = "http://stats.nba.com/stats/shotchartdetail?Season="+self.season+"&SeasonType=Regular+Season&LeagueID=00&TeamID="+str(team)+"&PlayerID=0&GameID="+self.game_id+"&Outcome=&Location=&Month=0&SeasonSegment=&DateFrom=&DateTo=&OpponentTeamID=0&VsConference=&VsDivision=&Position=&RookieYear=&GameSegment=&Period=0&LastNGames=0&ContextFilter=&ContextMeasure=FG_PCT&display-mode=performance&zone-mode=zone&zoneOverlays=false&zoneDetails=false&viewShots=true"
            game_shots += helper.get_data_from_url(shots_url, 0)
        return game_shots

    def game_info(self):
        return helper.get_data_from_url_add_game_id(self.summary_url, self.game_id, 4)

    def game_summary(self):
        return helper.get_data_from_url(self.summary_url, 0)

    def line_score(self):
        return helper.get_data_from_url(self.summary_url, 5)

    def officials(self):
        return helper.get_data_from_url_add_game_id(self.summary_url,self.game_id, 2)

    def other_stats(self):
        return helper.get_data_from_url_add_game_id(self.summary_url, self.game_id, 1)

    def inactives(self):
        return helper.get_data_from_url_add_game_id(self.summary_url, self.game_id, 3)
