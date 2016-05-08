# Takes two arguments, start date and end date. Date format YYYY-MM-DD. If no dates entered gets yesterday
import json
import sys
import datetime
from dateutil.rrule import rrule, DAILY
import logging
import re
import scrape.helper
import storage.db

from scrape import game_stats


def main():
    '''
    if len(sys.argv) < 3:
        start_date = datetime.date.today() - datetime.timedelta(1)
        end_date = datetime.date.today() - datetime.timedelta(1)
    elif len(sys.argv) > 3:
        print "Too many arguments. Enter a start and end date with format YYYY-MM-DD"
        sys.exit(0)
    else:
        start = sys.argv[1]
        end = sys.argv[2]
        # validate dates
        try:
            datetime.datetime.strptime(start, '%Y-%m-%d')
        except:
            print 'invalid format for start date'
            sys.exit(0)

        try:
            datetime.datetime.strptime(end, '%Y-%m-%d')
        except:
            print 'invalid format for end date'
            sys.exit(0)

        start_split = start.split("-")
        end_split = end.split("-")

        start_date = '2015-11-14'
        end_date = '2015-11-14'
        start_date = datetime.date(int(start_split[0]), int(start_split[1]), int(start_split[2]))
        end_date = datetime.date(int(end_split[0]), int(end_split[1]), int(end_split[2]))
     ''' 
    
    logging.basicConfig(filename='games.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())

        
    start_date = datetime.date(2016,4,12)
    end_date = datetime.date(2016,4,12)

    season = config["season"]
    # make sure season is valid format
    season_pattern = re.compile('\d{4}[-]\d{2}$')
    if season_pattern.match(season) == None:
        print "Invalid Season format. Example format: 2014-15"
        sys.exit(0)

    db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])

    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        print dt
        games = scrape.helper.get_game_ids_for_date(dt.strftime("%Y-%m-%d"))
        print games
        for game_id in games:
            if game_id[:3] == "002" or game_id[:3] == "004":
                try:
                    game_data = game_stats.GameData(game_id, season)
                    print "saved: "+ game_id
                    
                    db_storage.insert(game_data.traditional_boxscore(), "traditional_boxscores")
                    """
                    db_storage.insert(game_data.traditional_boxscore_q1(), "traditional_boxscores_q1")
                    db_storage.insert(game_data.traditional_boxscore_q2(), "traditional_boxscores_q2")
                    db_storage.insert(game_data.traditional_boxscore_q3(), "traditional_boxscores_q3")
                    db_storage.insert(game_data.traditional_boxscore_q4(), "traditional_boxscores_q4")
                    db_storage.insert(game_data.pbp(), "pbp")
                    db_storage.insert(game_data.player_tracking_boxscore(), "player_tracking_boxscores")
                    db_storage.insert(game_data.player_tracking_boxscore_team(), "player_tracking_boxscores_team")
                    db_storage.insert(game_data.shots(), "shots")
                    db_storage.insert(game_data.traditional_boxscore(), "traditional_boxscores")
                    db_storage.insert(game_data.traditional_boxscore_team(), "traditional_boxscores_team")
                    db_storage.insert(game_data.advanced_boxscore(), "advanced_boxscores")
                    db_storage.insert(game_data.advanced_boxscore_team(), "advanced_boxscores_team")
                    db_storage.insert(game_data.scoring_boxscore(), "scoring_boxscores")
                    db_storage.insert(game_data.scoring_boxscore_team(), "scoring_boxscores_team")
                    db_storage.insert(game_data.misc_boxscore(), "misc_boxscores")
                    db_storage.insert(game_data.misc_boxscore_team(), "misc_boxscores_team")
                    db_storage.insert(game_data.usage_boxscore(), "usage_boxscores")
                    db_storage.insert(game_data.four_factors_boxscore(), "four_factors_boxscores")
                    db_storage.insert(game_data.four_factors_boxscore_team(), "four_factors_boxscores_team")
                    
                    db_storage.insert(game_data.line_score(), "line_score")
                    db_storage.insert(game_data.other_stats(), "other_stats")
                    db_storage.insert(game_data.officials(), "officials")
                    db_storage.insert(game_data.inactives(), "inactives")
                    """
                    
                    #db_storage.insert(game_data.game_info(), "game_info")
                    #db_storage.insert(game_data.game_summary(), "game_summary")
                    
                    db_storage.commit()
                except:
                    logging.error('game %s not stored', game_id)
            
    db_storage.close()

if __name__ == '__main__':
    main()
