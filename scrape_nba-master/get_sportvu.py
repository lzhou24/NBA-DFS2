import json
import logging
import sys
import re
import pandas as pd
import datetime
from datetime import date, timedelta
from dateutil.rrule import rrule, DAILY

from scrape import sportvu_stats, helper, game_stats
import storage.db


def main():
    logging.basicConfig(filename='sportvu.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())

    season = config["season"]
    is_regular_season = config["is_regular_season"]
    # make sure season is valid format
    season_pattern = re.compile('\d{4}[-]\d{2}$')
    if season_pattern.match(season) == None:
        print "Invalid Season format. Example format: 2014-15"
        sys.exit(0)
    year = season.split("-")[0]

    if is_regular_season == 0:
        season_type = "Playoffs"
    elif is_regular_season == 1:
        season_type = "Regular+Season"
    else:
        print "Invalid is_regular_season value. Use 0 for regular season, 1 for playoffs"

    db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])
    
    start_date = datetime.date(2016,2,1)
    end_date = datetime.date(2016,4,1)
    

    
    """
        start = date(2015, 10, 27)        
        delta = date.today()-start
    """
        #selected_day = date.today() - timedelta(1) #yesterday
         
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        
        selected_day = dt.strftime("%Y-%m-%d")
        print selected_day
        try:
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "Passing"), "sportvu_passing_game_logs", is_regular_season, str(selected_day))                    
            
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "CatchShoot"), "sportvu_catch_shoot_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season,dt, season_type, "Player", "Defense"), "sportvu_defense_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "Drives"), "sportvu_drives_game_logs", is_regular_season, str(selected_day))                    
  #            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, str(selected_day), season_type, "Player", "PullUpShot"), "sportvu_pull_up_shoot_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "Rebounding"), "sportvu_rebounding_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "Efficiency"), "sportvu_shooting_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "SpeedDistance"), "sportvu_speed_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "ElbowTouch"), "sportvu_elbow_touches_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "PostTouch"), "sportvu_post_touches_game_logs", is_regular_season, str(selected_day))                    
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "PaintTouch"), "sportvu_paint_touches_game_logs", is_regular_season, str(selected_day))
            db_storage.insert_with_date_and_season_type(sportvu_stats.get_sportvu_data_game_logs(season, dt, season_type, "Player", "Possessions"), "sportvu_possessions_game_logs", is_regular_season, str(selected_day))
            
            db_storage.commit()
        except:
            logging.error('sportvu not stored')
    db_storage.close()

if __name__ == '__main__':
    main()
