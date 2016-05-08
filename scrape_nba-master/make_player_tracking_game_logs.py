# make player tracking game logs for date range, if no dates entered gets yesterday
import json
import logging
import sys
import re
import datetime
from dateutil.rrule import rrule, DAILY
import time

from scrape import sportvu_stats
import storage.db

def main():
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

        start_date = datetime.date(int(start_split[0]), int(start_split[1]), int(start_split[2]))
        end_date = datetime.date(int(end_split[0]), int(end_split[1]), int(end_split[2]))

    logging.basicConfig(filename='player_tracking_game_logs.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
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

    game_player_map = {}
    player_boxscores = db_storage.query("select GAME_ID, PLAYER_ID, TEAM_ID from traditional_boxscores")
    for player_line in player_boxscores:
        if player_line[1] in game_player_map.keys():
            game_player_map[player_line[1]][player_line[0]] = player_line[2]
        else:
            game_player_map[player_line[1]] = {player_line[0]:player_line[2]}

    game_team_map = {}
    team_boxscores = db_storage.query("select GAME_ID, TEAM_ID from traditional_boxscores_team")
    for team_line in team_boxscores:
        if team_line[1] in game_team_map.keys():
            game_team_map[team_line[1]][team_line[0]] = None
        else:
            game_team_map[team_line[1]] = {team_line[0]:None}

    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        day = dt.strftime("%d")
        month = dt.strftime("%m")
        year = dt.strftime("%Y")

        date = month + "%2F" + day + "%2F" + year
        date_to_store = year + "-" + month + "-" + day

        try:
            catch_shoot_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "CatchShoot", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(catch_shoot_player, date_to_store, db_storage, game_player_map), "sportvu_catch_shoot_game_logs", is_regular_season, date=date_to_store)
            catch_shoot_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "CatchShoot", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(catch_shoot_team, date_to_store, db_storage, game_team_map), "sportvu_catch_shoot_team_game_logs", is_regular_season, date=date_to_store)

            defense_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "Defense", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(defense_player, date_to_store, db_storage, game_player_map), "sportvu_defense_game_logs", is_regular_season, date=date_to_store)
            defense_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "Defense", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(defense_team, date_to_store, db_storage, game_team_map), "sportvu_defense_team_game_logs", is_regular_season, date=date_to_store)

            drives_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "Drives", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(drives_player, date_to_store, db_storage, game_player_map), "sportvu_drives_game_logs", is_regular_season, date=date_to_store)
            drives_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "Drives", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(drives_team, date_to_store, db_storage, game_team_map), "sportvu_drives_team_game_logs", is_regular_season, date=date_to_store)

            passing_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "Passing", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(passing_player, date_to_store, db_storage, game_player_map), "sportvu_passing_game_logs", is_regular_season, date=date_to_store)
            passing_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "Passing", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(passing_team, date_to_store, db_storage, game_team_map), "sportvu_passing_team_game_logs", is_regular_season, date=date_to_store)

            pull_up_shoot_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "PullUpShot", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(pull_up_shoot_player, date_to_store, db_storage, game_player_map), "sportvu_pull_up_shoot_game_logs", is_regular_season, date=date_to_store)
            pull_up_shoot_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "PullUpShot", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(pull_up_shoot_team, date_to_store, db_storage, game_team_map), "sportvu_pull_up_shoot_team_game_logs", is_regular_season, date=date_to_store)

            rebounding_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "Rebounding", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(rebounding_player, date_to_store, db_storage, game_player_map), "sportvu_rebounding_game_logs", is_regular_season, date=date_to_store)
            rebounding_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "Rebounding", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(rebounding_team, date_to_store, db_storage, game_team_map), "sportvu_rebounding_team_game_logs", is_regular_season, date=date_to_store)

            shooting_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "Efficiency", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(shooting_player, date_to_store, db_storage, game_player_map), "sportvu_shooting_game_logs", is_regular_season, date=date_to_store)
            shooting_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "Efficiency", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(shooting_team, date_to_store, db_storage, game_team_map), "sportvu_shooting_team_game_logs", is_regular_season, date=date_to_store)

            speed_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "SpeedDistance", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(speed_player, date_to_store, db_storage, game_player_map), "sportvu_speed_game_logs", is_regular_season, date=date_to_store)
            speed_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "SpeedDistance", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(speed_team, date_to_store, db_storage, game_team_map), "sportvu_speed_team_game_logs", is_regular_season, date=date_to_store)

            elbow_touches_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "ElbowTouch", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(elbow_touches_player, date_to_store, db_storage, game_player_map), "sportvu_elbow_touches_game_logs", is_regular_season, date=date_to_store)
            elbow_touches_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "ElbowTouch", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(elbow_touches_team, date_to_store, db_storage, game_team_map), "sportvu_elbow_touches_team_game_logs", is_regular_season, date=date_to_store)

            post_touches_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "PostTouch", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(post_touches_player, date_to_store, db_storage, game_player_map), "sportvu_post_touches_game_logs", is_regular_season, date=date_to_store)
            post_touches_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "PostTouch", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(post_touches_team, date_to_store, db_storage, game_team_map), "sportvu_post_touches_team_game_logs", is_regular_season, date=date_to_store)

            paint_touches_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "PaintTouch", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(paint_touches_player, date_to_store, db_storage, game_player_map), "sportvu_paint_touches_game_logs", is_regular_season, date=date_to_store)
            paint_touches_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "PaintTouch", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(paint_touches_team, date_to_store, db_storage, game_team_map), "sportvu_paint_touches_team_game_logs", is_regular_season, date=date_to_store)

            possessions_player = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Player", "Possessions", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_player(possessions_player, date_to_store, db_storage, game_player_map), "sportvu_possessions_game_logs", is_regular_season, date=date_to_store)
            possessions_team = sportvu_stats.get_sportvu_data_for_stat(season, season_type, "Team", "Possessions", start_date=date, end_date=date)
            db_storage.insert_with_date_and_season_type(sportvu_stats.add_game_id_to_game_log_for_team(possessions_team, date_to_store, db_storage, game_team_map), "sportvu_possessions_team_game_logs", is_regular_season, date=date_to_store)

            db_storage.commit()
        except:
            logging.error('sportvu not stored')
    db_storage.close()

if __name__ == '__main__':
    main()
