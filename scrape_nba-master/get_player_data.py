import json
import logging
import sys
import re

from scrape import player_stats
import storage.db

def main():
    logging.basicConfig(filename='players.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())

    season = config["season"]
    is_regular_season = config["is_regular_season"]
    # make sure season is valid format
    season_pattern = re.compile('\d{4}[-]\d{2}$')
    if season_pattern.match(season) == None:
        print "Invalid Season format. Example format: 2014-15"
        sys.exit(0)

    if is_regular_season == 0:
        season_type = "Playoffs"
        game_prefix = "004"
    elif is_regular_season == 1:
        season_type = "Regular+Season"
        game_prefix = "002"
    else:
        print "Invalid is_regular_season value. Use 0 for regular season, 1 for playoffs"

    db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])

    # get player_ids to update
    games_in_db = db_storage.query("select distinct GAME_ID from player_tracking_shot_logs where GAME_ID like '" + game_prefix +"%'")
    if len(games_in_db) > 0:
        games = []
        for game in games_in_db:
            games.append(game[0])

        games_string = ",".join(games)

        query = "select distinct PLAYER_ID, PLAYER_NAME from traditional_boxscores where GAME_ID not in (" + games_string + ") and GAME_ID like '" + game_prefix +"%'"
    else:
        query = "select distinct PLAYER_ID, PLAYER_NAME from traditional_boxscores where GAME_ID like '" + game_prefix +"%'"
    players_to_update = db_storage.query(query)
    for player in players_to_update:
        if int(player[0]) > 0 and int(player[0]) < 2147483647:
            try:
                player_id = player[0]
                player_name = player[1]
                player_data = player_stats.PlayerData(player_id, player_name, season, season_type)
                db_storage.insert(player_data.shot_logs(), "player_tracking_shot_logs")
                db_storage.insert(player_data.rebound_logs(), "player_tracking_rebound_logs")
                db_storage.insert_with_date_and_season_type(player_data.passes_made(), "player_tracking_passes_made", is_regular_season)
                db_storage.insert_with_date_and_season_type(player_data.passes_received(), "player_tracking_passes_received", is_regular_season)

                db_storage.commit()
            except:
                logging.error('player %s not stored', player_id)

    db_storage.close()


if __name__ == '__main__':
    main()
