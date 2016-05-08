import json
import logging

from process import combine_pbp_shot_logs
import storage.db

def main():
    logging.basicConfig(filename='merge_pbp_and_shot_logs.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())

    db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])

    games_query = "select distinct GAME_ID from pbp"
    games = db_storage.query(games_query)
    for game in games:
        game_id = game[0]
        pbp_query = "select * from pbp where GAME_ID = '"+game_id+"' and (EVENTMSGTYPE = 1 or EVENTMSGTYPE = 2)"
        pbp_data = db_storage.query_df(pbp_query)

        shot_log_query = "select * from player_tracking_shot_logs where GAME_ID = '"+game_id+"'"
        shot_log_data = db_storage.query_df(shot_log_query)

        players_and_periods = shot_log_data[['PERIOD', 'PLAYER_ID']]
        unique_players_and_periods = players_and_periods.drop_duplicates()

        for _, row in unique_players_and_periods.iterrows():
            try:
                shots = combine_pbp_shot_logs.combine_pbp_and_shot_logs_for_player_for_period(shot_log_data, pbp_data, row['PLAYER_ID'], row['PERIOD'], game_id)
                db_storage.insert(shots, "player_tracking_shot_logs")
                db_storage.commit()
            except:
                logging.error('game %s, period %s, player %s not processed', game_id, row['PERIOD'], row['PLAYER_ID'])

    db_storage.close()


if __name__ == '__main__':
    main()
