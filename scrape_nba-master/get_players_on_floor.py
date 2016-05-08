import json
import logging

from process import pbp
import storage.db

def main():
    logging.basicConfig(filename='process_pbp.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    config=json.loads(open('config.json').read())

    db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])

    # get game_ids to process
    missing_games_query = "select distinct GAME_ID from pbp where HOME_PLAYER1 is NULL"
    games_to_process = db_storage.query(missing_games_query)

    # process games
    for game in games_to_process:
        game_id = game[0]
        try:
            query = "select * from pbp where GAME_ID = '"+game_id+"'"
            pbp_data = db_storage.query_df(query)

            game_data = pbp.Lineups(pbp_data)
            pbp_with_lineups =  game_data.get_players_on_floor_for_game()
            db_storage.insert(pbp_with_lineups, "pbp")

            db_storage.commit()
        except:
            logging.error('game %s not processed', game_id)

    db_storage.close()


if __name__ == '__main__':
    main()
