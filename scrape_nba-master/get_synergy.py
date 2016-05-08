import json
import logging
import sys
import re

from scrape import synergy_stats
import storage.db

def main():
    logging.basicConfig(filename='synergy.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
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
        season_type = "_post"
    elif is_regular_season == 1:
        season_type = ""
    else:
        print "Invalid is_regular_season value. Use 0 for regular season, 1 for playoffs"

    db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])
    try:
        synergy_data = synergy_stats.SynergyData(season_type)
        
        
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_team_offense(), "synergy_isolation_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_team_defense(), "synergy_isolation_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_offense(), "synergy_isolation_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_defense(), "synergy_isolation_defense", is_regular_season)
        
        
        """
        db_storage.insert_with_date_and_season_type(synergy_data.transition_team_offense(), "synergy_transition_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.transition_team_defense(), "synergy_transition_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.transition_offense(), "synergy_transition_offense", is_regular_season)
        
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_team_offense(), "synergy_isolation_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_team_defense(), "synergy_isolation_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_offense(), "synergy_isolation_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.isolation_defense(), "synergy_isolation_defense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.pr_ball_handler_team_offense(), "synergy_pr_ball_handler_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.pr_ball_handler_team_defense(), "synergy_pr_ball_handler_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.pr_ball_handler_offense(), "synergy_pr_ball_handler_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.pr_ball_handler_defense(), "synergy_pr_ball_handler_defense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.pr_roll_man_team_offense(), "synergy_pr_roll_man_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.pr_roll_man_team_defense(), "synergy_pr_roll_man_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.pr_roll_man_offense(), "synergy_pr_roll_man_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.pr_roll_man_defense(), "synergy_pr_roll_man_defense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.post_up_team_offense(), "synergy_post_up_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.post_up_team_defense(), "synergy_post_up_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.post_up_offense(), "synergy_post_up_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.post_up_defense(), "synergy_post_up_defense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.spot_up_team_offense(), "synergy_spot_up_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.spot_up_team_defense(), "synergy_spot_up_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.spot_up_offense(), "synergy_spot_up_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.spot_up_defense(), "synergy_spot_up_defense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.handoff_team_offense(), "synergy_handoff_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.handoff_team_defense(), "synergy_handoff_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.handoff_offense(), "synergy_handoff_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.handoff_defense(), "synergy_handoff_defense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.cut_team_offense(), "synergy_cut_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.cut_team_defense(), "synergy_cut_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.cut_offense(), "synergy_cut_offense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.off_screen_team_offense(), "synergy_off_screen_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.off_screen_team_defense(), "synergy_off_screen_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.off_screen_offense(), "synergy_off_screen_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.off_screen_defense(), "synergy_off_screen_defense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.put_back_team_offense(), "synergy_put_back_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.put_back_team_defense(), "synergy_put_back_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.put_back_offense(), "synergy_put_back_offense", is_regular_season)

        db_storage.insert_with_date_and_season_type(synergy_data.misc_team_offense(), "synergy_misc_team_offense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.misc_team_defense(), "synergy_misc_team_defense", is_regular_season)
        db_storage.insert_with_date_and_season_type(synergy_data.misc_offense(), "synergy_misc_offense", is_regular_season)
        """
        db_storage.commit()
    except:
        logging.error('synergy not stored')
    db_storage.close()

if __name__ == '__main__':
    main()
