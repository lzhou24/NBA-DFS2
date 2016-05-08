# -*- coding: utf-8 -*-
"""
Created on Sun Nov 08 15:55:20 2015

@author: Lu Zhou
"""

import storage.db
import json

"""Goal is to make the export process easier
 Get all games by date in the database
     1) Don't have to import everyday
     2) Note the back to back games

"""
config=json.loads(open('config.json').read())
db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])

cursor = db_storage.curs()
df_pass = pd.read_sql("SELECT * FROM nba.sportvu_passing_game_logs", cursor)

# Boxscore summary

