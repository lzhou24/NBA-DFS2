# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 14:08:25 2015

@author: Lu Zhou
"""
import json
import storage.db
import sys
import pandas as pd
import re
import MySQLdb

config=json.loads(open('config.json').read())

db = MySQLdb.connect(host=config['host'], # your host, usually localhost
                     user=config['username'], # your username
                      passwd=config['password'], # your password
                      db=config['database']) # name of the data base

def get_minutes(game_id):
    
    df_starters = pd.read_sql("SELECT * FROM traditional_boxscores Where GAME_ID = '00"+game_id+"'", db)
    print "SELECT * FROM traditional_boxscores Where GAME_ID = '00"+game_id+"'"
    #initialize get starters minutes
    players_minutes = []
    for index, row in df_starters.iterrows():
        game_id = row['GAME_ID']
        team = row['TEAM_ABBREVIATION']
        name = row['PLAYER_NAME']
        if len(str(row['START_POSITION'])) > 0:
            player_minutes = [game_id, team, name, 1, 0, 1]
        else:
            player_minutes = [game_id, team, name, 1, 0, 0]
        players_minutes.append(player_minutes)
    
    df_initial = pd.DataFrame(players_minutes, columns=["GAME_ID", "TEAM", "PLAYER_NAME", "QUARTER","TIME", "ON_COURT"])
    with open("csv/minutes.csv", 'a') as f:
        df_initial.to_csv(f, header=False)
    
    df_pbp = pd.read_sql("SELECT * FROM nba.pbp Where GAME_ID = '"+game_id+"'", db)
    minutes = []
    for index, row in df_pbp.iterrows():
        if row['EVENTMSGTYPE'] == 8:
            game_id = row['GAME_ID']
            period = row['PERIOD']
            team = row['PLAYER1_TEAM_ABBREVIATION']
            game_time = 720 - (int(row['PCTIMESTRING'].split(":")[0])*60 + int(row['PCTIMESTRING'].split(":")[1])) + int(((int(period)-1)*720))
            player_off = row['PLAYER1_NAME']  #subbing off
            player_on = row['PLAYER2_NAME']   #subbing on  
            
            for row in reversed(minutes):
                if row[2] == player_on:
                    if row[5] == 1:
                        #error - player is already on the court.  
                        game_time_fix = game_time / 720 * 720
                        minutes.append([game_id, team, player_on, period, game_time_fix-1, 1])
                        minutes.append([game_id, team, player_on, period, game_time_fix, 0])
                    break
            for row in reversed(minutes):
                if row[2] == player_off:
                    if row[5] == 0:
                        #error - player is already off the court     
                        game_time_fix = game_time / 720 * 720
                        minutes.append([game_id, team, player_off, period, game_time_fix-1, 0])
                        minutes.append([game_id, team, player_off, period, game_time_fix, 1])
                    break
            #player1 is coming on to the court
            player1_off = [game_id, team, player_on, period, game_time-1, 0]        
            player1_on = [game_id, team, player_on, period, game_time, 1]
            
            #player2 is coming off the court
            player2_on = [game_id, team, player_off, period, game_time-1, 1]
            player2_off = [game_id, team, player_off, period, game_time, 0]
    
            minutes.append( player1_off )
            minutes.append( player1_on )
            minutes.append( player2_on )
            minutes.append( player2_off )
    
    minutes_df = pd.DataFrame(minutes, columns=["GAME_ID", "TEAM", "PLAYER_NAME", "QUARTER","TIME", "FOUL_NUMBER"])
    with open("csv/minutes.csv", 'a') as f:
        minutes_df.to_csv(f, header=False)

    

for index in range(90):
    
    #read
    get_minutes(str(int(index)+1+21500288))

    
