# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 20:03:33 2015

* Get Depth Charts from rotoworld
* get boxscore tables
* Get sportvu tables
* Get dk salaries

@author: Lu Zhou
"""
import json
import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import scrape.helper as helper

import storage.db
import MySQLdb



config=json.loads(open('config.json').read())
db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])


db = MySQLdb.connect(host=config['host'], # your host, usually localhost
                     user=config['username'], # your username
                      passwd=config['password'], # your password
                      db=config['database']) # name of the data base
cur = db.cursor() 

print "Ok"

df_traditional = pd.read_sql("SELECT * FROM traditional_boxscore", db)
df_traditional.to_csv("csv2/traditional_boxscore.csv")




def scrape_boxscores ():

    df_traditional = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM traditional_boxscores", db))
    df_traditional.to_csv("csv/traditional_boxscores.csv")
    df_traditional = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM traditional_boxscores_q1", db))
    df_traditional.to_csv("csv/traditional_boxscores_q1.csv")
    df_traditional = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM traditional_boxscores_q2", db))
    df_traditional.to_csv("csv/traditional_boxscores_q2.csv")
    df_traditional = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM traditional_boxscores_q3", db))
    df_traditional.to_csv("csv/traditional_boxscores_q3.csv")
    df_traditional = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM traditional_boxscores_q4", db))
    df_traditional.to_csv("csv/traditional_boxscores_q4.csv")    
    df_advanced = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM advanced_boxscores", db))
    df_advanced.to_csv("csv/advanced_boxscores.csv")
    df_misc = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM misc_boxscores", db))
    df_misc.to_csv("csv/misc_boxscores.csv")
    df_scoring = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM scoring_boxscores", db))
    df_scoring.to_csv("csv/scoring_boxscores.csv")
    df_usage = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM usage_boxscores", db))
    df_usage.to_csv("csv/usage_boxscores.csv")
    df_scoring = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM scoring_boxscores", db))
    df_scoring.to_csv("csv/scoring_boxscores.csv")
    df_four_factors = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM four_factors_boxscores", db))
    df_four_factors.to_csv("csv/four_factors_boxscores.csv")
    df_player_tracking = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM player_tracking_boxscores", db))
    df_player_tracking.to_csv("csv/player_tracking_boxscores.csv")
    
    df_traditional_team = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM traditional_boxscores_team", db))
    df_traditional_team.to_csv("csv/traditional_boxscores_team.csv")
    df_advanced_team = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM advanced_boxscores_team", db))
    df_advanced_team.to_csv("csv/advanced_boxscores_team.csv")
    df_misc_team = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM misc_boxscores_team", db))
    df_misc_team.to_csv("csv/misc_boxscores_team.csv")
    df_scoring_team = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM scoring_boxscores_team", db))
    df_scoring_team.to_csv("csv/scoring_boxscores_team.csv")
    #df_usage_team = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM usage_boxscores_team", db))
    #df_usage_team.to_csv("csv/usage_boxscores_team.csv")
    df_four_factors_team = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM four_factors_boxscores_team", db))
    df_four_factors_team.to_csv("csv/four_factors_boxscores_team.csv")
    df_player_tracking_team = helper.boxscore_fix_min(pd.read_sql("SELECT * FROM player_tracking_boxscores_team", db))
    df_player_tracking_team.to_csv("csv/player_tracking_boxscores_team.csv")

    #shot logs
    df_shot_log = pd.read_sql("SELECT * FROM player_tracking_shot_logs", db)
    df_shot_log.to_csv("csv/shot_logs.csv")


    #--------------------------------------------------------------------------


def scrape_sportvu_logs():
    df_pass = pd.read_sql("SELECT * FROM nba.sportvu_passing_game_logs", db)
    df_pass.to_csv("csv/passes_log.csv")    
    df_touches = pd.read_sql("SELECT * FROM nba.sportvu_possessions_game_logs", db)
    df_touches.to_csv("csv/touches_log.csv")
    df_touches = pd.read_sql("SELECT * FROM nba.sportvu_post_touches_game_logs", db)
    df_touches.to_csv("csv/post_logs.csv")
    df_touches = pd.read_sql("SELECT * FROM nba.sportvu_paint_touches_game_logs", db)
    df_touches.to_csv("csv/paint_logs.csv")    

def scrape_depth_charts():
    soup = BeautifulSoup(requests.get("http://www.rotoworld.com/teams/depth-charts/nba.aspx").text, "html5lib")
    table = soup.find('table', 'depthtable').tbody
    rows = table.findAll('tr', recursive=False)  #all teams
    depth_charts = []
    
    i=0
    for row in rows:
        if i % 2 == 0:
            ths = row.findAll("th")
            team_name = []
            for th in ths:
                team_name.append(th.a.text)
        else:
            tds = row.findAll("td", recursive=False)
            k=0
            for td in tds:
                team = td.table.tbody.findAll("tr")
                position = ""
                rank = 0;
                for player in team:
                    if player.td.text <> "":
                        position = player.td.text
                    player_position =  position
                    player_name = player.find("a").text
                    if "J.R. Smith" in player_name or "O.J. Mayo" in player_name or "D.J. Augustin" in player_name or "JJ Redick" in player_name:
                        pass
                    elif "J.J. Barea" in player_name:
                        player_name = "Jose Juan Barea"
                    elif "Nene Hilario" in player_name:
                        player_name = "Nene"                        
                    else:
                        player_name = player_name.replace(".", "")
                    player_report = player.find("div", "report");
                    if player_report is not None:
                        player_report = player.find("div", "report").text.replace('"', '')
                    player_impact = player.find("div", "impact");
                    if player_impact is not None:
                        player_impact = player.find("div", "impact").text.replace('"', '')
                    depth_charts.append([team_name[k], player_position, rank, player_name, player_report, player_impact])
                    rank = rank + 1
                k += 1
        i += 1    
    
    
    df = pd.DataFrame(depth_charts, columns=['Team', 'Position', 'Rank', 'PLAYER_NAME', 'Report', 'Impact'])

    
    headers = ['TEAM', 'POSITION', 'RANK', 'PLAYER_NAME', 'REPORT', 'IMPACT']
    rows = depth_charts
    
    
    
    depth_charts_db = [dict(zip(headers, row)) for row in rows]
    db_storage.insert(depth_charts_db, "depth_chart")
    db_storage.commit()
    db_storage.close()
#    df.to_csv("csv/depth_charts.csv", encoding='utf-8')
    
def scrape_salaries():
    
    soup = BeautifulSoup(requests.get("https://rotogrinders.com/projected-stats/nba?site=draftkings").text, "html5lib")
    table = soup.find('table')
    rows = table.findAll("tr")
    
    player_rotogrinders = []

    
    for tr in rows[1:]:
        tds = tr.findAll('td')
        name = tds[0].a.text
        salary = int(float(tds[3].get("data-sort"))*1000)
        projection = tds[4].get("data-sort")
        player_rotogrinders.append( [name, salary, projection])
        
    df = pd.DataFrame(player_rotogrinders, columns=['Name', 'Price', 'Projection'])
    df.to_csv("csv/salaries.csv", encoding='utf-8')

#scrape_depth_charts()
#scrape_boxscores()
#scrape_sportvu_logs()
#scrape_salaries()

