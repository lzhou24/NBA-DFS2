# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 09:34:46 2015

@author: Lu Zhou
"""

import pandas as pd
import json

#box scores
traditional_boxscores = pd.read_csv("csv/traditional_boxscores.csv")
traditional_boxscores_q1 = pd.read_csv("csv/traditional_boxscores_q1.csv")
traditional_boxscores_q1.rename(columns = {'MIN':'MIN_Q1'}, inplace = True)
traditional_boxscores_q2 = pd.read_csv("csv/traditional_boxscores_q2.csv")
traditional_boxscores_q2.rename(columns = {'MIN':'MIN_Q2'}, inplace = True)
traditional_boxscores_q3 = pd.read_csv("csv/traditional_boxscores_q3.csv")
traditional_boxscores_q3.rename(columns = {'MIN':'MIN_Q3'}, inplace = True)
traditional_boxscores_q4 = pd.read_csv("csv/traditional_boxscores_q4.csv")
traditional_boxscores_q4.rename(columns = {'MIN':'MIN_Q4'}, inplace = True)
advanced_boxscores = pd.read_csv("csv/advanced_boxscores.csv")
misc_boxscores = pd.read_csv("csv/misc_boxscores.csv")
scoring_boxscores = pd.read_csv("csv/scoring_boxscores.csv")
usage_boxscores = pd.read_csv("csv/usage_boxscores.csv")
four_factors_boxscores = pd.read_csv("csv/four_factors_boxscores.csv")
player_tracking_boxscores = pd.read_csv("csv/player_tracking_boxscores.csv")

# take traditional_boxscores as a base
traditional_q1 = traditional_boxscores_q1[['GAME_ID', 'PLAYER_ID','MIN_Q1']]
traditional_q2 = traditional_boxscores_q2[['GAME_ID', 'PLAYER_ID','MIN_Q2']]
traditional_q3 = traditional_boxscores_q3[['GAME_ID', 'PLAYER_ID','MIN_Q3']]
traditional_q4 = traditional_boxscores_q4[['GAME_ID', 'PLAYER_ID','MIN_Q4']]
advanced = advanced_boxscores[['GAME_ID', 'PLAYER_ID', 'AST_PCT', 'REB_PCT', 'PACE', 'USG_PCT', 'OFF_RATING', 'DEF_RATING', 'PIE']]
misc = misc_boxscores[['GAME_ID', 'PLAYER_ID', 'PTS_FB', 'PFD']]
usage = usage_boxscores[['GAME_ID', 'PLAYER_ID', 'PCT_PTS']]
tracking = player_tracking_boxscores[['GAME_ID', 'PLAYER_ID', 'RBC', 'CFGM', 'UFGM']]

#tracking
passing_log = pd.read_csv("csv/passes_log.csv")
passing = passing_log[['GAME_ID', 'PLAYER_ID', 'POTENTIAL_AST']]
touches_log = pd.read_csv("csv/touches_log.csv")
touches = touches_log[['GAME_ID', 'PLAYER_ID', 'FRONT_CT_TOUCHES']]
paint_log = pd.read_csv("csv/paint_logs.csv")
paint = paint_log[['GAME_ID', 'PLAYER_ID', 'PAINT_TOUCHES', 'PAINT_TOUCH_PTS']]
post_log = pd.read_csv("csv/post_logs.csv")
post = post_log[['GAME_ID', 'PLAYER_ID', 'POST_TOUCHES', 'POST_TOUCH_PTS']]

traditional_boxscores = traditional_boxscores.merge(traditional_q1, on=['GAME_ID', 'PLAYER_ID'], how='left').merge(traditional_q2, on=['GAME_ID', 'PLAYER_ID'], how='left').merge(traditional_q3, on=['GAME_ID', 'PLAYER_ID'], how='left').merge(traditional_q4, on=['GAME_ID', 'PLAYER_ID'], how='left')
boxscore_full = traditional_boxscores.merge(advanced, on=['GAME_ID', 'PLAYER_ID'], how='left').merge(misc, on=['GAME_ID', 'PLAYER_ID'], how='left').merge(usage, on=['GAME_ID', 'PLAYER_ID'], how='left').merge(tracking, on=['GAME_ID', 'PLAYER_ID'], how='left')
boxscore_full = pd.merge(boxscore_full, passing, on=['GAME_ID', 'PLAYER_ID'], how='left' )
boxscore_full = pd.merge(boxscore_full, touches, on=['GAME_ID', 'PLAYER_ID'], how='left' )
boxscore_full = pd.merge(boxscore_full, post, on=['GAME_ID', 'PLAYER_ID'], how='left' )
boxscore_full = pd.merge(boxscore_full, paint, on=['GAME_ID', 'PLAYER_ID'], how='left' )
boxscore_full.to_csv("csv/boxscore_full.csv")


#salaries and depth charts data
df_salaries = pd.read_csv("csv/salaries.csv")
df_depthcharts = pd.read_csv("csv/depth_charts.csv")

df_depthcharts = pd.merge(df_depthcharts, df_salaries, left_on = 'PLAYER_NAME', right_on = 'Name', how="left" )
depthcharts_boxscore = pd.merge(df_depthcharts, boxscore_full, on='PLAYER_NAME', how="left")
depthcharts_boxscore.to_csv("csv/depthcharts_boxscore.csv")



#Team Stats
traditional_boxscores_team = pd.read_csv("csv/traditional_boxscores_team.csv")
advanced_boxscores_team = pd.read_csv("csv/advanced_boxscores_team.csv")
misc_boxscores_team = pd.read_csv("csv/misc_boxscores_team.csv")
scoring_boxscores_team = pd.read_csv("csv/scoring_boxscores_team.csv")
four_factors_boxscores_team = pd.read_csv("csv/four_factors_boxscores_team.csv")
player_tracking_boxscores_team = pd.read_csv("csv/player_tracking_boxscores_team.csv")


advanced_team = advanced_boxscores_team[['GAME_ID', 'TEAM_ID', 'PACE', 'OFF_RATING', 'DEF_RATING']]
boxscore_full_team = traditional_boxscores_team.merge(advanced_team, on=['GAME_ID', 'TEAM_ID'], how='left')
boxscore_full_team[['GAME_ID', 'TEAM_ID']] = boxscore_full_team[['GAME_ID', 'TEAM_ID']].astype(float)
boxscore_full_team.to_csv("csv/boxscore_full_team.csv")
