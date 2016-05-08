import pandas as pd

def combine_pbp_and_shot_logs_for_player_for_period(shot_log_data, pbp_data, player_id, period, game_id):
    # return list of dicts containing player_id, game_id, shot_number, eventnum
    shot_log_event_num = []
    pbp_player_period = pbp_data[(pbp_data['PERIOD'] == period) & (pbp_data['PLAYER1_ID'] == player_id)]
    shot_logs_player_period = shot_log_data[(shot_log_data['PERIOD'] == period) & (shot_log_data['PLAYER_ID'] == player_id)]

    pbp_split = pbp_player_period['PCTIMESTRING'].str.split(":")
    pbp_player_period['seconds'] = pbp_split.map(lambda x: int(x[0])*60 + int(x[1]))

    shot_logs_split = shot_logs_player_period['GAME_CLOCK'].str.split(":")
    shot_logs_player_period['seconds'] = shot_logs_split.map(lambda x: int(x[0])*60 + int(x[1]))

    pbp_player_period = pbp_player_period.sort('seconds', ascending=False)
    shot_logs_player_period = shot_logs_player_period.sort('seconds', ascending=False)

    pbp_player_period = pbp_player_period.reset_index(drop=True)
    shot_logs_player_period = shot_logs_player_period.reset_index(drop=True)

    # sometimes pbp has an extra shot, find it and remove it
    if len(pbp_player_period.index) == len(shot_logs_player_period.index) + 1:
        for i, row in pbp_player_period.iterrows():
            if i > len(shot_logs_player_period.index)-1:
                pbp_player_period = pbp_player_period.drop(pbp_player_period.index[i])
                pbp_player_period = pbp_player_period.reset_index(drop=True)
                break
            elif abs(row['seconds'] - shot_logs_player_period['seconds'].iloc[i]) > 5:
                pbp_player_period = pbp_player_period.drop(pbp_player_period.index[i])
                pbp_player_period = pbp_player_period.reset_index(drop=True)
                break

    # keep shots where times are within 5 seconds between datasets and shot number for period is equal
    if len(pbp_player_period.index) == len(shot_logs_player_period.index):
        for i, row in shot_logs_player_period.iterrows():
            if abs(row['seconds'] - pbp_player_period['seconds'].iloc[i]) <= 5:
                shot_log_event_num.append({"GAME_ID": game_id, "PLAYER_ID": player_id, "SHOT_NUMBER": row['SHOT_NUMBER'], "PBP_EVENTNUM": pbp_player_period['EVENTNUM'].iloc[i]})
    else:
        # when number of shots is different in both datasets, find shots within 5 seconds, if there is only 1, keep it
        for i, row in shot_logs_player_period.iterrows():
            possible_matches = pbp_player_period[abs(row['seconds'] - pbp_player_period['seconds']) <= 5]
            if len(possible_matches.index) == 1 and len(shot_logs_player_period[abs(row['seconds'] - shot_logs_player_period['seconds']) <= 5]) == 1:
                shot_log_event_num.append({"GAME_ID": game_id, "PLAYER_ID": player_id, "SHOT_NUMBER": row['SHOT_NUMBER'], "PBP_EVENTNUM": possible_matches['EVENTNUM'].iloc[0]})

    return shot_log_event_num
