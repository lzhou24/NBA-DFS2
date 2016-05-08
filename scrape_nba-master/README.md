# stats.nba.com scraping library

* Clone the repository

```
cd path_to_folder
git clone https://github.com/dblackrun/scrape_nba.git
```

* Copy sample_config.json to config.json

```
cd scrape_nba
cp sample_config.json config.json
```

* Enter MySQL database credentials, table name in config.json
* To change the season or season type you are scraping just change the season and is_regular_season(1 for regular season, 0 for playoffs) values in config.json

* Create database and tables
```
python setup.py
```

* Get games for date range (start date first)
```
python get_games_for_date_range.py YYYY-MM-DD YYYY-MM-DD
```

* Get player data for games in db
```
python get_player_data.py
```

* Get sportvu data - can be run daily to get daily snapshot
```
python get_sportvu.py
```

* Get synergy data - can be run daily to get daily snapshot
```
python get_synergy.py
```

* Get players on floor for pbp in database
```
python get_players_on_floor.py
```
