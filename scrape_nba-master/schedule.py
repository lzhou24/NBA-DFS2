# -*- coding: utf-8 -*-

from dateutil.rrule import rrule, DAILY
import sys
import datetime
import json
import re
import scrape.helper
import storage.db
from scrape import game_stats

start_date = datetime.date(2015, 11, 2)
end_date = datetime.date(2015, 11, 2)

dates = []
schedule = []

config=json.loads(open('config.json').read())
db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])
cursor = db_storage.curs()

for dt in rrule(DAILY, dtstart=start_date, until=end_date):
    print dt
    teams = scrape.helper.get_game_schedule_for_date(dt.strftime("%Y-%m-%d"))
    for team in teams:
        dates.append(dt)
        schedule.append(teams)
#        query = 'Insert into schedule (DATE, TEAM_ABBREVIATION) VALUES(\''+str(dt)+'\', \''+team+'\')'
#        cursor.execute(query)

#db_storage.commit()
#db_storage.close()

    
