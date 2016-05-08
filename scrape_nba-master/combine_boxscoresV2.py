# -*- coding: utf-8 -*-
"""
Created on Sat May 07 21:12:57 2016

@author: Lu Zhou
"""

import json
import csv
import pandas as pd
import scrape.helper as helper
import storage.db

config=json.loads(open('config.json').read())
db_storage = storage.db.Storage(config['host'], config['username'], config['password'], config['database'])

