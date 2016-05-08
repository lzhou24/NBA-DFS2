import json
import storage.db

config=open('config.json').read()
db_config = json.loads(config)

db = storage.db.Db(db_config['host'], db_config['username'], db_config['password'], db_config['database'])
db.create_db()
db.create_tables()
