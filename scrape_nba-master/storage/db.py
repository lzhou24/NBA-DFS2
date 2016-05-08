import MySQLdb
import time
import pandas.io.sql as sql

class Storage:
    def __init__(self, host, user, pw, db):
        self._host=host
        self._user=user
        self._pw=pw
        self._db=db
        self.conn = MySQLdb.connect(host, user, pw, db)
        self.conn.set_character_set('utf8')

    def probe(self, table_name):
        query = "Select MAX(GAME_ID) From "+ table_name
        return self.query(query)
        

    def insert(self, data, table_name):
        for line in data:
            headers = [key for key,val in sorted(line.items())]
            quoted_values = ['"%s"' % (val) for key,val in sorted(line.items())]
            duplicate_key_clauses = ['`%s`="%s"' % (key,val) for key,val in sorted(line.items())]
            for i in range(len(headers)):
                headers[i] = "`" + headers[i] + "`"

            self.query("""
                INSERT INTO %s
                (%s)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE
                %s
            """ % (table_name, ','.join(headers), ','.join(quoted_values),','.join(duplicate_key_clauses)))


    def insert_with_date_and_season_type(self, data, table_name, is_regular_season, date=time.strftime("%Y-%m-%d")):
        
        for line in data:
            line['DATE'] = date
            
            line['IS_REGULAR_SEASON'] = is_regular_season
            headers = [key for key,val in sorted(line.items())]
            quoted_values = ['"%s"' % (val) for key,val in sorted(line.items())]
            duplicate_key_clauses = ['`%s`="%s"' % (key,val) for key,val in sorted(line.items())]

            query = """
                INSERT INTO %s
                (%s)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE
                %s
            """ % (table_name, ','.join(headers), ','.join(quoted_values),','.join(duplicate_key_clauses))

            
            self.query("""
                INSERT INTO %s
                (%s)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE
                %s
            """ % (table_name, ','.join(headers), ','.join(quoted_values),','.join(duplicate_key_clauses)))
    
    def truncate(self, table_name):
        self.query(""" Truncate TABLE %s """ % (table_name))
        
        
    
    def close(self):
        return self.conn.close()

    def query(self, sql_query):
        curs = self.curs()
        curs.execute(sql_query)

        return curs.fetchall()

    def query_df(self, sql_query):
        return sql.read_frame(sql_query, self.conn)

    def curs(self):
        return self.conn.cursor()

    def commit(self):
        return self.conn.commit()

class Db:
    def __init__(self, host, user, pw, db):
        self._host=host
        self._user=user
        self._pw=pw
        self._db=db

    def create_db(self):
        conn = MySQLdb.connect(host=self._host, user=self._user, passwd=self._pw)
        cursor = conn.cursor()
        query = 'CREATE DATABASE IF NOT EXISTS ' + self._db
        cursor.execute(query)
        conn.close()

    def create_tables(self):
        conn = MySQLdb.connect(host=self._host, user=self._user, passwd=self._pw, db=self._db)
        cursor = conn.cursor()
        pbp_query = 'CREATE TABLE IF NOT EXISTS pbp\
        (\
        GAME_ID VARCHAR(255),\
        EVENTNUM INT,\
        EVENTMSGTYPE INT,\
        EVENTMSGACTIONTYPE INT,\
        PERIOD INT,\
        WCTIMESTRING VARCHAR(255),\
        PCTIMESTRING VARCHAR(255),\
        HOMEDESCRIPTION VARCHAR(255),\
        NEUTRALDESCRIPTION VARCHAR(255),\
        VISITORDESCRIPTION VARCHAR(255),\
        SCORE VARCHAR(255),\
        SCOREMARGIN VARCHAR(255),\
        PERSON1TYPE VARCHAR(255),\
        PLAYER1_ID VARCHAR(255),\
        PLAYER1_NAME VARCHAR(255),\
        PLAYER1_TEAM_ID VARCHAR(255),\
        PLAYER1_TEAM_CITY VARCHAR(255),\
        PLAYER1_TEAM_NICKNAME VARCHAR(255),\
        PLAYER1_TEAM_ABBREVIATION VARCHAR(255),\
        PERSON2TYPE VARCHAR(255),\
        PLAYER2_ID VARCHAR(255),\
        PLAYER2_NAME VARCHAR(255),\
        PLAYER2_TEAM_ID VARCHAR(255),\
        PLAYER2_TEAM_CITY VARCHAR(255),\
        PLAYER2_TEAM_NICKNAME VARCHAR(255),\
        PLAYER2_TEAM_ABBREVIATION VARCHAR(255),\
        PERSON3TYPE VARCHAR(255),\
        PLAYER3_ID VARCHAR(255),\
        PLAYER3_NAME VARCHAR(255),\
        PLAYER3_TEAM_ID VARCHAR(255),\
        PLAYER3_TEAM_CITY VARCHAR(255),\
        PLAYER3_TEAM_NICKNAME VARCHAR(255),\
        PLAYER3_TEAM_ABBREVIATION VARCHAR(255),\
        HOME_PLAYER1 VARCHAR(255),\
        HOME_PLAYER2 VARCHAR(255),\
        HOME_PLAYER3 VARCHAR(255),\
        HOME_PLAYER4 VARCHAR(255),\
        HOME_PLAYER5 VARCHAR(255),\
        VISITOR_PLAYER1 VARCHAR(255),\
        VISITOR_PLAYER2 VARCHAR(255),\
        VISITOR_PLAYER3 VARCHAR(255),\
        VISITOR_PLAYER4 VARCHAR(255),\
        VISITOR_PLAYER5 VARCHAR(255),\
        PRIMARY KEY(GAME_ID, EVENTNUM)\
        );'
        cursor.execute(pbp_query)

        shots_query = 'CREATE TABLE IF NOT EXISTS shots\
        (\
        GRID_TYPE VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GAME_EVENT_ID INT,\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        PERIOD INT,\
        MINUTES_REMAINING INT,\
        SECONDS_REMAINING INT,\
        EVENT_TYPE VARCHAR(255),\
        ACTION_TYPE VARCHAR(255),\
        SHOT_TYPE VARCHAR(255),\
        SHOT_ZONE_BASIC VARCHAR(255),\
        SHOT_ZONE_AREA VARCHAR(255),\
        SHOT_ZONE_RANGE VARCHAR(255),\
        SHOT_DISTANCE INT,\
        LOC_X INT,\
        LOC_Y INT,\
        SHOT_ATTEMPTED_FLAG BOOLEAN,\
        SHOT_MADE_FLAG BOOLEAN,\
        PRIMARY KEY(GAME_ID, GAME_EVENT_ID)\
        );'
        cursor.execute(shots_query)

        advanced_boxscores_query = 'CREATE TABLE IF NOT EXISTS advanced_boxscores\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        OFF_RATING DOUBLE,\
        DEF_RATING DOUBLE,\
        NET_RATING DOUBLE,\
        AST_PCT DOUBLE,\
        AST_TOV DOUBLE,\
        AST_RATIO DOUBLE,\
        OREB_PCT DOUBLE,\
        DREB_PCT DOUBLE,\
        REB_PCT DOUBLE,\
        TM_TOV_PCT DOUBLE,\
        EFG_PCT DOUBLE,\
        TS_PCT DOUBLE,\
        USG_PCT DOUBLE,\
        PACE DOUBLE,\
        PIE DOUBLE,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(advanced_boxscores_query)

        advanced_boxscores_team_query = 'CREATE TABLE IF NOT EXISTS advanced_boxscores_team\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        MIN VARCHAR(255),\
        OFF_RATING DOUBLE,\
        DEF_RATING DOUBLE,\
        NET_RATING DOUBLE,\
        AST_PCT DOUBLE,\
        AST_TOV DOUBLE,\
        AST_RATIO DOUBLE,\
        OREB_PCT DOUBLE,\
        DREB_PCT DOUBLE,\
        REB_PCT DOUBLE,\
        TM_TOV_PCT DOUBLE,\
        EFG_PCT DOUBLE,\
        TS_PCT DOUBLE,\
        USG_PCT DOUBLE,\
        PACE DOUBLE,\
        PIE DOUBLE,\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(advanced_boxscores_team_query)

        traditional_boxscores_query = 'CREATE TABLE IF NOT EXISTS traditional_boxscores\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        FTM INT,\
        FTA INT,\
        FT_PCT DOUBLE,\
        OREB INT,\
        DREB INT,\
        REB INT,\
        AST INT,\
        STL INT,\
        BLK INT,\
        `TO` INT,\
        PF INT,\
        PTS INT,\
        PLUS_MINUS INT,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(traditional_boxscores_query)
        
        traditional_boxscores_query_Q1 = 'CREATE TABLE IF NOT EXISTS traditional_boxscores_Q1\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        FTM INT,\
        FTA INT,\
        FT_PCT DOUBLE,\
        OREB INT,\
        DREB INT,\
        REB INT,\
        AST INT,\
        STL INT,\
        BLK INT,\
        `TO` INT,\
        PF INT,\
        PTS INT,\
        PLUS_MINUS INT,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(traditional_boxscores_query_Q1)

        traditional_boxscores_query_Q2 = 'CREATE TABLE IF NOT EXISTS traditional_boxscores_Q2\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        FTM INT,\
        FTA INT,\
        FT_PCT DOUBLE,\
        OREB INT,\
        DREB INT,\
        REB INT,\
        AST INT,\
        STL INT,\
        BLK INT,\
        `TO` INT,\
        PF INT,\
        PTS INT,\
        PLUS_MINUS INT,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(traditional_boxscores_query_Q2)
        
        traditional_boxscores_query_Q3 = 'CREATE TABLE IF NOT EXISTS traditional_boxscores_Q3\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        FTM INT,\
        FTA INT,\
        FT_PCT DOUBLE,\
        OREB INT,\
        DREB INT,\
        REB INT,\
        AST INT,\
        STL INT,\
        BLK INT,\
        `TO` INT,\
        PF INT,\
        PTS INT,\
        PLUS_MINUS INT,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(traditional_boxscores_query_Q3)

        traditional_boxscores_query_Q4 = 'CREATE TABLE IF NOT EXISTS traditional_boxscores_Q4\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        FTM INT,\
        FTA INT,\
        FT_PCT DOUBLE,\
        OREB INT,\
        DREB INT,\
        REB INT,\
        AST INT,\
        STL INT,\
        BLK INT,\
        `TO` INT,\
        PF INT,\
        PTS INT,\
        PLUS_MINUS INT,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(traditional_boxscores_query_Q4)        

        traditional_boxscores_team_query = 'CREATE TABLE IF NOT EXISTS traditional_boxscores_team\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        MIN VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        FTM INT,\
        FTA INT,\
        FT_PCT DOUBLE,\
        OREB INT,\
        DREB INT,\
        REB INT,\
        AST INT,\
        STL INT,\
        BLK INT,\
        `TO` INT,\
        PF INT,\
        PTS INT,\
        PLUS_MINUS INT,\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(traditional_boxscores_team_query)

        player_tracking_boxscores_query = 'CREATE TABLE IF NOT EXISTS player_tracking_boxscores\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        SPD DOUBLE,\
        DIST DOUBLE,\
        ORBC INT,\
        DRBC INT,\
        RBC INT,\
        TCHS INT,\
        SAST INT,\
        FTAST INT,\
        PASS INT,\
        AST INT,\
        CFGM INT,\
        CFGA INT,\
        CFG_PCT DOUBLE,\
        UFGM INT,\
        UFGA INT,\
        UFG_PCT DOUBLE,\
        FG_PCT DOUBLE,\
        DFGM INT,\
        DFGA INT,\
        DFG_PCT DOUBLE,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(player_tracking_boxscores_query)

        player_tracking_boxscores_team_query = 'CREATE TABLE IF NOT EXISTS player_tracking_boxscores_team\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_NICKNAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        MIN VARCHAR(255),\
        SPD DOUBLE,\
        DIST DOUBLE,\
        ORBC INT,\
        DRBC INT,\
        RBC INT,\
        TCHS INT,\
        SAST INT,\
        FTAST INT,\
        PASS INT,\
        AST INT,\
        CFGM INT,\
        CFGA INT,\
        CFG_PCT DOUBLE,\
        UFGM INT,\
        UFGA INT,\
        UFG_PCT DOUBLE,\
        FG_PCT DOUBLE,\
        DFGM INT,\
        DFGA INT,\
        DFG_PCT DOUBLE,\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(player_tracking_boxscores_team_query)

        scoring_boxscores_query = 'CREATE TABLE IF NOT EXISTS scoring_boxscores\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        PCT_FGA_2PT DOUBLE,\
        PCT_FGA_3PT DOUBLE,\
        PCT_PTS_2PT DOUBLE,\
        PCT_PTS_2PT_MR DOUBLE,\
        PCT_PTS_3PT DOUBLE,\
        PCT_PTS_FB DOUBLE,\
        PCT_PTS_FT DOUBLE,\
        PCT_PTS_OFF_TOV DOUBLE,\
        PCT_PTS_PAINT DOUBLE,\
        PCT_AST_2PM DOUBLE,\
        PCT_UAST_2PM DOUBLE,\
        PCT_AST_3PM DOUBLE,\
        PCT_UAST_3PM DOUBLE,\
        PCT_AST_FGM DOUBLE,\
        PCT_UAST_FGM DOUBLE,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(scoring_boxscores_query)

        scoring_boxscores_team_query = 'CREATE TABLE IF NOT EXISTS scoring_boxscores_team\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        MIN VARCHAR(255),\
        PCT_FGA_2PT DOUBLE,\
        PCT_FGA_3PT DOUBLE,\
        PCT_PTS_2PT DOUBLE,\
        PCT_PTS_2PT_MR DOUBLE,\
        PCT_PTS_3PT DOUBLE,\
        PCT_PTS_FB DOUBLE,\
        PCT_PTS_FT DOUBLE,\
        PCT_PTS_OFF_TOV DOUBLE,\
        PCT_PTS_PAINT DOUBLE,\
        PCT_AST_2PM DOUBLE,\
        PCT_UAST_2PM DOUBLE,\
        PCT_AST_3PM DOUBLE,\
        PCT_UAST_3PM DOUBLE,\
        PCT_AST_FGM DOUBLE,\
        PCT_UAST_FGM DOUBLE,\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(scoring_boxscores_team_query)

        misc_boxscores_query = 'CREATE TABLE IF NOT EXISTS misc_boxscores\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        PTS_OFF_TOV INT,\
        PTS_2ND_CHANCE INT,\
        PTS_FB INT,\
        PTS_PAINT INT,\
        OPP_PTS_OFF_TOV INT,\
        OPP_PTS_2ND_CHANCE INT,\
        OPP_PTS_FB INT,\
        OPP_PTS_PAINT INT,\
        BLK INT,\
        BLKA INT,\
        PF INT,\
        PFD INT,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(misc_boxscores_query)

        misc_boxscores_team_query = 'CREATE TABLE IF NOT EXISTS misc_boxscores_team\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        MIN VARCHAR(255),\
        PTS_OFF_TOV INT,\
        PTS_2ND_CHANCE INT,\
        PTS_FB INT,\
        PTS_PAINT INT,\
        OPP_PTS_OFF_TOV INT,\
        OPP_PTS_2ND_CHANCE INT,\
        OPP_PTS_FB INT,\
        OPP_PTS_PAINT INT,\
        BLK INT,\
        BLKA INT,\
        PF INT,\
        PFD INT,\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(misc_boxscores_team_query)

        usage_boxscores_query = 'CREATE TABLE IF NOT EXISTS usage_boxscores\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        USG_PCT DOUBLE,\
        PCT_FGM DOUBLE,\
        PCT_FGA DOUBLE,\
        PCT_FG3M DOUBLE,\
        PCT_FG3A DOUBLE,\
        PCT_FTM DOUBLE,\
        PCT_FTA DOUBLE,\
        PCT_OREB DOUBLE,\
        PCT_DREB DOUBLE,\
        PCT_REB DOUBLE,\
        PCT_AST DOUBLE,\
        PCT_TOV DOUBLE,\
        PCT_STL DOUBLE,\
        PCT_BLK DOUBLE,\
        PCT_BLKA DOUBLE,\
        PCT_PF DOUBLE,\
        PCT_PFD DOUBLE,\
        PCT_PTS DOUBLE,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(usage_boxscores_query)

        four_factors_boxscores_query = 'CREATE TABLE IF NOT EXISTS four_factors_boxscores\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        START_POSITION VARCHAR(255),\
        COMMENT VARCHAR(255),\
        MIN VARCHAR(255),\
        EFG_PCT DOUBLE,\
        FTA_RATE DOUBLE,\
        TM_TOV_PCT DOUBLE,\
        OREB_PCT DOUBLE,\
        OPP_EFG_PCT DOUBLE,\
        OPP_FTA_RATE DOUBLE,\
        OPP_TOV_PCT DOUBLE,\
        OPP_OREB_PCT DOUBLE,\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(four_factors_boxscores_query)

        four_factors_boxscores_team_query = 'CREATE TABLE IF NOT EXISTS four_factors_boxscores_team\
        (\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        MIN VARCHAR(255),\
        EFG_PCT DOUBLE,\
        FTA_RATE DOUBLE,\
        TM_TOV_PCT DOUBLE,\
        OREB_PCT DOUBLE,\
        OPP_EFG_PCT DOUBLE,\
        OPP_FTA_RATE DOUBLE,\
        OPP_TOV_PCT DOUBLE,\
        OPP_OREB_PCT DOUBLE,\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(four_factors_boxscores_team_query)

        catch_shoot_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_catch_shoot\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        CATCH_SHOOT_FGM DOUBLE,\
        CATCH_SHOOT_FGA DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG3M DOUBLE,\
        CATCH_SHOOT_FG3A DOUBLE,\
        CATCH_SHOOT_FG3_PCT DOUBLE,\
        CATCH_SHOOT_EFG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(catch_shoot_sportvu_query)

        catch_shoot_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_catch_shoot_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        CATCH_SHOOT_FGM DOUBLE,\
        CATCH_SHOOT_FGA DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG3M DOUBLE,\
        CATCH_SHOOT_FG3A DOUBLE,\
        CATCH_SHOOT_FG3_PCT DOUBLE,\
        CATCH_SHOOT_EFG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(catch_shoot_sportvu_team_query)

        defense_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_defense\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        STL DOUBLE,\
        BLK DOUBLE,\
        DREB DOUBLE,\
        DEF_RIM_FGM DOUBLE,\
        DEF_RIM_FGA DOUBLE,\
        DEF_RIM_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(defense_sportvu_query)

        defense_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_defense_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        STL DOUBLE,\
        BLK DOUBLE,\
        DREB DOUBLE,\
        DEF_RIM_FGM DOUBLE,\
        DEF_RIM_FGA DOUBLE,\
        DEF_RIM_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(defense_sportvu_team_query)

        drives_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_drives\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DRIVES DOUBLE,\
        DRIVE_FGM DOUBLE,\
        DRIVE_FGA DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        DRIVE_FTM DOUBLE,\
        DRIVE_FTA DOUBLE,\
        DRIVE_FT_PCT DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_PTS_PCT DOUBLE,\
        DRIVE_PASSES DOUBLE,\
        DRIVE_PASSES_PCT DOUBLE,\
        DRIVE_AST DOUBLE,\
        DRIVE_AST_PCT DOUBLE,\
        DRIVE_TOV DOUBLE,\
        DRIVE_TOV_PCT DOUBLE,\
        DRIVE_PF DOUBLE,\
        DRIVE_PF_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(drives_sportvu_query)

        drives_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_drives_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DRIVES DOUBLE,\
        DRIVE_FGM DOUBLE,\
        DRIVE_FGA DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        DRIVE_FTM DOUBLE,\
        DRIVE_FTA DOUBLE,\
        DRIVE_FT_PCT DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_PTS_PCT DOUBLE,\
        DRIVE_PASSES DOUBLE,\
        DRIVE_PASSES_PCT DOUBLE,\
        DRIVE_AST DOUBLE,\
        DRIVE_AST_PCT DOUBLE,\
        DRIVE_TOV DOUBLE,\
        DRIVE_TOV_PCT DOUBLE,\
        DRIVE_PF DOUBLE,\
        DRIVE_PF_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(drives_sportvu_team_query)

        passing_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_passing\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        PASSES_MADE DOUBLE,\
        PASSES_RECEIVED DOUBLE,\
        AST DOUBLE,\
        FT_AST DOUBLE,\
        SECONDARY_AST DOUBLE,\
        POTENTIAL_AST DOUBLE,\
        AST_PTS_CREATED DOUBLE,\
        AST_ADJ DOUBLE,\
        AST_TO_PASS_PCT DOUBLE,\
        AST_TO_PASS_PCT_ADJ DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(passing_sportvu_query)
        
        passing_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_passing_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        PASSES_MADE DOUBLE,\
        PASSES_RECEIVED DOUBLE,\
        AST DOUBLE,\
        FT_AST DOUBLE,\
        SECONDARY_AST DOUBLE,\
        POTENTIAL_AST DOUBLE,\
        AST_PTS_CREATED DOUBLE,\
        AST_ADJ DOUBLE,\
        AST_TO_PASS_PCT DOUBLE,\
        AST_TO_PASS_PCT_ADJ DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(passing_sportvu_team_query)

        
        touches_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_touches\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        TOUCHES DOUBLE,\
        FRONT_CT_TOUCHES DOUBLE,\
        TIME_OF_POSS DOUBLE,\
        AVG_SEC_PER_TOUCH DOUBLE,\
        AVG_DRIB_PER_TOUCH DOUBLE,\
        PTS_PER_TOUCH DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PTS_PER_ELBOW_TOUCH DOUBLE,\
        PTS_PER_POST_TOUCH DOUBLE,\
        PTS_PER_PAINT_TOUCH DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        print touches_sportvu_query
        cursor.execute(touches_sportvu_query)
        
        
        
        touches_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_touches_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        TOUCHES DOUBLE,\
        FRONT_CT_TOUCHES DOUBLE,\
        TIME_OF_POSS DOUBLE,\
        AVG_SEC_PER_TOUCH DOUBLE,\
        AVG_DRIB_PER_TOUCH DOUBLE,\
        PTS_PER_TOUCH DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PTS_PER_ELBOW_TOUCH DOUBLE,\
        PTS_PER_POST_TOUCH DOUBLE,\
        PTS_PER_PAINT_TOUCH DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(touches_sportvu_team_query)        
        
        pull_up_shoot_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_pull_up_shoot\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(pull_up_shoot_sportvu_query)

        pull_up_shoot_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_pull_up_shoot_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(pull_up_shoot_sportvu_team_query)

        rebounding_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_rebounding\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        OREB DOUBLE,\
        OREB_CONTEST DOUBLE,\
        OREB_UNCONTEST DOUBLE,\
        OREB_CONTEST_PCT DOUBLE,\
        OREB_CHANCES DOUBLE,\
        OREB_CHANCE_PCT DOUBLE,\
        OREB_CHANCE_DEFER DOUBLE,\
        OREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_OREB_DIST DOUBLE,\
        DREB DOUBLE,\
        DREB_CONTEST DOUBLE,\
        DREB_UNCONTEST DOUBLE,\
        DREB_CONTEST_PCT DOUBLE,\
        DREB_CHANCES DOUBLE,\
        DREB_CHANCE_PCT DOUBLE,\
        DREB_CHANCE_DEFER DOUBLE,\
        DREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_DREB_DIST DOUBLE,\
        REB DOUBLE,\
        REB_CONTEST DOUBLE,\
        REB_UNCONTEST DOUBLE,\
        REB_CONTEST_PCT DOUBLE,\
        REB_CHANCES DOUBLE,\
        REB_CHANCE_PCT DOUBLE,\
        REB_CHANCE_DEFER DOUBLE,\
        REB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_REB_DIST DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(rebounding_sportvu_query)

        rebounding_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_rebounding_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        OREB DOUBLE,\
        OREB_CONTEST DOUBLE,\
        OREB_UNCONTEST DOUBLE,\
        OREB_CONTEST_PCT DOUBLE,\
        OREB_CHANCES DOUBLE,\
        OREB_CHANCE_PCT DOUBLE,\
        OREB_CHANCE_DEFER DOUBLE,\
        OREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_OREB_DIST DOUBLE,\
        DREB DOUBLE,\
        DREB_CONTEST DOUBLE,\
        DREB_UNCONTEST DOUBLE,\
        DREB_CONTEST_PCT DOUBLE,\
        DREB_CHANCES DOUBLE,\
        DREB_CHANCE_PCT DOUBLE,\
        DREB_CHANCE_DEFER DOUBLE,\
        DREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_DREB_DIST DOUBLE,\
        REB DOUBLE,\
        REB_CONTEST DOUBLE,\
        REB_UNCONTEST DOUBLE,\
        REB_CONTEST_PCT DOUBLE,\
        REB_CHANCES DOUBLE,\
        REB_CHANCE_PCT DOUBLE,\
        REB_CHANCE_DEFER DOUBLE,\
        REB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_REB_DIST DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(rebounding_sportvu_team_query)

        shooting_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_shooting\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(shooting_sportvu_query)

        shooting_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_shooting_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(shooting_sportvu_team_query)

        speed_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_speed\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DIST_FEET DOUBLE,\
        DIST_MILES DOUBLE,\
        DIST_MILES_OFF DOUBLE,\
        DIST_MILES_DEF DOUBLE,\
        AVG_SPEED DOUBLE,\
        AVG_SPEED_OFF DOUBLE,\
        AVG_SPEED_DEF DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(speed_sportvu_query)

        speed_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_speed_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DIST_FEET DOUBLE,\
        DIST_MILES DOUBLE,\
        DIST_MILES_OFF DOUBLE,\
        DIST_MILES_DEF DOUBLE,\
        AVG_SPEED DOUBLE,\
        AVG_SPEED_OFF DOUBLE,\
        AVG_SPEED_DEF DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(speed_sportvu_team_query)

        elbow_touches_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_elbow_touches\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        ELBOW_TOUCH_FGM DOUBLE,\
        ELBOW_TOUCH_FGA DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_FTM DOUBLE,\
        ELBOW_TOUCH_FTA DOUBLE,\
        ELBOW_TOUCH_FT_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_PTS_PCT DOUBLE,\
        ELBOW_TOUCH_PASSES DOUBLE,\
        ELBOW_TOUCH_PASSES_PCT DOUBLE,\
        ELBOW_TOUCH_AST DOUBLE,\
        ELBOW_TOUCH_AST_PCT DOUBLE,\
        ELBOW_TOUCH_TOV DOUBLE,\
        ELBOW_TOUCH_TOV_PCT DOUBLE,\
        ELBOW_TOUCH_FOULS DOUBLE,\
        ELBOW_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(elbow_touches_sportvu_query)

        elbow_touches_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_elbow_touches_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        ELBOW_TOUCH_FGM DOUBLE,\
        ELBOW_TOUCH_FGA DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_FTM DOUBLE,\
        ELBOW_TOUCH_FTA DOUBLE,\
        ELBOW_TOUCH_FT_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_PTS_PCT DOUBLE,\
        ELBOW_TOUCH_PASSES DOUBLE,\
        ELBOW_TOUCH_PASSES_PCT DOUBLE,\
        ELBOW_TOUCH_AST DOUBLE,\
        ELBOW_TOUCH_AST_PCT DOUBLE,\
        ELBOW_TOUCH_TOV DOUBLE,\
        ELBOW_TOUCH_TOV_PCT DOUBLE,\
        ELBOW_TOUCH_FOULS DOUBLE,\
        ELBOW_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(elbow_touches_sportvu_team_query)

        post_touches_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_post_touches\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        POST_TOUCH_FGM DOUBLE,\
        POST_TOUCH_FGA DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_FTM DOUBLE,\
        POST_TOUCH_FTA DOUBLE,\
        POST_TOUCH_FT_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_PTS_PCT DOUBLE,\
        POST_TOUCH_PASSES DOUBLE,\
        POST_TOUCH_PASSES_PCT DOUBLE,\
        POST_TOUCH_AST DOUBLE,\
        POST_TOUCH_AST_PCT DOUBLE,\
        POST_TOUCH_TOV DOUBLE,\
        POST_TOUCH_TOV_PCT DOUBLE,\
        POST_TOUCH_FOULS DOUBLE,\
        POST_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(post_touches_sportvu_query)

        post_touches_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_post_touches_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        POST_TOUCH_FGM DOUBLE,\
        POST_TOUCH_FGA DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_FTM DOUBLE,\
        POST_TOUCH_FTA DOUBLE,\
        POST_TOUCH_FT_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_PTS_PCT DOUBLE,\
        POST_TOUCH_PASSES DOUBLE,\
        POST_TOUCH_PASSES_PCT DOUBLE,\
        POST_TOUCH_AST DOUBLE,\
        POST_TOUCH_AST_PCT DOUBLE,\
        POST_TOUCH_TOV DOUBLE,\
        POST_TOUCH_TOV_PCT DOUBLE,\
        POST_TOUCH_FOULS DOUBLE,\
        POST_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(post_touches_sportvu_team_query)

        paint_touches_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_paint_touches\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PAINT_TOUCH_FGM DOUBLE,\
        PAINT_TOUCH_FGA DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        PAINT_TOUCH_FTM DOUBLE,\
        PAINT_TOUCH_FTA DOUBLE,\
        PAINT_TOUCH_FT_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_PTS_PCT DOUBLE,\
        PAINT_TOUCH_PASSES DOUBLE,\
        PAINT_TOUCH_PASSES_PCT DOUBLE,\
        PAINT_TOUCH_AST DOUBLE,\
        PAINT_TOUCH_AST_PCT DOUBLE,\
        PAINT_TOUCH_TOV DOUBLE,\
        PAINT_TOUCH_TOV_PCT DOUBLE,\
        PAINT_TOUCH_FOULS DOUBLE,\
        PAINT_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(paint_touches_sportvu_query)

        paint_touches_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_paint_touches_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PAINT_TOUCH_FGM DOUBLE,\
        PAINT_TOUCH_FGA DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        PAINT_TOUCH_FTM DOUBLE,\
        PAINT_TOUCH_FTA DOUBLE,\
        PAINT_TOUCH_FT_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_PTS_PCT DOUBLE,\
        PAINT_TOUCH_PASSES DOUBLE,\
        PAINT_TOUCH_PASSES_PCT DOUBLE,\
        PAINT_TOUCH_AST DOUBLE,\
        PAINT_TOUCH_AST_PCT DOUBLE,\
        PAINT_TOUCH_TOV DOUBLE,\
        PAINT_TOUCH_TOV_PCT DOUBLE,\
        PAINT_TOUCH_FOULS DOUBLE,\
        PAINT_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(paint_touches_sportvu_team_query)

        possessions_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_possessions\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        TOUCHES DOUBLE,\
        FRONT_CT_TOUCHES DOUBLE,\
        TIME_OF_POSS DOUBLE,\
        AVG_SEC_PER_TOUCH DOUBLE,\
        AVG_DRIB_PER_TOUCH DOUBLE,\
        PTS_PER_TOUCH DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PTS_PER_ELBOW_TOUCH DOUBLE,\
        PTS_PER_POST_TOUCH DOUBLE,\
        PTS_PER_PAINT_TOUCH DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(possessions_sportvu_query)

        possessions_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_possessions_team\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        TOUCHES DOUBLE,\
        FRONT_CT_TOUCHES DOUBLE,\
        TIME_OF_POSS DOUBLE,\
        AVG_SEC_PER_TOUCH DOUBLE,\
        AVG_DRIB_PER_TOUCH DOUBLE,\
        PTS_PER_TOUCH DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PTS_PER_ELBOW_TOUCH DOUBLE,\
        PTS_PER_POST_TOUCH DOUBLE,\
        PTS_PER_PAINT_TOUCH DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(possessions_sportvu_team_query)

        catch_shoot_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_catch_shoot_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        CATCH_SHOOT_FGM DOUBLE,\
        CATCH_SHOOT_FGA DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG3M DOUBLE,\
        CATCH_SHOOT_FG3A DOUBLE,\
        CATCH_SHOOT_FG3_PCT DOUBLE,\
        CATCH_SHOOT_EFG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(catch_shoot_sportvu_query)

        catch_shoot_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_catch_shoot_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        CATCH_SHOOT_FGM DOUBLE,\
        CATCH_SHOOT_FGA DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG3M DOUBLE,\
        CATCH_SHOOT_FG3A DOUBLE,\
        CATCH_SHOOT_FG3_PCT DOUBLE,\
        CATCH_SHOOT_EFG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(catch_shoot_sportvu_team_query)

        defense_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_defense_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        STL DOUBLE,\
        BLK DOUBLE,\
        DREB DOUBLE,\
        DEF_RIM_FGM DOUBLE,\
        DEF_RIM_FGA DOUBLE,\
        DEF_RIM_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(defense_sportvu_query)

        defense_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_defense_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        STL DOUBLE,\
        BLK DOUBLE,\
        DREB DOUBLE,\
        DEF_RIM_FGM DOUBLE,\
        DEF_RIM_FGA DOUBLE,\
        DEF_RIM_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(defense_sportvu_team_query)

        drives_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_drives_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DRIVES DOUBLE,\
        DRIVE_FGM DOUBLE,\
        DRIVE_FGA DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        DRIVE_FTM DOUBLE,\
        DRIVE_FTA DOUBLE,\
        DRIVE_FT_PCT DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_PTS_PCT DOUBLE,\
        DRIVE_PASSES DOUBLE,\
        DRIVE_PASSES_PCT DOUBLE,\
        DRIVE_AST DOUBLE,\
        DRIVE_AST_PCT DOUBLE,\
        DRIVE_TOV DOUBLE,\
        DRIVE_TOV_PCT DOUBLE,\
        DRIVE_PF DOUBLE,\
        DRIVE_PF_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(drives_sportvu_query)

        drives_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_drives_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DRIVES DOUBLE,\
        DRIVE_FGM DOUBLE,\
        DRIVE_FGA DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        DRIVE_FTM DOUBLE,\
        DRIVE_FTA DOUBLE,\
        DRIVE_FT_PCT DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_PTS_PCT DOUBLE,\
        DRIVE_PASSES DOUBLE,\
        DRIVE_PASSES_PCT DOUBLE,\
        DRIVE_AST DOUBLE,\
        DRIVE_AST_PCT DOUBLE,\
        DRIVE_TOV DOUBLE,\
        DRIVE_TOV_PCT DOUBLE,\
        DRIVE_PF DOUBLE,\
        DRIVE_PF_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(drives_sportvu_team_query)

        passing_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_passing_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        PASSES_MADE DOUBLE,\
        PASSES_RECEIVED DOUBLE,\
        AST DOUBLE,\
        FT_AST DOUBLE,\
        SECONDARY_AST DOUBLE,\
        POTENTIAL_AST DOUBLE,\
        AST_PTS_CREATED DOUBLE,\
        AST_ADJ DOUBLE,\
        AST_TO_PASS_PCT DOUBLE,\
        AST_TO_PASS_PCT_ADJ DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(passing_sportvu_query)

        passing_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_passing_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        PASSES_MADE DOUBLE,\
        PASSES_RECEIVED DOUBLE,\
        AST DOUBLE,\
        FT_AST DOUBLE,\
        SECONDARY_AST DOUBLE,\
        POTENTIAL_AST DOUBLE,\
        AST_PTS_CREATED DOUBLE,\
        AST_ADJ DOUBLE,\
        AST_TO_PASS_PCT DOUBLE,\
        AST_TO_PASS_PCT_ADJ DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(passing_sportvu_team_query)

        pull_up_shoot_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_pull_up_shoot_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(pull_up_shoot_sportvu_query)

        pull_up_shoot_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_pull_up_shoot_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(pull_up_shoot_sportvu_team_query)

        rebounding_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_rebounding_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        OREB DOUBLE,\
        OREB_CONTEST DOUBLE,\
        OREB_UNCONTEST DOUBLE,\
        OREB_CONTEST_PCT DOUBLE,\
        OREB_CHANCES DOUBLE,\
        OREB_CHANCE_PCT DOUBLE,\
        OREB_CHANCE_DEFER DOUBLE,\
        OREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_OREB_DIST DOUBLE,\
        DREB DOUBLE,\
        DREB_CONTEST DOUBLE,\
        DREB_UNCONTEST DOUBLE,\
        DREB_CONTEST_PCT DOUBLE,\
        DREB_CHANCES DOUBLE,\
        DREB_CHANCE_PCT DOUBLE,\
        DREB_CHANCE_DEFER DOUBLE,\
        DREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_DREB_DIST DOUBLE,\
        REB DOUBLE,\
        REB_CONTEST DOUBLE,\
        REB_UNCONTEST DOUBLE,\
        REB_CONTEST_PCT DOUBLE,\
        REB_CHANCES DOUBLE,\
        REB_CHANCE_PCT DOUBLE,\
        REB_CHANCE_DEFER DOUBLE,\
        REB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_REB_DIST DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(rebounding_sportvu_query)

        rebounding_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_rebounding_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        OREB DOUBLE,\
        OREB_CONTEST DOUBLE,\
        OREB_UNCONTEST DOUBLE,\
        OREB_CONTEST_PCT DOUBLE,\
        OREB_CHANCES DOUBLE,\
        OREB_CHANCE_PCT DOUBLE,\
        OREB_CHANCE_DEFER DOUBLE,\
        OREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_OREB_DIST DOUBLE,\
        DREB DOUBLE,\
        DREB_CONTEST DOUBLE,\
        DREB_UNCONTEST DOUBLE,\
        DREB_CONTEST_PCT DOUBLE,\
        DREB_CHANCES DOUBLE,\
        DREB_CHANCE_PCT DOUBLE,\
        DREB_CHANCE_DEFER DOUBLE,\
        DREB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_DREB_DIST DOUBLE,\
        REB DOUBLE,\
        REB_CONTEST DOUBLE,\
        REB_UNCONTEST DOUBLE,\
        REB_CONTEST_PCT DOUBLE,\
        REB_CHANCES DOUBLE,\
        REB_CHANCE_PCT DOUBLE,\
        REB_CHANCE_DEFER DOUBLE,\
        REB_CHANCE_PCT_ADJ DOUBLE,\
        AVG_REB_DIST DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(rebounding_sportvu_team_query)

        shooting_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_shooting_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(shooting_sportvu_query)

        shooting_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_shooting_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        DRIVE_PTS DOUBLE,\
        DRIVE_FG_PCT DOUBLE,\
        CATCH_SHOOT_PTS DOUBLE,\
        CATCH_SHOOT_FG_PCT DOUBLE,\
        PULL_UP_PTS DOUBLE,\
        PULL_UP_FG_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        EFF_FG_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(shooting_sportvu_team_query)

        speed_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_speed_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DIST_FEET DOUBLE,\
        DIST_MILES DOUBLE,\
        DIST_MILES_OFF DOUBLE,\
        DIST_MILES_DEF DOUBLE,\
        AVG_SPEED DOUBLE,\
        AVG_SPEED_OFF DOUBLE,\
        AVG_SPEED_DEF DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(speed_sportvu_query)

        speed_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_speed_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        DIST_FEET DOUBLE,\
        DIST_MILES DOUBLE,\
        DIST_MILES_OFF DOUBLE,\
        DIST_MILES_DEF DOUBLE,\
        AVG_SPEED DOUBLE,\
        AVG_SPEED_OFF DOUBLE,\
        AVG_SPEED_DEF DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(speed_sportvu_team_query)

        elbow_touches_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_elbow_touches_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        ELBOW_TOUCH_FGM DOUBLE,\
        ELBOW_TOUCH_FGA DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_FTM DOUBLE,\
        ELBOW_TOUCH_FTA DOUBLE,\
        ELBOW_TOUCH_FT_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_PTS_PCT DOUBLE,\
        ELBOW_TOUCH_PASSES DOUBLE,\
        ELBOW_TOUCH_PASSES_PCT DOUBLE,\
        ELBOW_TOUCH_AST DOUBLE,\
        ELBOW_TOUCH_AST_PCT DOUBLE,\
        ELBOW_TOUCH_TOV DOUBLE,\
        ELBOW_TOUCH_TOV_PCT DOUBLE,\
        ELBOW_TOUCH_FOULS DOUBLE,\
        ELBOW_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(elbow_touches_sportvu_query)

        elbow_touches_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_elbow_touches_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        ELBOW_TOUCH_FGM DOUBLE,\
        ELBOW_TOUCH_FGA DOUBLE,\
        ELBOW_TOUCH_FG_PCT DOUBLE,\
        ELBOW_TOUCH_FTM DOUBLE,\
        ELBOW_TOUCH_FTA DOUBLE,\
        ELBOW_TOUCH_FT_PCT DOUBLE,\
        ELBOW_TOUCH_PTS DOUBLE,\
        ELBOW_TOUCH_PTS_PCT DOUBLE,\
        ELBOW_TOUCH_PASSES DOUBLE,\
        ELBOW_TOUCH_PASSES_PCT DOUBLE,\
        ELBOW_TOUCH_AST DOUBLE,\
        ELBOW_TOUCH_AST_PCT DOUBLE,\
        ELBOW_TOUCH_TOV DOUBLE,\
        ELBOW_TOUCH_TOV_PCT DOUBLE,\
        ELBOW_TOUCH_FOULS DOUBLE,\
        ELBOW_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(elbow_touches_sportvu_team_query)

        post_touches_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_post_touches_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        POST_TOUCH_FGM DOUBLE,\
        POST_TOUCH_FGA DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_FTM DOUBLE,\
        POST_TOUCH_FTA DOUBLE,\
        POST_TOUCH_FT_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_PTS_PCT DOUBLE,\
        POST_TOUCH_PASSES DOUBLE,\
        POST_TOUCH_PASSES_PCT DOUBLE,\
        POST_TOUCH_AST DOUBLE,\
        POST_TOUCH_AST_PCT DOUBLE,\
        POST_TOUCH_TOV DOUBLE,\
        POST_TOUCH_TOV_PCT DOUBLE,\
        POST_TOUCH_FOULS DOUBLE,\
        POST_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(post_touches_sportvu_query)

        post_touches_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_post_touches_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        POST_TOUCH_FGM DOUBLE,\
        POST_TOUCH_FGA DOUBLE,\
        POST_TOUCH_FG_PCT DOUBLE,\
        POST_TOUCH_FTM DOUBLE,\
        POST_TOUCH_FTA DOUBLE,\
        POST_TOUCH_FT_PCT DOUBLE,\
        POST_TOUCH_PTS DOUBLE,\
        POST_TOUCH_PTS_PCT DOUBLE,\
        POST_TOUCH_PASSES DOUBLE,\
        POST_TOUCH_PASSES_PCT DOUBLE,\
        POST_TOUCH_AST DOUBLE,\
        POST_TOUCH_AST_PCT DOUBLE,\
        POST_TOUCH_TOV DOUBLE,\
        POST_TOUCH_TOV_PCT DOUBLE,\
        POST_TOUCH_FOULS DOUBLE,\
        POST_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(post_touches_sportvu_team_query)

        paint_touches_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_paint_touches_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PAINT_TOUCH_FGM DOUBLE,\
        PAINT_TOUCH_FGA DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        PAINT_TOUCH_FTM DOUBLE,\
        PAINT_TOUCH_FTA DOUBLE,\
        PAINT_TOUCH_FT_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_PTS_PCT DOUBLE,\
        PAINT_TOUCH_PASSES DOUBLE,\
        PAINT_TOUCH_PASSES_PCT DOUBLE,\
        PAINT_TOUCH_AST DOUBLE,\
        PAINT_TOUCH_AST_PCT DOUBLE,\
        PAINT_TOUCH_TOV DOUBLE,\
        PAINT_TOUCH_TOV_PCT DOUBLE,\
        PAINT_TOUCH_FOULS DOUBLE,\
        PAINT_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(paint_touches_sportvu_query)

        paint_touches_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_paint_touches_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PAINT_TOUCH_FGM DOUBLE,\
        PAINT_TOUCH_FGA DOUBLE,\
        PAINT_TOUCH_FG_PCT DOUBLE,\
        PAINT_TOUCH_FTM DOUBLE,\
        PAINT_TOUCH_FTA DOUBLE,\
        PAINT_TOUCH_FT_PCT DOUBLE,\
        PAINT_TOUCH_PTS DOUBLE,\
        PAINT_TOUCH_PTS_PCT DOUBLE,\
        PAINT_TOUCH_PASSES DOUBLE,\
        PAINT_TOUCH_PASSES_PCT DOUBLE,\
        PAINT_TOUCH_AST DOUBLE,\
        PAINT_TOUCH_AST_PCT DOUBLE,\
        PAINT_TOUCH_TOV DOUBLE,\
        PAINT_TOUCH_TOV_PCT DOUBLE,\
        PAINT_TOUCH_FOULS DOUBLE,\
        PAINT_TOUCH_FOULS_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(paint_touches_sportvu_team_query)

        possessions_sportvu_query = 'CREATE TABLE IF NOT EXISTS sportvu_possessions_game_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        TOUCHES DOUBLE,\
        FRONT_CT_TOUCHES DOUBLE,\
        TIME_OF_POSS DOUBLE,\
        AVG_SEC_PER_TOUCH DOUBLE,\
        AVG_DRIB_PER_TOUCH DOUBLE,\
        PTS_PER_TOUCH DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PTS_PER_ELBOW_TOUCH DOUBLE,\
        PTS_PER_POST_TOUCH DOUBLE,\
        PTS_PER_PAINT_TOUCH DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ABBREVIATION, `DATE`, GAME_ID)\
        );'
        cursor.execute(possessions_sportvu_query)

        possessions_sportvu_team_query = 'CREATE TABLE IF NOT EXISTS sportvu_possessions_team_game_logs\
        (\
        TEAM_ID VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        GP INT,\
        W INT,\
        L INT,\
        MIN DOUBLE,\
        POINTS DOUBLE,\
        TOUCHES DOUBLE,\
        FRONT_CT_TOUCHES DOUBLE,\
        TIME_OF_POSS DOUBLE,\
        AVG_SEC_PER_TOUCH DOUBLE,\
        AVG_DRIB_PER_TOUCH DOUBLE,\
        PTS_PER_TOUCH DOUBLE,\
        ELBOW_TOUCHES DOUBLE,\
        POST_TOUCHES DOUBLE,\
        PAINT_TOUCHES DOUBLE,\
        PTS_PER_ELBOW_TOUCH DOUBLE,\
        PTS_PER_POST_TOUCH DOUBLE,\
        PTS_PER_PAINT_TOUCH DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, GAME_ID)\
        );'
        cursor.execute(possessions_sportvu_team_query)

        transition_team_query = 'CREATE TABLE IF NOT EXISTS synergy_transition_team_offense\
        (\
        TEAM_ID VARCHAR(255),\
        TeamName VARCHAR(255),\
        TeamNameAbbreviation VARCHAR(255),\
        TeamShortName VARCHAR(255),\
        GP INT,\
        Poss INT,\
        `Time` DOUBLE,\
        Points INT,\
        FGA INT,\
        FGM INT,\
        PPP DOUBLE,\
        WorsePPP INT,\
        BetterPPP INT,\
        PossG DOUBLE,\
        PPG DOUBLE,\
        FGAG DOUBLE,\
        FGMG DOUBLE,\
        FG_mG DOUBLE,\
        FG_m DOUBLE,\
        FG DOUBLE,\
        aFG DOUBLE,\
        FT DOUBLE,\
        `TO` DOUBLE,\
        SF DOUBLE,\
        PlusOne DOUBLE,\
        Score DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(transition_team_query)
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_transition_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_isolation_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_isolation_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_ball_handler_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_ball_handler_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_roll_man_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_roll_man_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_post_up_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_post_up_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_spot_up_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_spot_up_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_handoff_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_handoff_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_cut_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_cut_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_off_screen_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_off_screen_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_put_back_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_put_back_team_defense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_misc_team_offense LIKE synergy_transition_team_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_misc_team_defense LIKE synergy_transition_team_offense')

        transition_query = 'CREATE TABLE IF NOT EXISTS synergy_transition_offense\
        (\
        PLAYER_ID VARCHAR(255),\
        PlayerFirstName VARCHAR(255),\
        PlayerLastName VARCHAR(255),\
        PlayerNumber VARCHAR(255),\
        P VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TeamName VARCHAR(255),\
        TeamNameAbbreviation VARCHAR(255),\
        TeamShortName VARCHAR(255),\
        GP INT,\
        Poss INT,\
        `Time` DOUBLE,\
        Points INT,\
        FGA INT,\
        FGM INT,\
        PPP DOUBLE,\
        WorsePPP INT,\
        BetterPPP INT,\
        PossG DOUBLE,\
        PPG DOUBLE,\
        FGAG DOUBLE,\
        FGMG DOUBLE,\
        FG_mG DOUBLE,\
        FG_m DOUBLE,\
        FG DOUBLE,\
        aFG DOUBLE,\
        FT DOUBLE,\
        `TO` DOUBLE,\
        SF DOUBLE,\
        PlusOne DOUBLE,\
        Score DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(transition_query)
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_isolation_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_isolation_defense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_ball_handler_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_ball_handler_defense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_roll_man_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_pr_roll_man_defense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_post_up_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_post_up_defense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_spot_up_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_spot_up_defense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_handoff_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_handoff_defense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_cut_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_off_screen_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_off_screen_defense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_put_back_offense LIKE synergy_transition_offense')
        cursor.execute('CREATE TABLE IF NOT EXISTS synergy_misc_offense LIKE synergy_transition_offense')

        player_tracking_shot_logs_query = 'CREATE TABLE IF NOT EXISTS player_tracking_shot_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        MATCHUP VARCHAR(255),\
        LOCATION VARCHAR(255),\
        W VARCHAR(255),\
        FINAL_MARGIN INT,\
        SHOT_NUMBER INT,\
        PERIOD INT,\
        GAME_CLOCK VARCHAR(255),\
        SHOT_CLOCK VARCHAR(255),\
        DRIBBLES INT,\
        TOUCH_TIME DOUBLE,\
        SHOT_DIST DOUBLE,\
        PTS_TYPE INT,\
        SHOT_RESULT VARCHAR(255),\
        CLOSEST_DEFENDER VARCHAR(255),\
        CLOSEST_DEFENDER_PLAYER_ID VARCHAR(255),\
        CLOSE_DEF_DIST DOUBLE,\
        FGM BOOLEAN,\
        PTS INT,\
        PBP_EVENTNUM INT,\
        PRIMARY KEY(PLAYER_ID, GAME_ID, SHOT_NUMBER)\
        );'
        cursor.execute(player_tracking_shot_logs_query)

        player_tracking_rebound_logs_query = 'CREATE TABLE IF NOT EXISTS player_tracking_rebound_logs\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        MATCHUP VARCHAR(255),\
        LOCATION VARCHAR(255),\
        W VARCHAR(255),\
        FINAL_MARGIN INT,\
        REB_NUMBER INT,\
        PERIOD INT,\
        GAME_CLOCK VARCHAR(255),\
        REB_TYPE VARCHAR(255),\
        CONTESTED VARCHAR(255),\
        NUM_CONTESTED INT,\
        REB_DIST DOUBLE,\
        SHOOTER VARCHAR(255),\
        SHOOTER_PLAYER_ID VARCHAR(255),\
        SHOT_DIST DOUBLE,\
        SHOT_TYPE VARCHAR(255),\
        OREB BOOLEAN,\
        DREB BOOLEAN,\
        REB BOOLEAN,\
        PBP_EVENTNUM INT,\
        PRIMARY KEY(PLAYER_ID, GAME_ID, REB_NUMBER)\
        );'
        cursor.execute(player_tracking_rebound_logs_query)

        player_tracking_passes_made_query = 'CREATE TABLE IF NOT EXISTS player_tracking_passes_made\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME_LAST_FIRST VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        PASS_TYPE VARCHAR(255),\
        G INT,\
        PASS_TO VARCHAR(255),\
        PASS_TEAMMATE_PLAYER_ID VARCHAR(255),\
        FREQUENCY DOUBLE,\
        PASS VARCHAR(255),\
        AST VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG2M INT,\
        FG2A INT,\
        FG2_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, PASS_TEAMMATE_PLAYER_ID, TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(player_tracking_passes_made_query)

        player_tracking_passes_received_query = 'CREATE TABLE IF NOT EXISTS player_tracking_passes_received\
        (\
        PLAYER_ID VARCHAR(255),\
        PLAYER_NAME_LAST_FIRST VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        PASS_TYPE VARCHAR(255),\
        G INT,\
        PASS_FROM VARCHAR(255),\
        PASS_TEAMMATE_PLAYER_ID VARCHAR(255),\
        FREQUENCY DOUBLE,\
        PASS VARCHAR(255),\
        AST VARCHAR(255),\
        FGM INT,\
        FGA INT,\
        FG_PCT DOUBLE,\
        FG2M INT,\
        FG2A INT,\
        FG2_PCT DOUBLE,\
        FG3M INT,\
        FG3A INT,\
        FG3_PCT DOUBLE,\
        `DATE` DATE,\
        IS_REGULAR_SEASON BOOLEAN,\
        PRIMARY KEY(PLAYER_ID, PASS_TEAMMATE_PLAYER_ID, TEAM_ID, `DATE`, IS_REGULAR_SEASON)\
        );'
        cursor.execute(player_tracking_passes_received_query)

        game_info_query = 'CREATE TABLE IF NOT EXISTS game_info\
        (\
        GAME_DATE VARCHAR(255),\
        ATTENDANCE INT,\
        GAME_TIME VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        PRIMARY KEY(GAME_ID)\
        );'
        cursor.execute(game_info_query)

        game_summary_query = 'CREATE TABLE IF NOT EXISTS game_summary\
        (\
        GAME_DATE_EST VARCHAR(255),\
        GAME_SEQUENCE INT,\
        GAME_ID VARCHAR(255),\
        GAME_STATUS_ID VARCHAR(255),\
        GAME_STATUS_TEXT VARCHAR(255),\
        GAMECODE VARCHAR(255),\
        HOME_TEAM_ID VARCHAR(255),\
        VISITOR_TEAM_ID VARCHAR(255),\
        SEASON VARCHAR(255),\
        LIVE_PERIOD INT,\
        LIVE_PC_TIME VARCHAR(255),\
        NATL_TV_BROADCASTER_ABBREVIATION VARCHAR(255),\
        LIVE_PERIOD_TIME_BCAST VARCHAR(255),\
        WH_STATUS VARCHAR(255),\
        PRIMARY KEY(GAME_ID)\
        );'
        cursor.execute(game_summary_query)

        officials_query = 'CREATE TABLE IF NOT EXISTS officials\
        (\
        OFFICIAL_ID VARCHAR(255),\
        FIRST_NAME VARCHAR(255),\
        LAST_NAME VARCHAR(255),\
        JERSEY_NUM INT,\
        GAME_ID VARCHAR(255),\
        PRIMARY KEY(GAME_ID, OFFICIAL_ID)\
        );'
        cursor.execute(officials_query)

        other_stats_query = 'CREATE TABLE IF NOT EXISTS other_stats\
        (\
        LEAGUE_ID VARCHAR(255),\
        SEASON_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        PTS_PAINT INT,\
        PTS_2ND_CHANCE INT,\
        PTS_FB INT,\
        LARGEST_LEAD INT,\
        LEAD_CHANGES INT,\
        TIMES_TIED INT,\
        GAME_ID VARCHAR(255),\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(other_stats_query)

        line_score_query = 'CREATE TABLE IF NOT EXISTS line_score\
        (\
        GAME_DATE_EST VARCHAR(255),\
        GAME_SEQUENCE INT,\
        GAME_ID VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        TEAM_CITY_NAME VARCHAR(255),\
        TEAM_NICKNAME VARCHAR(255),\
        TEAM_WINS_LOSSES VARCHAR(255),\
        PTS_QTR1 INT,\
        PTS_QTR2 INT,\
        PTS_QTR3 INT,\
        PTS_QTR4 INT,\
        PTS_OT1 INT,\
        PTS_OT2 INT,\
        PTS_OT3 INT,\
        PTS_OT4 INT,\
        PTS_OT5 INT,\
        PTS_OT6 INT,\
        PTS_OT7 INT,\
        PTS_OT8 INT,\
        PTS_OT9 INT,\
        PTS_OT10 INT,\
        PTS INT,\
        PRIMARY KEY(GAME_ID, TEAM_ID)\
        );'
        cursor.execute(line_score_query)

        inactives_query = 'CREATE TABLE IF NOT EXISTS inactives\
        (\
        PLAYER_ID VARCHAR(255),\
        FIRST_NAME VARCHAR(255),\
        LAST_NAME VARCHAR(255),\
        JERSEY_NUM VARCHAR(255),\
        TEAM_ID VARCHAR(255),\
        TEAM_CITY VARCHAR(255),\
        TEAM_NAME VARCHAR(255),\
        TEAM_ABBREVIATION VARCHAR(255),\
        GAME_ID VARCHAR(255),\
        PRIMARY KEY(GAME_ID, PLAYER_ID)\
        );'
        cursor.execute(inactives_query)
        
        schedule = 'CREATE TABLE IF NOT EXISTS schedule\
        (\
        `DATE` DATE,\
        TEAM_ABBREVIATION VARCHAR(255)\
        );'
        cursor.execute(schedule)

        """
        game_summary = 'CREATE TABLE IF NOT EXISTS game_summary\
        (\
        GAME_DATE_EST, DATE,\
        GAME_SEQUENCE, VARCHAR(255),\
        GAME_ID, VARCHAR(255),\
        GAME_STATUS_ID, VARCHAR(255),\
        GAME_STATUS_TEXT, VARCHAR(255),\
        GAMECODE, VARCHAR(255),\
        HOME_TEAM_ID, VARCHAR(255),\
        VISITOR_TEAM_ID, VARCHAR(255),\
        SEASON, VARCHAR(255),\
        LIVE_PERIOD, VARCHAR(255),\
        LIVE_PC_TIME, VARCHAR(255),\
        NATL_TV_BROADCASTER_ABBREVIATION, VARCHAR(255),\
        LIVE_PERIOD_TIME_BCAST, VARCHAR(255),\
        WH_STATUS, VARCHAR(255)\
        );'
        cursor.execute(game_summary)
        """
        
        depth_chart_rotoworld = 'CREATE TABLE IF NOT EXISTS depth_chart\
        (\
        TEAM VARCHAR(255),\
        POSITION VARCHAR(255),\
        RANK VARCHAR(255),\
        PLAYER_NAME VARCHAR(255),\
        REPORT LONGTEXT,\
        IMPACT LONGTEXT\
        );'
        cursor.execute(depth_chart_rotoworld)
        
        
        
        conn.commit()
        conn.close()
