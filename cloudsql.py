import MySQLdb
import logging


class dbCursor(object):

    def __init__(self, env_string=None):
        self.env = env_string

    def __enter__(self):
        # open a sql connection
        self._get_sql_connection()
        self.cursor = self.db.cursor()
        return self.cursor

    def __exit__(self, type, value, traceback):
        self.db.commit()
        self.cursor.close()
        self.db.close()

    def _get_sql_connection(self):
        if (self.env and self.env.startswith('gce')):
            self.db = MySQLdb.connect(
                host='localhost',
                user='mjfm',
                passwd='mjfm',
                charset='utf8',
                db='auction_ball',
                use_unicode=True
            )

        else:
            self.db = MySQLdb.connect(
                host='localhost',
                user='mjfm',
                passwd='mjfm',
                charset='utf8',
                db='auction_ball',
                use_unicode=True
            )


def update_league_season(league_id,season_id,field,value):
    string = """
        UPDATE league_seasons set {} = {}
        WHERE league_id = {} and season_id = {}
    """.format(field,value,league_id,season_id)
    with dbCursor('gce') as cursor:
        cursor.execute(string)