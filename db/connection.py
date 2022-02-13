import psycopg2
from db.config import config

class Connection(object):
    def __init__(self):
        self.conn = None
        self.setConnection()

    def setConnection(self):
        try:
            # read connection parameters
            params = config()		
            self.conn = psycopg2.connect(**params)            
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getConnection(self):
        return self.conn