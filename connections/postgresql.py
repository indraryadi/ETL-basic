import json
from sqlalchemy import create_engine
from psycopg2 import connect
from psycopg2.extras import execute_values

class PostgreSql:
    def __init__(self,cfg):
        self.host=cfg["host"]
        self.port=cfg["port"]
        self.database=cfg["database"]
        self.username=cfg["username"]
        self.password=cfg["password"]
    
    def conn(self,conn_type='engine'):
        try:
            if conn_type == 'engine':
                engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
                conn_engine = engine.connect()
                print("Connect Engine Postgresql")
                return engine, conn_engine
            else:
                conn = connect(
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.database
                    )
                cursor = conn.cursor()
                print("Connect Cursor Postgresql")
                return conn, cursor
        except (Exception) as e:
            print('CANNOT CONNECT TO POSTGRESQL!!!')
            return e