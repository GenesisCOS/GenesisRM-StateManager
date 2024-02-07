import psycopg2 as pg
from psycopg2 import pool 
import pandas as pd

# TODO Add connection pool 
class PostgresqlDriver(object):
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 database: str):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.conn = self.connect()
        self.pool = pool.ThreadedConnectionPool(
            40, 100, 
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
        )

    def connect(self):
        return pg.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def exec(self, sql: str) -> pd.DataFrame | None:
        cursor = None 
        conn = None
        retval = None 
         
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()
            cursor.execute(sql)
            col_names = [i.name for i in cursor.description]
            records = cursor.fetchall()
            retval = pd.DataFrame(records, columns=col_names)
            for i in cursor.description:
                if i.type_code == 1114:
                    retval[i.name] = pd.to_datetime(retval[i.name].astype('str'))
        except Exception as e:
            print(e)
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                self.pool.putconn(conn)

        return retval
            

    def __exec(self, sql: str) -> pd.DataFrame:
        
        
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            col_names = [i.name for i in cur.description]

            # 一次性获取所有查询数据
            data = cur.fetchall()

            # 将查询到的数据整理为 Pandas DataFrame
            res = pd.DataFrame(data, columns=col_names)

        except pg.InterfaceError:
            self.conn.close()
            self.conn = self.connect()
            return self.exec(sql)
        
        for i in cur.description:
            if i.type_code == 1114:
                res[i.name] = pd.to_datetime(res[i.name].astype('str'))
        return res

    def close(self):
        self.conn.close()
