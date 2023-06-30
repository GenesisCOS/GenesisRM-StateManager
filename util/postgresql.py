import psycopg2 as pg
import pandas as pd


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

    def connect(self):
        return pg.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def exec(self, sql: str) -> pd.DataFrame:
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
