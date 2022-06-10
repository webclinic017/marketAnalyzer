
from config import load_config
import pandas as pd
import mysql.connector
config=load_config.config()
import sqlalchemy
class bd_handler:
    def __init__(self, database=None):
        self.properties=config["bd"]
        if database is None:
            database= self.properties["database"]
        self.connection=mysql.connector.connect(
                host=self.properties["host"],
                user=self.properties["user"],
                port=self.properties["port"],
                password=self.properties["password"],
                database=database
        )
        self.cursor=self.connection.cursor()
        self.engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(
            self.properties["user"], self.properties["password"], self.properties["host"] + ":" + str(self.properties["port"]), database), pool_recycle=3600, pool_size=5).connect()

    def execute(self,statement,params=None):
        if params is not None:
            self.cursor.execute(statement,params)
        else:
            self.cursor.execute(statement)
        self.connection.commit()

    def execute_query(self, statement, params=None):
        if params is not None:
            self.cursor.execute(statement,params)
        else:
            self.cursor.execute(statement)
        data=self.cursor.fetchall()
        self.connection.commit()
        if len(data)==0:
            return None
        return data

    def execute_query_dataframe(self, statement, params=None):
        if params is not None:
            self.cursor.execute(statement, params)
        else:
            self.cursor.execute(statement)
        data = pd.DataFrame(self.cursor.fetchall())
        if len(data)>0:
            data.columns = self.cursor.column_names
        else:
            data=None
        self.connection.commit()
        return data

    def bulk_insert(self,data,table_name):
        """

        :param data:  dataframe to insert
        :param table_name: name of the table in the db
        """
        data.to_sql(table_name, self.engine, if_exists="append", chunksize=1000)


