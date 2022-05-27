
from config import load_config
import pandas as pd
import mysql.connector
config=load_config.config()
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
    def execute(self,statement,params=None):
        if params is not None:
            self.cursor.execute(statement,params)
        else:
            self.cursor.execute(statement)
        self.connection.commit()

    def execute_consult(self,statement,params=None):
        if params is not None:
            self.cursor.execute(statement,params)
        else:
            self.cursor.execute(statement)
        data=self.cursor.fetchall()
        self.connection.commit()
        return data

    def execute_consult_dataframe(self, statement, params=None):
        if params is not None:
            self.cursor.execute(statement, params)
        else:
            self.cursor.execute(statement)
        data = pd.DataFrame(self.cursor.fetchall())
        data.columns = self.cursor.column_names
        self.connection.commit()
        return data


