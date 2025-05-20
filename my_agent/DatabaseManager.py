import requests
import os
from typing import List, Any
import mysql.connector
import sys
import time


class DatabaseManager:
    def __init__(self):
        self.endpoint_url = "mysql://root:Mysqlharsh17@127.0.0.1/blinkit"


    def connect_to_database(self, host, user, password, database=None, connection_timeout=5, connect_timeout=5, use_pure=True):
        """Attempt to connect to the MySQL database."""
        try:
            if database:
                conn = mysql.connector.connect(
                    host=host,
                    user=user,
                    password=password,
                    database=database,
                    connection_timeout=5,
                    connect_timeout=5,
                    use_pure=True
                )
                print(f"Connected to database '{database}'")
            else:
                conn = mysql.connector.connect(
                    host=host,
                    user=user,
                    password=password,
                    connection_timeout=5,
                    connect_timeout=5,
                    use_pure=True
                )
                print(f"Connected to MySQL server but not to a {database} database")
            return conn
        except mysql.connector.Error as err:
            print(f"Connection error: {err}")
            return None
    



    # def execute_query(self, uuid: str, query: str) -> List[Any]:
    #     """Execute SQL query on the remote database and return results."""
    #     try:
    #         response = requests.post(
    #             f"{self.endpoint_url}/execute-query",
    #             json={"uuid": uuid, "query": query}
    #         )
    #         response.raise_for_status()
    #         return response.json()['results']
    #     except requests.RequestException as e:
    #         raise Exception(f"Error executing query: {str(e)}")