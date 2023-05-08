from .configuration import api_configuration

import psycopg2
import sys

class Database:
    
    def __init__(self) -> None:
        self.connection = self.establish_connection()

    def establish_connection(self):
        try:
            return psycopg2.connect(
                    host = api_configuration.DATABASE_HOST,
                    database = api_configuration.DATABASE_NAME,
                    user = api_configuration.DATABASE_USER,
                    password = api_configuration.DATABASE_PASSWORD,
                    port = api_configuration.DATABASE_PORT
                )
        except psycopg2.OperationalError as error:
            print(f"Error during database connection: { error }")
            sys.exit(1)

    def terminate_connection(self) -> None:
        self.connection.close()

    def close_cursor(self) -> None:
        self.cursor.close()

    def create_cursor(self) -> None:
        self.cursor = self.connection.cursor()

    def single_record(self, query: str) -> tuple | None:
        self.create_cursor()
        self.cursor.execute(query)

        try:
            return self.cursor.fetchone()
        except psycopg2.ProgrammingError as error:
            print(f"Error during single record query: { error }")
            return None

    def multiple_records(self, query: str, amount: int) -> list[tuple] | None:
        self.create_cursor()
        self.cursor.execute(query)

        try:
            return self.cursor.fetchmany(amount)
        except psycopg2.ProgrammingError as error:
            print(f"Error during multiple records query: { error }")
            return None

    def all_records(self, query: str) -> list[tuple] | None:
        self.create_cursor()
        self.cursor.execute(query)

        try:
            return self.cursor.fetchall()
        except psycopg2.ProgrammingError as error:
            print(f"Error during all records query: { error }")
            return None

database = Database()