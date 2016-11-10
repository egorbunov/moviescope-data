import psycopg2
import json


def read_db_config(file_in):
    """
    Reads postgres database config
    Config must be stored as json object, example:

        {
          "user": "eg",
          "database": "moviescope",
          "host": "localhost",
          "password": ""
        }

    :param file_in: opened config stream
    :return: tuple (username, database name, host)
    """
    config = json.load(file_in)
    user = config["user"]
    db_name = config["database"]
    host = "localhost" if "host" not in config else config["host"]
    password = "" if "password" not in config else config["password"]
    return user, db_name, host, password


def connect_db(user, db_name, host, password):
    return psycopg2.connect(
        "dbname='{}' user='{}' host='{}' password='{}'"
            .format(db_name, user, host, password)
    )
