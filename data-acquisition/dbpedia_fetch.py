import json
import sys
from tqdm import tqdm
import psycopg2
from DbpediaFetcher import DbpediaFetcher


def read_db_config(filename):
    """
    Reads postgres database config
    Config must be stored as json object, example:

        {
          "user": "eg",
          "database": "moviescope",
          "host": "localhost",
          "password": ""
        }

    :param filename: path to config
    :return: tuple (username, database name, host)
    """
    with open(filename, 'r') as f:
        config = json.load(f)
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


def fill_db_with_movies(conn):
    cursor = conn.cursor()
    dbpedia = DbpediaFetcher()

    cursor.execute("""SELECT wiki_id FROM movie""")
    rows = cursor.fetchall()
    movie_ids = set()
    for r in rows:
        movie_ids.add(r[0])

    for year in tqdm(range(1870, 2050)):
        films = dbpedia.get_films_for_year(year)
        for f in films:
            if f.wiki_id in movie_ids:
                continue
            movie_ids.add(f.wiki_id)
            cursor.execute(
                """INSERT INTO movie (wiki_id, title, abstract, year) VALUES (%s, %s, %s, %s);""",
                (f.wiki_id, f.title, f.abstract, f.year))
        conn.commit()
    cursor.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json>")
        sys.exit(1)
    conn = connect_db(*read_db_config(sys.argv[1]))
    fill_db_with_movies(conn)
    conn.close()
