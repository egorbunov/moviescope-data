import psycopg2
import json


class Review:
    def __init__(self, summary, body, score):
        self.summary = summary
        self.body = body
        self.score = score


class ReviewedMovie:
    def __init__(self, imdb_id, title, date, actors, plots, poster_url, reviews):
        self.imdb_id = imdb_id
        self.title = title
        self.date = date
        self.actors = actors
        self.plots = plots
        self.poster_url = poster_url
        self.reviews = reviews

    def as_dict(self):
        return self.__dict__


def add_new_movie(movie, connection):
    """
    :param movie:ReviewedMovie
    :return:
    """
    plot = "\n".join(movie.plots)
    actors = "; ".join(movie.actors)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO movie (imdb_id, title, plot, poster_url, actors, date) "
                   "VALUES (%s, %s, %s, %s, %s, %s)",
                   (movie.imdb_id, movie.title, plot, movie.poster_url, actors, movie.date))

    for r in movie.reviews:
        cursor.execute("INSERT INTO review (movie_id, summary, body, score) "
                       "VALUES (%s, %s, %s, %s)",
                       (movie.imdb_id, r.summary, r.body, r.score))

    connection.commit()


def get_all_movie_ids(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT imdb_id FROM movie")
    return set([r[0] for r in cursor])


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
