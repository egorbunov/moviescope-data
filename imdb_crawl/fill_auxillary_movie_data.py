import sys
import time
from imdbpie import Imdb
from requests.exceptions import HTTPError
from tqdm import tqdm

from imdb_crawl_db import *


def fill_one_movie(imdb_id, connection):
    imdb = Imdb(anonymize=True)
    movie = imdb.get_title_by_id(imdb_id)
    if movie is None:
        print("Movie data is none for id = {} =/".format(imdb_id))
        return
    genres = movie.genres
    votes = 0 if movie.votes is None else movie.votes
    rating = movie.rating
    m_type = movie.type
    year = movie.year

    cursor = connection.cursor()
    s = cursor.mogrify("UPDATE movie SET genres=%s, votes=%s, rating=%s, type=%s, year=%s WHERE imdb_id=%s",
                   (genres, votes, rating, m_type, year, imdb_id))
    print(s)
    cursor.execute("UPDATE movie SET genres=%s, votes=%s, rating=%s, type=%s, year=%s WHERE imdb_id=%s",
                   (genres, votes, rating, m_type, year, imdb_id))
    connection.commit()


def fill_data(connection):
    """
    I suppose that score is guaranteed to be filled, so we fill
    data for movies, where score is null
    """
    cursor = connection.cursor()
    cursor.execute("SELECT imdb_id FROM movie WHERE rating IS NULL;")
    id_rows = cursor.fetchall()
    imdb_ids = [r[0] for r in id_rows]
    for imdb_id in tqdm(imdb_ids):
        try:
            fill_one_movie(imdb_id, conn)
        except (KeyboardInterrupt, SystemExit):
            print("Got keyboard interrupt or sys exit. Finishing...")
            break
        except HTTPError as e:
            rsp = e.response
            if rsp.status_code == 404:
                print("Error: id {} not found (got http 404 error), skipping to next id...".format(imdb_id))
                continue
            elif rsp.status_code == 400:
                print("Error: bad request for id {} (got http 400 error), skipping to next id...".format(imdb_id))
            elif rsp.status_code == 502:
                print("Sleeping for 10 seconds...")
                time.sleep(10)
            else:
                print("BOOM")
                raise e
        except json.decoder.JSONDecodeError as _:
            print("Error: json decode error (imdb-pie internal error), skipping to next id...")
            continue

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = connect_db(*read_db_config(f))

    fill_data(conn)

    conn.close()

