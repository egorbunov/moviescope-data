import sys
import traceback

from imdbpie import Imdb
from requests.exceptions import HTTPError
from tqdm import tqdm

from imdb_crawl_db import *


def process_one_movie(imdb_id, imdb):
    max_reviews_num = 1000
    m = imdb.get_title_by_id(imdb_id)
    if m is None:
        return None
    plots = imdb.get_title_plots(imdb_id) or []
    reviews = imdb.get_title_reviews(imdb_id, max_results=max_reviews_num) or []
    return ReviewedMovie(
        imdb_id, m.title, m.release_date,
        [a.name for a in m.credits], plots, m.poster_url,
        [Review(r.summary, r.text, r.rating) for r in reviews]
    )


def process_movies(ids, connection):
    imdb = Imdb(anonymize=True)

    imdb_ids = get_all_movie_ids(connection)
    print("{} ids need to proceed".format(len(ids)))
    print("{} films in database".format(len(imdb_ids)))
    to_process = set(ids).difference(set(imdb_ids))
    print("{} ids left to proceed".format(len(to_process)))

    for imdb_id in tqdm(to_process):
        try:
            m = process_one_movie(imdb_id, imdb)
            if m is None:
                continue
            add_new_movie(m, connection)
        except (KeyboardInterrupt, SystemExit):
            print("Got keyboard interrupt or sys exit...finishing...")
            break
        except HTTPError as e:
            rsp = e.response
            if rsp.status_code == 404:
                print("ERROR: Not found id {}".format(imdb_id))
                continue
            elif rsp.status_code == 400:
                print("ERROR: Bad request for id {}".format(imdb_id))
            else:
                traceback.print_exc()
                break
        except json.decoder.JSONDecodeError as e:
            print("GOT JSON DECODE ERROR!!!!!!!! Next id...")
            continue
        except Exception as e:
            traceback.print_exc()
            break


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json> <imdb ids file>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = connect_db(*read_db_config(f))

    with open(sys.argv[2]) as f:
        array = json.load(f)["results"]["bindings"]
        ids = [obj["id"]["value"] for obj in array]

    process_movies(ids, conn)

    conn.close()




