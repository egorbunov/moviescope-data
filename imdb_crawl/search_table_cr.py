from imdb_crawl_db import *
import sys
from itertools import islice


def batch_gen(iterator, batch_size):
    batch = islice(iterator, batch_size)
    while True:
        lst = list(batch)
        yield lst
        if len(lst) < batch_size:
            break
        batch = islice(iterator, batch_size)


def fill_plots(connection):
    print("Filling/updating plots...")
    get_cursor = connection.cursor()
    get_cursor.execute("SELECT movie_id FROM search")
    already_filled = set([r[0] for r in get_cursor])
    get_cursor.execute("SELECT imdb_id, plot FROM movie")
    insert_cursor = connection.cursor()
    cnt = 0
    for r in get_cursor:
        if cnt != 0 and cnt % 1000 == 0:
            print("{} plots filled".format(cnt))
        cnt += 1
        m_id = r[0]
        plot = r[1]
        if m_id in already_filled:
            insert_cursor.execute("UPDATE search SET plot=%s, reviews=%s where movie_id=%s", (plot, "", m_id))
        else:
            insert_cursor.execute("INSERT INTO search (movie_id, plot, reviews) VALUES (%s, %s, %s)", (m_id, plot, ""))
    connection.commit()
    print("Done!")


def fill_reviews(connection):
    get_cursor = connection.cursor()
    get_cursor.execute("SELECT movie_id, summary, body FROM review")
    insert_cursor = connection.cursor()
    cnt = 0
    batch_size = 10000
    for batch in batch_gen(get_cursor, batch_size):
        print("{} reviews processed".format(cnt))
        movies_dict = {}
        for row in batch:
            m_id = row[0]
            summary = row[1]
            review = row[2]
            if m_id not in movies_dict:
                movies_dict[m_id] = ""
            movies_dict[m_id] = "{}\n{}.{}".format(movies_dict[m_id], summary, review)
        for m_id in movies_dict:
            insert_cursor.execute("UPDATE search SET reviews = reviews || %s WHERE movie_id=%s",
                                  (movies_dict[m_id], m_id))
        connection.commit()
        cnt += len(batch)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        connection = connect_db(*read_db_config(f))
        fill_plots(connection)
        fill_reviews(connection)