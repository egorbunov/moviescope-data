from imdb_crawl_db import *
import sys


def movies_with_plots_and_reviews(connection):
    all_ids = get_all_movie_ids(connection)
    cursor = connection.cursor()
    for movie_id in all_ids:
        cursor.execute("SELECT plot FROM movie WHERE imdb_id=%s", [movie_id])
        rows = cursor.fetchall()
        assert len(rows) == 0 or len(rows) == 1
        plot = "" if len(rows) == 0 else rows[0][0]
        cursor.execute("SELECT summary, body FROM review WHERE movie_id=%s", [movie_id])
        # summary and review body are concatenated
        reviews = ["{}.{}".format(r[0], r[1]) for r in cursor]
        # all reviews are concatenated
        concatenated_reviews = "\n".join(reviews)
        yield {'movie_id': movie_id, 'plot': plot, 'reviews': concatenated_reviews}

    return set([r[0] for r in cursor])


def fill_search_table(connection):
    cursor = connection.cursor()
    # getting already filled movies data to decide UPDATE or INSERT
    cursor.execute("SELECT movie_id FROM search")
    already_filled = set([r[0] for r in cursor])
    for m in movies_with_plots_and_reviews(connection):
        if m['movie_id'] in already_filled:
            cursor.execute("UPDATE search SET plot=%s, reviews=%s WHERE movie_id=%s",
                           (m['plot'], m['reviews'], m['movie_id']))
        else:
            cursor.execute("INSERT INTO search (movie_id, plot, reviews) VALUES (%s, %s, %s)",
                           (m['movie_id'], m['plot'], m['reviews']))
        connection.commit()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        connection = connect_db(*read_db_config(f))
        fill_search_table(connection)