import csv
import sys

import moviescope_db

"""
This scripts used to fill up database with movie data from movie summary tsv files
(or any other properly constructed tsv files):
movie metadata tsv file row:
    <wiki id> <lmdb id?> <title> <date> ... (other info not considered)

plots data tsv file row:
    <wiki id> <plot>

This files mush conform (for every plot pair there must be metadata)
"""


def get_all_db_wiki_ids(cursor):
    cursor.execute("SELECT wiki_id FROM movie;")
    movie_ids = set()
    for r in cursor:
        movie_ids.add(r[0])
    return movie_ids


def get_one_movie_data(cursor, wiki_id):
    cursor.execute("SELECT * FROM movie WHERE wiki_id = %s;", [wiki_id])
    rows = cursor.fetchall()
    if len(rows) == 0:
        return None
    else:
        assert len(rows) == 1
        res = rows[0]
        return {'title': res[2], 'plot': res[5]}


def update_movie_plot(cursor, wiki_id, new_plot):
    cursor.execute("UPDATE movie SET plot = %s WHERE wiki_id = %s;", (new_plot, wiki_id))


def add_new_movie(cursor, wiki_id, title, plot, year):
    cursor.execute("INSERT INTO movie (wiki_id, title, plot, year) VALUES (%s, %s, %s, %s);",
                   (wiki_id, title, plot, year))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json> <MOVIE_SUMMARY_METADATA.tsv> <MOVIE_SUMMARIES.txt>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = moviescope_db.connect_db(*moviescope_db.read_db_config(f))

    with open(sys.argv[2]) as tsv_meta_file, open(sys.argv[3]) as plots_f:
        cursor = conn.cursor()

        known_wiki_movie_ids = get_all_db_wiki_ids(cursor)

        skip_no_year = 0
        movies_metas = {}
        meta_reader = csv.reader(tsv_meta_file, delimiter='\t')
        for row in meta_reader:
            wiki_id = int(row[0])
            title = row[2]
            year = row[3].split('-')[0]
            if len(year) == 0:
                skip_no_year += 1
                continue
            movies_metas[wiki_id] = ({'title': title, 'year': year})

        print("Got {} movie metadata entries".format(len(movies_metas)))
        print("Skipped {} movies due to NULL year".format(skip_no_year))

        plots_reader = csv.reader(plots_f, delimiter='\t')
        new_movies = 0
        plot_added = 0
        skip_no_meta = 0
        movie_already_exists = 0
        plot_num = 0
        for row in plots_reader:
            plot_num += 1
            wiki_id = int(row[0])
            plot = row[1]
            movie_data = get_one_movie_data(cursor, wiki_id)
            if wiki_id not in movies_metas:
                if movie_data is not None and movie_data['plot'] is None:
                    plot_added += 1
                    update_movie_plot(cursor, wiki_id, plot)
                else:
                    skip_no_meta += 1
                continue

            meta = movies_metas[wiki_id]
            title = meta['title']
            year = meta['year']
            if movie_data is None:
                new_movies += 1
                add_new_movie(cursor, wiki_id, title, plot, year)
            elif movie_data['plot'] is None:
                plot_added += 1
                update_movie_plot(cursor, wiki_id, plot)
            else:
                movie_already_exists += 1

        conn.commit()
        print("Whole plot num proceed = {}".format(plot_num))
        print("Meta not found, but plot exists for {} movies".format(skip_no_meta))
        print("{} movies already in database".format(movie_already_exists))
        print("Add {} new movies!".format(new_movies))
        print("Fill {} empty plots!".format(plot_added))
