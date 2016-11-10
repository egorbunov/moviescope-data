import sys
from tqdm import tqdm
from DbpediaFetcher import DbpediaFetcher
import moviescope_db


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
    with open(sys.argv[1]) as f:
        conn = moviescope_db.connect_db(*moviescope_db.read_db_config(f))
    fill_db_with_movies(conn)
    conn.close()
