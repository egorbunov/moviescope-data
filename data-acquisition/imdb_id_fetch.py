import wikipedia as wiki
import moviescope_db
import sys
from tqdm import tqdm

cache = open('cache.txt', 'w')


def get_imdb_id(wiki_page_id):
    page = wiki.page(pageid=wiki_page_id, preload=False)
    imdb_links = [s for s in page.references if "http://www.imdb.com/title" in s]
    if len(imdb_links) > 1:
        print("Ambiguous imdb id! Need more complex analysis...")
        return None
    if len(imdb_links) == 0:
        print("No imdb link found =(")
        return None

    imdb_id = imdb_links[0].strip().strip("/").split("/")[-1]
    cache.write("{} | {}".format(wiki_page_id, imdb_id))
    return imdb_id


def fill_imdb_ids(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("""SELECT wiki_id FROM movie WHERE imdb_id is NULL""")
    rows = cursor.fetchall()
    wiki_ids = set()
    for r in rows:
        wiki_ids.add(r[0])

    for wiki_id in tqdm(wiki_ids):
        imdb_id = get_imdb_id(wiki_id)
        if imdb_id is None:
            continue
        cursor.execute("""UPDATE movie SET imdb_id = %s WHERE wiki_id = %s""",
                       (imdb_id, wiki_id))
        db_connection.commit()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = moviescope_db.connect_db(*moviescope_db.read_db_config(f))
    fill_imdb_ids(conn)

    cache.close()
