import moviescope_db
import sys
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import re
from itertools import islice


def get_wiki_pages_for_movies(movies):
    PAGES_QUERY = "https://en.wikipedia.org/w/index.php?title=Special:Export&offset=1&limit=5&action=submit"
    r = requests.post(url=PAGES_QUERY,
                      data={'pages': "\n".join(movies),
                            'curonly': '1'},
                      headers={
                          'User-Agent': 'Movie Searcher Study Project (egor-mailbox@ya.ru)',
                      })

    xml = r.content
    soup = BeautifulSoup(xml, 'lxml')
    page_items = soup.find_all('page')
    return page_items


class MovieData:
    def __init__(self, wiki_id, plot, imdb_id, title):
        self.wiki_id = wiki_id
        self.plot = plot
        self.imdb_id = imdb_id
        self.title = title


def parse_movie_page(page_item):
    PLOT_PATTERN = re.compile("==[\s]*Plot[\s]*==|==[\s]*Synopsis[\s]*==|==[\s]*Plot[\s]*synopsis==[\s]*",
                              re.IGNORECASE)
    IMDB_PATTERN = re.compile(
        "\{[\s]*imdb[\s]*(?:title|name|movie)?[\s]*\|[\s]*(?:id[\s]*=[\s]*)?(?:tt)?([\d]+)",
        re.IGNORECASE)
    # not used for now
    IMDB_URL_PATTERN = re.compile("imdb.com/title", re.IGNORECASE)

    title = page_item.title.string
    wiki_id = page_item.id.string
    text = page_item.text
    split = re.split('(==[^=\n]+==)', text, )

    # retrieving plot
    plot_idxs = []
    plot = None
    for i, s in enumerate(split):
        if PLOT_PATTERN.match(s):
            plot_idxs.append(i + 1)
            break
    if len(plot_idxs) != 0:
        plot = "\n".join([split[i] for i in plot_idxs])

    # retrieve imdb
    imdb_id = None
    all_matches = IMDB_PATTERN.findall(text)
    if len(all_matches) == 1:
        imdb_id = all_matches[0]
        if not imdb_id.startswith("tt"):
            imdb_id = "tt{}".format(imdb_id)

    return MovieData(int(wiki_id), plot, imdb_id, title)


def batch_gen(iterator, batch_size):
    batch = islice(iterator, batch_size)
    while True:
        lst = list(batch)
        yield lst
        if len(lst) < batch_size:
            break
        batch = islice(iterator, batch_size)


def tqdm_timer(size, step):
    for i in tqdm(range(0, size + step, step)):
        yield i


def fill_movies_data(connection):
    cursor = connection.cursor()
    cursor.execute("""SELECT wiki_id, title, imdb_id  FROM movie""")
    imdb_set = set()

    rows = cursor.fetchall()
    movies = {}
    for r in rows:
        movies[r[0]] = r[1]
        imdb_set.add(r[2])

    page_id_no_imdb = open("imdb_not_found.txt", 'w')

    imdb_filled = 0
    plot_filled = 0
    batch_size = 100
    mit = iter(movies.items())

    progress_bar = tqdm_timer(len(movies), batch_size)
    for batch in batch_gen(mit, batch_size):
        movie_batch_dict = dict(batch)
        page_items = get_wiki_pages_for_movies([movie[1] for movie in batch])
        movies_data = (parse_movie_page(p) for p in page_items)
        for movie_data in movies_data:
            if movie_data.wiki_id not in movie_batch_dict:
                print("Strange! Got wrong page id for title = {}", movie_data.title)
                continue
            if movie_data.imdb_id is None:
                page_id_no_imdb.write("{} {}\n".format(movie_data.wiki_id, movie_data.title))

            cursor.execute("UPDATE movie SET imdb_id = %s, plot = %s WHERE wiki_id = %s",
                           (None if movie_data.imdb_id in imdb_set else movie_data.imdb_id,
                            movie_data.plot,
                            movie_data.wiki_id))
            connection.commit()

            if movie_data.imdb_id is not None:
                imdb_set.add(movie_data.imdb_id)
                imdb_filled += 1
            if movie_data.plot is not None:
                plot_filled += 1
        next(progress_bar)

    print("Only {} plots out of {} filled =(".format(plot_filled, len(movies)))
    print("Only {} imdb ids out of {} filled =(".format(imdb_filled, len(movies)))

    page_id_no_imdb.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = moviescope_db.connect_db(*moviescope_db.read_db_config(f))

    fill_movies_data(conn)

