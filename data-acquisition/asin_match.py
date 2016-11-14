import moviescope_db
import sys
import json


def get_all_titles(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT wiki_id, title FROM movie")
    return dict([(row[0], row[1]) for row in cursor])


def read_asins_and_data(file):
    data_arr = json.load(file)
    return [(o['asin'], o['title']) for o in data_arr]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json> <ASIN_DATA.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = moviescope_db.connect_db(*moviescope_db.read_db_config(f))

    with open(sys.argv[2]) as asin_data:
        asins_data = read_asins_and_data(asin_data)

    known_titles_dict = get_all_titles(conn)
    known_titles = set(s.lower() for s in known_titles_dict.values())
    asin_match_cnt = 0
    for (asin, title) in asins_data:
        if title.lower() in known_titles:
            asin_match_cnt += 1

    print("Got {} asin raw matches.".format(asin_match_cnt))

