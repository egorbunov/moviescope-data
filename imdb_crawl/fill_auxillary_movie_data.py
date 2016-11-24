from imdb_crawl_db import *
import sys


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = connect_db(*read_db_config(f))



    conn.close()

