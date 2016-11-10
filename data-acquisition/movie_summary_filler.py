import csv

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("USAGE: python p/t/script <DB_CONFIG_FILE.json> <MOVIE_SUMMARY_METADATA.tsv> <MOVIE_SUMMARIES.txt>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        conn = moviescope_db.connect_db(*moviescope_db.read_db_config(f))
    fill_imdb_ids(conn)

    cache.close()
