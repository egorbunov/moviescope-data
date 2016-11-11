CREATE TABLE movie(
  imdb_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  plot TEXT,
  poster_url TEXT,
  actors TEXT,
  date TEXT
);

CREATE TABLE review(
  movie_id TEXT NOT NULL references movie(imdb_id),
  summary TEXT,
  body TEXT,
  score SMALLINT
);

