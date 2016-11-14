CREATE TABLE search(
  movie_id TEXT NOT NULL references movie(imdb_id),
  plot TEXT,
  reviews TEXT,
  preprocessed_plot TEXT,
  preprocessed_reviews TEXT
);
