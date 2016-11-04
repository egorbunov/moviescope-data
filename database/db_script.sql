CREATE TABLE movie(
  id SERIAL PRIMARY KEY,
  wiki_id BIGINT UNIQUE NOT NULL,
  imdb_id BIGINT,
  abstract TEXT NOT NULL,
  year SMALLINT NOT NULL
);

CREATE TABLE review(
  id SERIAL PRIMARY KEY,
  movie_id INT NOT NULL references movie(id),
  summary TEXT,
  body TEXT NOT NULL,
  score SMALLINT
);
 
CREATE TABLE actors(
  id SERIAL PRIMARY KEY,
  movie_id INT NOT NULL references movie(id),
  name char(300) NOT NULL
);
  
  