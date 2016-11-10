CREATE TABLE movie(
  wiki_id BIGINT PRIMARY KEY,
  imdb_id TEXT UNIQUE,
  title TEXT NOT NULL,
  abstract TEXT NOT NULL,
  year SMALLINT NOT NULL,
  synopsys TEXT
);

CREATE TABLE review(
  id SERIAL PRIMARY KEY,
  movie_id BIGINT NOT NULL references movie(wiki_id),
  summary TEXT,
  body TEXT NOT NULL,
  score SMALLINT
);

-- movie maker roles
CREATE TYPE role AS ENUM ('actor', 'director');
 
-- movie maker description
CREATE TABLE maker(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  wiki_id BIGINT,
  about TEXT
);

-- movie maker participation
CREATE TABLE participation(
  maker_id INT NOT NULL references maker(id),
  movie_id BIGINT NOT NULL references movie(wiki_id),
  p_role role
);
