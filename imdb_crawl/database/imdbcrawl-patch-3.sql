ALTER TABLE movie ADD COLUMN credits text[];
UPDATE movie SET credits = string_to_array(actors, ';');

ALTER TABLE movie ADD COLUMN rating real;
ALTER TABLE movie ADD COLUMN genres text[];
ALTER TABLE movie ADD COLUMN votes int;
ALTER TABLE movie ADD COLUMN year smallint;
ALTER TABLE movie ADD COLUMN type text;