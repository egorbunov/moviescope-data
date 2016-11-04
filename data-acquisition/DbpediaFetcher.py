from SPARQLWrapper import SPARQLWrapper, JSON
import itertools
import concurrent.futures as cf


class Movie:
    def __init__(self, wiki_id, title, abstract, year):
        self.wiki_id = wiki_id
        self.title = title
        self.abstract = abstract
        self.year = year


class Maker:
    """
    Movie maker (actor or ...) (now it is only actor
    """
    def __init__(self, name, wiki_id, about, films, role="actor"):
        self.name = name
        self.wiki_id = wiki_id
        self.about = about
        self.films = films
        self.role = role


class DbpediaFetcher:
    """
    Film stuff data fetcher from dbpedia
    """

    # query to get films for specific year
    Q_FILMS_FOR_YEAR = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX dct: <http://purl.org/dc/terms/>
        SELECT ?title ?abstract ?wikiPageId
        WHERE {{
          ?film rdf:type dbo:Film ;
                dbo:abstract ?abstract ;
                dbo:wikiPageID ?wikiPageId ;
                dct:subject <http://dbpedia.org/resource/Category:{0}_films> ;
                rdfs:label ?title .
          FILTER (LANG(?abstract)='en')
          FILTER (LANG(?title)='en')
        }}
    """

    # query to get actors with movies starring attached
    Q_ACTORS = """
        PREFIX dbpedia2: <http://dbpedia.org/property/>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        SELECT ?label ?actorWikiId ?about (GROUP_CONCAT(?filmWikiPageId;separator=" | ") as ?filmsWikiIds)
        WHERE {
            ?film dbpedia2:starring ?actor .
            ?film dbo:wikiPageID ?filmWikiPageId .
            ?film rdf:type dbo:Film .
            OPTIONAL {
                ?actor dbo:wikiPageID ?actorWikiId .
            }
            OPTIONAL {
                ?actor rdfs:comment ?about .
            }
            OPTIONAL {
                ?actor rdfs:label ?label .
            }
            FILTER (LANG(?about)='en') .
            FILTER (LANG(?label)='en')
        }
        GROUP BY ?label ?actorWikiId ?about
    """

    def __init__(self):
        self.sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        pass

    def _make_request(self, query):
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        response = self.sparql.query().convert()
        return response['results']['bindings']

    def _dbpedia_iterative_fetch(self, query, rec_per_query=500):
        """
        iteratively fetches all query eval. output
        this method is needed due to fixed max amount of
        records per query execution result on dbpedia
        """
        limited_query = query.replace("{", "{{").replace("}", "}}")
        limited_query += "\nLIMIT {0} OFFSET {1}"
        executor = cf.ThreadPoolExecutor(max_workers=1)
        future = None
        for offset in itertools.count(0, step=rec_per_query):
            q = limited_query.format(rec_per_query, offset)
            nq = limited_query.format(rec_per_query, offset + rec_per_query)
            records = future.result() if future else self._make_request(q)
            if len(records) == rec_per_query:
                future = executor.submit(DbpediaFetcher._make_request, self, nq)
            for r in records:
                yield r
            if len(records) < rec_per_query:
                break

    def get_films_for_year(self, year):
        gen = self._dbpedia_iterative_fetch(DbpediaFetcher.Q_FILMS_FOR_YEAR.format(year))
        for rec in gen:
            yield Movie(int(rec['wikiPageId']['value']),
                        str(rec['title']['value']),
                        str(rec['abstract']['value']),
                        year)

    def get_all_films(self):
        """
        fetches film data for all (hopefully) films for particular year
        film data contains:
            * title
            * abstract
            * year
            * actors
            * wikipedia page id
        :return movie data array
        """
        return itertools.chain(*[self.get_films_for_year(y) for y in range(1870, 2020)])

    def get_all_actors(self):
        gen = self._dbpedia_iterative_fetch(DbpediaFetcher.Q_ACTORS, rec_per_query=1000)
        for rec in gen:
            film_ids = [int(s.strip()) for s in rec['filmsWikiIds']['value'].split("|")]
            yield Maker(name=str(rec['label']['value']),
                        wiki_id=int(rec['actorWikiId']['value']),
                        about=str(rec['about']['value']),
                        films=film_ids,
                        role="actor")
