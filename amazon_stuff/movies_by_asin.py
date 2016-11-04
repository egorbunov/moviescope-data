import sys
import getopt
import json
import bottlenose
from bs4 import BeautifulSoup
import traceback
import time
from tqdm import tqdm
from urllib.error import HTTPError

def print_usage():
    print("USAGE: python movies_by_asin.py -i <ASINS FILE> -o <OUT FILE> -k <AWS KEYS FILE> [-p <PREV RESULT FILE>]")
    print(" * -i <ASINS FILE>          path to file with amazon ids: each line interpreted as ID)")
    print(" * -o <OUT FILE>            path to output file with map [ ASIN to Movie data ]")
    print(" * -p <PREV RESULT FILE>    path to file with previous output from this script, so")
    print("                            in case if movie data is already queried for some ASIN")
    print("                            script won't make request to Amazon Product Api. That is")
    print("                            made due to total Amazon Api calls number is limited")
    print(" * -k <AWS KEYS FILE>       path to file with Amazon Product Adv Api keys")
    print("                            File must contain first 3 lines: access key, secret key, assoc. id")
    print("WARNING: script does calls to Amazon Product Adv. Api; It uses only one set of")
    print("         api keys; So use it carefully, because total call number is limited")

             
class Movie:
    def __init__(self, **kwargs):
        self.asin = kwargs['asin']
        self.title = kwargs['title']
        self.actors = kwargs['actors']
        self.director = kwargs['director']
        self.year = kwargs['year']
        self.genre = kwargs['genre']
        self.m_type = kwargs['m_type']
 
    def __eq__(self, other):
        return self.title == other.title and \
               self.year == other.year and \
               self.director == other.director   

    def __hash__(self):
        s = "{}{}{}".format(self.title, self.year, self.director)
        return hash(s)

    def as_dict(self):
        return self.__dict__

               
def get_movie_for_asin(asin, amazon):
    request_ok = False
    err_cnt = 0
    while not request_ok:
        try:
            xml_response = amazon.ItemLookup(ItemId=asin, ResponseGroup="ItemAttributes")
            request_ok = True
        except HTTPError as e:
            if e.code != 503 or e.code != 104 or err_cnt > 0:
                raise
            else:
                sleep_time = 5
                err_cnt += 1
                print("Got servise unavailable error," 
                      "sleeping for {} sec and then retry..".format(sleep_time))
                time.sleep(sleep_time)

    soup = BeautifulSoup(xml_response, 'lxml')
    items = soup.find_all('item')
    try:
        movie = next(i for i in items if i.asin.string == asin).itemattributes
    except StopIteration as e:
        print("Error retrieving movie for asin {}, no such item".format(asin))
        return None
     
    get_str = lambda tag: "" if movie.find(tag) is None else movie.find(tag).string

    m = {}
    m['asin'] = asin
    m['title'] = get_str('title')
    m['actors'] = [a.string for a in movie.find_all('actor')]
    m['director'] = get_str('director')
    m['year'] = get_str('releasedate')
    m['genre'] = get_str('genre')
    m['m_type'] = get_str("productgroup")
    
    return Movie(**m)

def read_movies(inp):
    movies_arr = json.load(inp)
    return [Movie(**m) for m in movies_arr]

def get_movies(asins, amazon_keys):
    (aws_k, aws_sk, aws_aid) = amazon_keys
    amazon = bottlenose.Amazon(aws_k, aws_sk, aws_aid)

    movies = set()
    try:
        for asin in tqdm(asins):
            retry = True
            while retry:
                time.sleep(0.7)
                try:
                    movie = get_movie_for_asin(asin, amazon)
                    retry = False
                except ConnectionResetError as e:
                    print("Got connection reset...retrying...")
                    amazon = bottlenose.Amazon(aws_k, aws_sk, aws_aid)
            if movie is not None:
                movies.add(movie)
    except (KeyboardInterrupt, SystemExit):
        print("Got keyboard interrupt or sys exit...finishing...")
    except Exception as e:
        print("ERROR: exception occured during film retrieving, finishing")
        traceback.print_exc()

    return movies


if __name__ == '__main__':
    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'i:o:p:k:')
    except getopt.GetoptError as err:
        print_usage()
        sys.exit(2)    
    
    optdict = dict(optlist)
    if '-i' not in optdict or '-o' not in optdict:
        print("Error: input or ouput files not provided")
        print_usage()
        sys.exit(2) 
    
    with open(optdict['-i'], 'r') as asin_in:
    	already_got_movies = set()
    	asins = set([s.strip() for s in asin_in.readlines()])
    
    print("Going to process {} asins".format(len(asins)))
    if '-p' in optdict:
        with open(optdict['-p']) as f:
            already_got_movies = set(read_movies(f))
            already_done = [ m.asin for m in already_got_movies ]
            print("Omitting {} already processed asins".format(len(already_done)))
            asins = set(asins).difference(set(already_done))
    else:
        print("WARNING: Previous results not passed, all ids will be proceed")

    with open(optdict['-k']) as f:
        amazon_keys = [s.strip() for s in f.readlines()[:3]]
    movies = get_movies(asins, amazon_keys)
    print("Merging previously processed movies with movies just processed...")
    movies = movies.union(already_got_movies)
    print("Got {} movie(s)!".format(len(movies)))
    print("Dumping to json and writing...")   
    json_movies = [ m.as_dict() for m in movies ]    

    movies_out = open(optdict['-o'], 'w')
    json.dump(json_movies, movies_out, indent=4)
    
    movies_out.close()
    print("Done")
