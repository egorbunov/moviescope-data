import tqdm
from imdbpie import Imdb
import json
from requests.exceptions import HTTPError
from requests import Response
from tqdm import tqdm
import sys
import getopt
import traceback
import os

class ReviewedMovie:
	def __init__(self, imdb_id, title, date, actors, plots, poster_url, reviews):
		self.imdb_id = imdb_id
		self.title = title
		self.date = date
		self.actors = actors
		self.plots = plots
		self.poster_url = poster_url
		self.reviews = reviews

	def add_review(self, r):
		self.reviews.append(r)

	def add_reviews(self, rs):
		self.reviews.extend(rs)

	def as_dict(self):
		return self.__dict__


def proc_one_movie(imdb_id, imdb):
	max_reviews_num = 100
	m = imdb.get_title_by_id(imdb_id)
	if m is None:
		return None
	plots = imdb.get_title_plots(imdb_id) or []
	reviews = imdb.get_title_reviews(imdb_id, max_results=max_reviews_num) or []
	return ReviewedMovie(
		imdb_id, m.title, m.release_date, 
		[a.name for a in m.credits], plots, m.poster_url,
		[{'summary': r.summary, 'text': r.text, 'rating': r.rating} for r in reviews]
	)


def proc_movies(ids):
	imdb = Imdb(anonymize=False)
	movies = []
	for imdb_id in tqdm(ids):
		try:
			m = proc_one_movie(imdb_id, imdb)
			if m is None:
				break
			movies.append(m)
		except (KeyboardInterrupt, SystemExit):
			print("Got keyboard interrupt or sys exit...finishing...")
			break
		except HTTPError as e:
			traceback.print_exc()
			rsp = e.response
			if rsp.status_code == 404:
				continue
			else:
				return movies

	return movies


def read_prev_movies(prev_res_in):
	mjsons = json.load(prev_res_in)
	return [ReviewedMovie(
		m['imdb_id'], m['title'], m['date'], 
		m['actors'], m['plots'], m['poster_url'],
		[{'summary': r['summary'], 'text': r['text'], 'rating': r['rating'] } for r in m['reviews']])
	    for m in mjsons ]

if __name__ == "__main__":
	try:
		optlist, args = getopt.getopt(sys.argv[1:], 'f:t:o:p:')
	except getopt.GetoptError as err:
		print("Bad args!")
		sys.exit(2)    

	optdict = dict(optlist)
	if '-o' not in optdict or '-f' not in optdict or \
	   '-t' not in optdict:
		print("Bad args!!!")
		sys.exit(2)


	prev_mvs = []
	if '-p' in optdict:
		with open(optdict['-p'], 'r') as prev_res_in:
			prev_mvs = read_prev_movies(prev_res_in)


	already_done_ids = set([m.imdb_id for m in prev_mvs])

	print("Already got {} movies".format(len(already_done_ids)))

	fr = int(optdict['-f'])
	to = int(optdict['-t'])
	fname = str(optdict['-o'])

	todo_ids = set(["tt{:07}".format(i) for i in range(fr, to)]).difference(already_done_ids)

	with open(fname, 'w') as f_out:
		path, name = os.path.split(fname)
		movies = prev_mvs
		print("Going to retrieve {} movies...".format(len(todo_ids)))
		movies.extend(proc_movies(todo_ids))
		print("Got {} movies!".format(len(movies)))
		print("Dumping...")
		json.dump([m.as_dict() for m in movies], f_out, indent=4)
		print("Done!")

