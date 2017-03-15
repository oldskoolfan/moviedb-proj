import logging
import threading
import pdb
from django.db.models import Q
from django.core.exceptions import ObjectDoesNotExist
from django_mysqlpool import auto_close_db
from models import *

logger = logging.getLogger(__name__)

class WorkerThread(threading.Thread):
	def __init__(self, movieGenres):
		threading.Thread.__init__(self)
		self.movieGenres = movieGenres

	def run(self):
		self.persistMovieGenres()

	@auto_close_db
	def persistMovieGenres(self):
		movieGenreInserts = []
		titles = [k.split(':')[0] for k, v in self.movieGenres.iteritems()]
		MovieGenre = Movie.genres.through
		
		# map movie ids
		for m in Movie.objects.filter(title__in = titles):
			for g in Genre.objects.all():
				key = m.title + ':' + g.name
				if key in self.movieGenres:
					self.movieGenres[key]['movie_id'] = m.id
					self.movieGenres[key]['genre_id'] = g.id

		logger.info(self.movieGenres.values()[0])