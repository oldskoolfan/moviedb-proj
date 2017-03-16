""" 
WorkerThread class - multithreading for inserting millions of M2M records
"""

import logging
import threading
from django_mysqlpool import auto_close_db
from imdbcrawler.models import Movie, Genre

LOGGER = logging.getLogger(__name__)

class WorkerThread(threading.Thread):
    """ thread class for persisting MovieGenres """

    def __init__(self, movieGenres):
        threading.Thread.__init__(self)
        self.movieGenres = movieGenres

    def run(self):
        self.persistMovieGenres()

    @auto_close_db
    def persistMovieGenres(self):
        """ get foreign keys we need, create MovieGenre objects, execute a single SQL insert """

        movieGenreInserts = []
        titles = [k.split(':')[0] for k in self.movieGenres.iterkeys()]
        MovieGenre = Movie.genres.through
        
        # map movie ids
        for m in Movie.objects.filter(title__in = titles):
            for g in Genre.objects.all():
                key = m.title + ':' + g.name
                if key in self.movieGenres:
                    movieGenreInserts.append(MovieGenre(movie_id=m.id, genre_id=g.id))

        # bulk insert
        MovieGenre.objects.bulk_create(movieGenreInserts)
        ct = len(movieGenreInserts)
        msg = 'Created %s movie_genre records in thread %s' % (ct, self.name)
        LOGGER.info(msg)
