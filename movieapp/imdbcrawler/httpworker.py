"""
worker for hitting omdbapi
"""

import pdb
import math
import Queue
import threading
import logging
import requests
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError
from django.db import transaction
from imdbcrawler.baseworker import BaseWorker
from imdbcrawler.models import Movie

LOGGER = logging.getLogger(__name__)

class HttpThread(threading.Thread):
    """ thread class for making http requests """
    
    API_URL = 'http://www.omdbapi.com/?'

    def __init__(self, movies):
        threading.Thread.__init__(self)
        self.movies = movies
        self.queue = Queue.Queue(len(movies))

    @transaction.atomic
    def run(self):
        for m in self.movies:
            self.queue.put(m)
        while not self.queue.empty():
            self.getMovieRating(self.queue.get()) 

    def getMovieRating(self, movie):
        """ call omdbapi to get movie rating info """

        data = {'t': movie.title}
        try:
            req = requests.get(self.API_URL, params=data, timeout=1)
            req.raise_for_status()
            resp = req.json()
            if resp['Response'] == 'True':
                # pdb.set_trace()
                movie.rating = float(resp['imdbRating'])
                movie.save()
                logMsg = '%s is rated %.1f out of 10' % (movie.title, movie.rating)
                LOGGER.info(logMsg)
        except (ReadTimeout, ConnectionError, HTTPError, ValueError) as e:
            LOGGER.error(e)

class HttpWorker(BaseWorker):
    """ http worker class """    

    def getMovieInfo(self):
        """ for each movie, hit the api for rating info """
        
        # break into chunks
        chunkSize = math.ceil(Movie.objects.count() / 10)

        threads = []
        for chunk in self.getChunks(Movie.objects.all(), chunkSize):
            thread = HttpThread(chunk)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
