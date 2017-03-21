"""
worker class - get genres.gz from imdb ftp, process into movie and genre tables
"""
import pdb
import os
import io
import re
import sys
import traceback
import zlib
import threading
import logging
import requests
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError
from Queue import Queue
from ftplib import FTP
from itertools import izip
from django.db import transaction
from django.utils.encoding import smart_text
from django.conf import settings
from django_mysqlpool import auto_close_db
from imdbcrawler.baseworker import BaseWorker
from imdbcrawler.models import Movie, Genre

LOGGER = logging.getLogger(__name__)

class FileWorker(BaseWorker):
    """ worker class """

    FILES = [
        {
            'startstr': 'THE GENRES LIST',
            'zipname': 'genres.list.gz',
            'tmpname': settings.PROJ_DIR + '/tmp/genrefile.list',
            'fileobj': None,
            'fields': [
                {
                    'name': 'title',
                    'type': 'string',
                    'regex': r'(.*)(?=\([0-9]+\))',
                },
                {
                    'name': 'genre',
                    'type': 'string',
                    'regex': r'(?<=\t)([^\s]+)$',
                },
                {
                    'name': 'year',
                    'type': 'int',
                    'regex': r'(?<=\()([0-9]+)(?=\))',
                },
            ],
        },
        {
            'startstr': 'MOVIE RATINGS REPORT',
            'zipname': 'ratings.list.gz',
            'tmpname': settings.PROJ_DIR + '/tmp/ratingfile.list',
            'file': None,
            'fields': [
                {
                    'name': 'title',
                    'type': 'string',
                    'regex': r'(?<=[0-9]\.[0-9]\s\s)(.+)(?=\([0-9]+\))',
                },
                {
                    'name': 'rating',
                    'type': 'float',
                    'regex': r'(?<=[0-9]\s\s\s)([0-9]+\.[0-9])',
                },
                {
                    'name': 'year',
                    'type': 'int',
                    'regex': r'(?<=\()([0-9]+)(?=\))',
                },
            ],
        },
    ]

    def getFiles(self):
        """ get zip file from ftp and save to tmp dir """
        
        sizes = {}
        fileThreads = []        
        try:
            
            # clear tmp files, reopen
            for f in self.FILES:
                tmp = f['tmpname']
                if os.path.isfile(tmp):
                    os.remove(tmp)
                f['fileobj'] = io.open(tmp, mode='w+b')
            
            # get zip file, decompress and save
                thread = FileThread(f)
                thread.start()
                fileThreads.append(thread)

            for t in fileThreads:
                t.join()
                name = t.file['zipname']
                for f in self.FILES:
                    if f['zipname'] == name:
                        f['fileobj'] = t.file['fileobj']
                        sizes[name] = os.fstat(f['fileobj'].fileno()).st_size
                        break
        except:
            eType, eVal, tb = sys.exc_info()
            errMsg = traceback.format_exception(eType, eVal, tb)
            LOGGER.error(errMsg)
        finally:
            for f in self.FILES:
                if f['fileobj'] is not None:
                    f['fileobj'].close()
            return sizes

    def parseLists(self):
        """ heavy lifting...parse a million-something records from text file """

        threads = []
        movieLists = []

        for f in self.FILES:
            thread = ParseThread(f)
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()
            movieLists.append(t.movies)            

        movies = []
        for k, v in movieLists[0].iteritems():
            if k in movieLists[1]:
                v['rating'] = movieLists[1][k]['rating']
                movies.append(v)

        # save to database
        self.persistMovies(movies)

        return Movie.objects.count()

    def persistMovies(self, movies):
        """ save movies to database """

        genres = set()
        movieList = {}
        movieGenreList = {}

        # create list of movie info
        for m in movies:
            movieList[m['title'] + str(m['year'])] = m
            movieGenreList[m['title'] + ':' + m['genre']] = m
            genres.add(m['genre'])

        # convert to list of movie objects
        movieInserts = [
            Movie(
                title=val['title'], 
                year=val['year'], 
                rating=val['rating'],
            ) for val in movieList.itervalues()
        ]

        # delete then re-add
        Movie.objects.all().delete()
        Movie.objects.bulk_create(movieInserts)
        LOGGER.info("Created %s movie records" % Movie.objects.count())

        # same for genres
        genreInserts = [Genre(name=g) for g in genres]

        Genre.objects.all().delete()
        Genre.objects.bulk_create(genreInserts)
        LOGGER.info("Created %s genre records" % Genre.objects.count())

        # persist movie-genre linking records
        MovieGenre = Movie.genres.through
        MovieGenre.objects.all().delete()
        self.multithreadProcessList(WorkerThread, movieGenreList)

    @classmethod
    def multithreadProcessList(cls, threadCls, itemList, n=20):
        """ open up to 20 threads to process the MovieGenre records """

        threads = []
        cls.queue = Queue(maxsize=len(itemList))
        cls.lock = threading.Lock()
        cls.exit = False
        
        # create queue
        if isinstance(itemList, dict):
            for k, v in itemList.iteritems():
                cls.queue.put(item=(k, v), block=False)
        else:
            for item in itemList:
                cls.queue.put(item=item, block=False)

        # create threads
        for i in xrange(n):
            thread = threadCls()
            thread.start()
            threads.append(thread)

        while not cls.queue.empty():
            pass

        cls.exit = True
        
        for thread in threads:
            thread.join()

class ParseThread(threading.Thread):
    """ thread class for parsing imdb list file """

    def __init__(self, listFile):
        threading.Thread.__init__(self)
        self.listFile = listFile
        self.movies = {}

    def run(self):
        self.parseList()

    def parseList(self):
        """ heavy lifting...parse a million-something records from text file """

        safeLine = u''
        foundStart = False

        # parse genre text file
        with io.open(self.listFile['tmpname'], mode='rb') as text:
            for line in text:
                movie = {}
                safeLine = smart_text(line, errors='ignore')
                if not foundStart:
                    foundStart = safeLine.find(self.listFile['startstr']) >= 0
                if foundStart:
                    missing = False
                    for field in self.listFile['fields']:
                        match = re.search(field['regex'], safeLine)
                        if missing: 
                            continue
                        missing = match is None
                        if not missing:
                            if field['type'] == 'string':
                                match = match.group(0).lower().strip().replace('"', '')
                            elif field['type'] == 'int':
                                match = int(match.group(0))
                            elif field['type'] == 'float':
                                match = float(match.group(0))
                            movie[field['name']] = match
                    if not missing:
                        title = movie['title']
                        year = str(movie['year'])
                        self.movies[title + ':' + year] = movie

class FileThread(threading.Thread):
    """ thread class for getting data files """

    FTP_POST = 'ftp.fu-berlin.de'
    FTP_DIR = '/pub/misc/movies/database/'

    def __init__(self, f):
        threading.Thread.__init__(self)
        self.file = f
        self.ftp = FTP(self.FTP_POST)
        self.zipObj = zlib.decompressobj(zlib.MAX_WBITS | 16)

        # login to ftp
        self.ftp.login()
        self.ftp.cwd(self.FTP_DIR)

    def run(self):
        retrCmd = 'RETR %s' % self.file['zipname']
        callback = lambda x: self.file['fileobj'].write(self.zipObj.decompress(x))
        self.ftp.retrbinary(retrCmd, callback)


class WorkerThread(threading.Thread):
    """ thread class for persisting MovieGenres """

    def run(self):
        self.persistMovieGenres()

    @auto_close_db
    def persistMovieGenres(self):
        """ get foreign keys we need, create MovieGenre objects, execute a single SQL insert """

        movieGenreInserts = []
        movieGenreDict = {}
        
        # burn through the queue
        while not FileWorker.exit:
            FileWorker.lock.acquire()
            if not FileWorker.queue.empty():
                item = FileWorker.queue.get()
                movieGenreDict[item[0]] = item[1]
            FileWorker.lock.release()

        titles = [k.split(':')[0] for k in movieGenreDict.iterkeys()]
        MovieGenre = Movie.genres.through
        
        # map movie ids
        genres = Genre.objects.all()
        for m in Movie.objects.filter(title__in = titles):
            for g in genres:
                key = m.title + ':' + g.name
                if key in movieGenreDict:
                    movieGenreInserts.append(MovieGenre(movie_id=m.id, genre_id=g.id))

        # bulk insert
        MovieGenre.objects.bulk_create(movieGenreInserts)
        ct = len(movieGenreInserts)
        msg = 'Created %s movie_genre records in thread %s' % (ct, self.name)
        LOGGER.info(msg)

