"""
worker class - get genres.gz from imdb ftp, process into movie and genre tables
"""
import pdb
import os
import io
import re
import sys
import traceback
import threading
import logging
from Queue import Queue
from memory_profiler import profile
from django.utils.encoding import smart_text
from django.db import connection
from django_mysqlpool import auto_close_db
from imdbcrawler.models import Movie, Genre
from imdbcrawler.files import FILES
from imdbcrawler.filethread import FileThread

LOGGER = logging.getLogger(__name__)

class FileWorker(object):
    """ worker class """

    def __init__(self):
        pass

    @staticmethod
    def getFiles():
        """ get zip file from ftp and save to tmp dir """
        
        sizes = {}
        fileThreads = []        
        try:
            
            # clear tmp files, reopen
            for f in FILES:
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
                for f in FILES:
                    if f['zipname'] == name:
                        f['fileobj'] = t.file['fileobj']
                        sizes[name] = os.fstat(f['fileobj'].fileno()).st_size
                        break
        except:
            eType, eVal, tb = sys.exc_info()
            errMsg = traceback.format_exception(eType, eVal, tb)
            LOGGER.error(errMsg)
        finally:
            for f in FILES:
                if f['fileobj'] is not None:
                    f['fileobj'].close()
            return sizes

    @staticmethod
    def resetDatabaseTables():
        """ delete data from all database tables, reset auto_increment valus """

        with connection.cursor() as cursor:
            cursor.execute('delete from movies_genres')
            cursor.execute('alter table movies_genres auto_increment = 0')
            cursor.execute('delete from movie')
            cursor.execute('alter table movie auto_increment = 0')
            cursor.execute('delete from genre')
            cursor.execute('alter table genre auto_increment = 0')

    #@profile
    def parseLists(self):
        """ heavy lifting...parse a million-something records from text file """

        # threads = []
        # movieLists = []

        # for f in FILES:
        #     thread = ParseThread(f)
        #     thread.start()
        #     threads.append(thread)

        # for t in threads:
        #     t.join()
        #     movieLists.append(t.movies)            

        # movies = []
        # for k, v in movieLists[0].iteritems():
        #     if k in movieLists[1]:
        #         v['rating'] = movieLists[1][k]['rating']
        #         v['votes'] = movieLists[1][k]['votes']
        #         movies.append(v)

        # # save to database
        # self.persistMovies(movies)

        # reset db
        self.resetDatabaseTables()

        # first do genre file, 1000 rows at a time
        self.parseListFile(FILES[0])
        

        return Movie.objects.count()

    def parseListFile(self, listFile):
        """ parse list file line by line """
        
        movies = {}
        safeLine = u''
        foundStart = False
        cap = 1000

        with io.open(listFile['tmpname'], mode='rb') as text:
        
            # loop through each row in the listfile. every 1000 movies we accumulate,
            # save to db and purge the list
            for line in text:    
                safeLine = smart_text(line, errors='ignore')
                
                if not foundStart:
                    foundStart = safeLine.find(listFile['startstr']) >= 0
                    continue
                
                if len(movies) < cap:
                    movies = self.parseRow(movies, listFile['fields'], safeLine)                    
                else:
                    # save and purge
                    self.persistMovies(movies)
                    movies = {}

    def parseRow(self, movies, fields, safeLine):
        """ parse individual row in list file """

        movie = {}
        missing = False
                    
        for field in fields:
            match = re.search(field['regex'], safeLine, re.MULTILINE)
            missing = match is None
            
            if missing: 
                break    
            
            if field['type'] == 'string' or field['type'] == 'string[]':
                match = match.group(0).lower().strip().replace('"', '')
            elif field['type'] == 'int':
                match = int(match.group(0))
            elif field['type'] == 'float':
                match = float(match.group(0))

            if field['type'] != 'string[]':
                movie[field['name']] = match
            else:
                movie = self.addToOrInitList(field, movie, match)
        
        if not missing:
            title = movie['title']
            year = str(movie['year'])
            key = title + ':' + year
            if key in movies:
                for fKey, fVal in movie.iteritems():
                    if isinstance(fVal, list):
                        movies[key][fKey] += fVal
            else:
                movies[title + ':' + year] = movie

        return movies

    @staticmethod
    def addToOrInitList(field, movie, match):
        """ add to list or create new one """

        if field['name'] in movie and isinstance(movie[field['name']], list):
            movie[field['name']].append(match)
        else:
            movie[field['name']] = [match]

        return movie

    def persistMovies(self, movies):
        """ save movies to database """

        genres = Genre.objects.all()
        genreInserts = set()
        movieInserts = []
        movieGenreList = {}

        # create list of movie info
        for m in movies.itervalues():
            movieInserts.append(
                Movie(
                    title=m['title'], 
                    year=m['year'], 
                    rating=m['rating'] if 'rating' in m else None,
                    votes=m['votes'] if 'votes' in m else None,
                )
            )
            for g in m['genre']:
                movieGenreList[m['title'] + ':' + g] = m
                if not genres.filter(name=g).exists():
                    genreInserts.add(g)

        Movie.objects.bulk_create(movieInserts)
        LOGGER.info("Created %s movie records" % len(movieInserts))

        genreInserts = [Genre(name=g) for g in genreInserts]
        Genre.objects.bulk_create(genreInserts)
        LOGGER.info("Created %s genre records" % len(genreInserts))

        # persist movie-genre linking records
        MovieGenre = Movie.genres.through
        self.multithreadProcessList(WorkerThread, movieGenreList)

    @classmethod
    def multithreadProcessList(cls, threadCls, itemList, n=4):
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

        # make sure none already exist
        movieGenreInserts = [mg for mg in movieGenreInserts if not MovieGenre.objects.filter(movie_id=mg.movie_id, genre_id=mg.genre_id).exists()]

        # bulk insert
        MovieGenre.objects.bulk_create(movieGenreInserts)
        ct = len(movieGenreInserts)
        msg = 'Created %s movie_genre records in thread %s' % (ct, self.name)
        LOGGER.info(msg)

