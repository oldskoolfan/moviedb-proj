"""
worker class - get genres.gz from imdb ftp, process into movie and genre tables
"""
import os
import io
import re
import sys
import traceback
import zlib
import logging
from ftplib import FTP
from itertools import izip
from django.utils.encoding import smart_text
from django.conf import settings
from imdbcrawler.baseworker import BaseWorker
from imdbcrawler.workerthread import WorkerThread
from imdbcrawler.models import Movie, Genre


LOGGER = logging.getLogger(__name__)

class FileWorker(BaseWorker):
    """ worker class """

    FTP_POST = 'ftp.fu-berlin.de'
    FTP_DIR = '/pub/misc/movies/database/'
    GENRE_FILENAME = 'genres.list.gz'
    TMP_NAME = settings.BASE_DIR + "/../tmp/tmpfile.list"
    TEST_NAME = settings.BASE_DIR + "/../tmp/test.list"
    START_STR = 'THE GENRES LIST'

    def getGenreList(self):
        """ get zip file from ftp and save to tmp dir """

        size = 0
        zipObj = zlib.decompressobj(zlib.MAX_WBITS | 16)
        tmpFile = None
        
        try:
            
            if os.path.isfile(self.TMP_NAME):
                os.remove(self.TMP_NAME)
            tmpFile = io.open(self.TMP_NAME, mode='w+b')
            ftp = FTP(self.FTP_POST)

            # login to ftp
            ftp.login()
            ftp.cwd(self.FTP_DIR)
            
            # get genres zip file, decompress and save
            retrCmd = 'RETR %s' % self.GENRE_FILENAME
            callback = lambda x: tmpFile.write(zipObj.decompress(x))
            ftp.retrbinary(retrCmd, callback)
            
            size = os.fstat(tmpFile.fileno()).st_size
        except:
            eType, eVal, tb = sys.exc_info()
            errMsg = traceback.format_exception(eType, eVal, tb)
            LOGGER.error(errMsg)
        finally:
            if tmpFile != None:
                tmpFile.close()
            return size

    def parseList(self):
        """ heavy lifting...parse a million-something records from text file """

        safeLine = u''
        movies = []
        foundStart = False

        # parse text file
        with io.open(self.TMP_NAME, mode='rb') as listFile:
            for line in listFile:
                movie = {}
                safeLine = smart_text(line, errors='ignore')
                if not foundStart:
                    foundStart = safeLine.find(self.START_STR) >= 0
                if foundStart:
                    titleMatch = re.search(r'(.*)(?=\([0-9]+\))', safeLine)
                    genreMatch = re.search(r'(?<=\t)([^\s]+)$', safeLine)
                    yearMatch = re.search(r'(?<=\()([0-9]+)(?=\))', safeLine)
                    if titleMatch is None or genreMatch is None or yearMatch is None:
                        continue
                    title = titleMatch.group(0).lower().strip().replace('"', '')
                    movie['title'] = title
                    movie['genre'] = genreMatch.group(0).lower()
                    movie['year'] = int(yearMatch.group(0))
                    movies.append(movie)
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
            Movie(title=val['title'], year=val['year']) for val in movieList.itervalues()
        ]

        # delete, then re-add
        Movie.objects.all().delete()
        Movie.objects.bulk_create(movieInserts)
        logger.info("Created %s movie records" % Movie.objects.count())

        # same for genres
        genreInserts = [Genre(name=g) for g in genres]

        Genre.objects.all().delete()
        Genre.objects.bulk_create(genreInserts)
        logger.info("Created %s genre records" % Genre.objects.count())

        # persist movie-genre linking records
        self.persistMovieGenres(movieGenreList)

    def persistMovieGenres(self, movieGenreList):
        """ open up to 10 threads to process the MovieGenre records """

        MovieGenre = Movie.genres.through
        MovieGenre.objects.all().delete()
        threads = []
        chunkSize = len(movieGenreList) / 10
        for chunk in self.getChunks(movieGenreList, chunkSize):
            thread = WorkerThread(chunk)
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()

