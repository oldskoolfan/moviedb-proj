from django.utils.encoding import smart_text
from django.db import IntegrityError
from django.db.models import Q
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings
from django_mysqlpool import auto_close_db
from ftplib import FTP
from models import *
import os
import io
import re
import sys
import traceback
import zlib
import logging
import threading
import pdb

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
		MovieGenre = Movie.genres.through
		MovieGenre.objects.all().delete()
		for m in self.movieGenres:
			try:
				movieId = Movie.objects.get(Q(title=m['title']), Q(year=m['year'])).id
				genreId = Genre.objects.get(name=m['genre']).id
				movieGenreInserts.append(MovieGenre(movie_id=movieId, genre_id=genreId))
			except ObjectDoesNotExist:
				logger.error('could not save movie-genre record for ' + m['title'])
		MovieGenre.objects.bulk_create(movieGenreInserts)
		logger.info('Created %s MovieGenre records in thread %s' % (MovieGenre.objects.count(), self.name))

class Worker:
	GENRE_FILENAME = 'genres.list.gz'
	TMP_NAME = settings.BASE_DIR + "/../tmp/tmpfile.list"
	TEST_NAME = settings.BASE_DIR + "/../tmp/test.list"
	START_STR = 'THE GENRES LIST'

	def getGenreList(self):
		size = 0
		zipObj = zlib.decompressobj(zlib.MAX_WBITS | 16)
		tmpFile = None
		try:
			
			if os.path.isfile(self.TMP_NAME):
				os.remove(self.TMP_NAME)
			tmpFile = io.open(self.TMP_NAME, mode = 'w+b')
			ftp = FTP('ftp.fu-berlin.de')

			# login to ftp
			ftp.login()
			ftp.cwd('/pub/misc/movies/database/')
			
			# get genres zip file, decompress and save
			retrCmd = 'RETR %s' % self.GENRE_FILENAME
			callback = lambda x: tmpFile.write(zipObj.decompress(x))
			ftp.retrbinary(retrCmd, callback)
			
			size = os.fstat(tmpFile.fileno()).st_size
		except:
			eType, eVal, tb = sys.exc_info()
			errMsg = traceback.format_exception(eType, eVal, tb)
			logger.error(errMsg)
		finally:
			if tmpFile != None:
				tmpFile.close()
			return size

	def parseList(self):
		safeLine = u''
		movies = []
		foundStart = False

		# parse text file
		with io.open(self.TMP_NAME, mode='rb') as listFile:
			for line in listFile:
				movie = {}
				safeLine = smart_text(line, errors = 'ignore')
				if not foundStart:
					foundStart = safeLine.find(self.START_STR) >= 0
				if foundStart:
					titleMatch = re.search(r'(.*)(?=\([0-9]+\))', safeLine)
					genreMatch = re.search(r'(?<=\t)([^\s]+)$', safeLine)
					yearMatch = re.search(r'(?<=\()([0-9]+)(?=\))', safeLine)
					if titleMatch == None or genreMatch == None or yearMatch == None:
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
		movieInserts = []
		movieList = {}
		genreInserts = []
		genres = set()
		movieGenreList = {}

		# create list of movie info
		for m in movies:
			movieList[m['title'] + str(m['year'])] = m
			movieGenreList[m['title'] + m['genre']] = m
			genres.add(m['genre'])

		# convert to list of movie objects
		for key, val in movieList.iteritems():
			movieInserts.append(Movie(title = val['title'], year = val['year'])) 

		# delete, then re-add
		Movie.objects.all().delete()
		Movie.objects.bulk_create(movieInserts)
		logger.info("Created %s movie records" % Movie.objects.count())

		# same for genres
		for g in genres:
			genreInserts.append(Genre(name = g))

		Genre.objects.all().delete()
		Genre.objects.bulk_create(genreInserts)
		logger.info("Created %s genre records" % Genre.objects.count())

		# persist movie-genre linking records
		self.persistMovieGenres(movieGenreList.values())

	def getChunks(self, l, n):
		for i in xrange(0, len(l), n):
			intI = int(i)
			yield l[i:i + n]

	def persistMovieGenres(self, movieGenreList):
		threads = []
		chunkSize =  len(movieGenreList) / 10
		chunks = self.getChunks(movieGenreList, chunkSize)
		for chunk in chunks:
			thread = WorkerThread(chunk)
			thread.start()
			threads.append(thread)
		
		for thread in threads:
			thread.join()

