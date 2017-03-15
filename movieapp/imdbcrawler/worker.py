from django.utils.encoding import smart_text
from django.conf import settings
from ftplib import FTP
from itertools import izip
from workerthread import WorkerThread
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
				title = val['title'], 
				year = val['year'],
			) for key, val in movieList.iteritems() 
		]

		# delete, then re-add
		Movie.objects.all().delete()
		Movie.objects.bulk_create(movieInserts)
		logger.info("Created %s movie records" % Movie.objects.count())

		# same for genres
		genreInserts = [Genre(name = g) for g in genres]

		Genre.objects.all().delete()
		Genre.objects.bulk_create(genreInserts)
		logger.info("Created %s genre records" % Genre.objects.count())

		# persist movie-genre linking records
		self.persistMovieGenres(movieGenreList)

	def getChunks(self, l, n):
		for i in xrange(0, len(l), n):
			keys = l.keys()[i:i + n]
			vals = l.values()[i:i + n]
			yield dict(izip(keys, vals))

	def persistMovieGenres(self, movieGenreList):
		MovieGenre = Movie.genres.through
		MovieGenre.objects.all().delete()
		threads = []
		chunkSize =  len(movieGenreList) / 10
		for chunk in self.getChunks(movieGenreList, chunkSize):
			thread = WorkerThread(chunk)
			thread.start()
			threads.append(thread)
		
		for thread in threads:
			thread.join()

