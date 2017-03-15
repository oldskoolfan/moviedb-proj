from django.core.management.base import BaseCommand, CommandError
import logging
import pdb
from itertools import izip

logger = logging.getLogger('imdbcrawler')

class Command(BaseCommand):
	def handle(self, *args, **options):
		d = {
			'me:1': 1, 
			'me:2': 2, 
			'me:3': 3, 
			'me:4': 4,
			'me:5': 5,  
			'me:6': 6, 
			'me:7': 7, 
			'me:8': 8, 
			'me:9': 9, 
			'me:10': 10, 
		}
		chunkSize = len(d) / 3
		for chunk in self.getChunks(d, chunkSize):
			logger.info(chunk)

	# n = chunk size
	def getChunks(self, l, n):
		for i in xrange(0, len(l), n):
			keys = l.keys()[i:i + n]
			vals = l.values()[i:i + n]
			yield dict(izip(keys, vals))

	def msg(self, msg):
		self.stdout.write(self.style.SUCCESS(msg))
