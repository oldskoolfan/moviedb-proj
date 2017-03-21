"""
manage.py command for parsing genres.list file
"""
import re
from itertools import izip
from pprint import pprint
from datetime import datetime
from imdbcrawler.fileworker import FileWorker
from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    """ our command class, extending django BaseCommand """

    def handle(self, *args, **options):
        """ create worker, parse list file """
        
        self.stdout.write(self.style.SUCCESS('start: %s' % datetime.now()))
        try:
            myWork = FileWorker()
            numberOfMovies = myWork.parseLists()
            self.stdout.write(self.style.SUCCESS('end: %s' % datetime.now()))
            msg = "Number of movies: %s" % numberOfMovies
            
            self.stdout.write(self.style.SUCCESS(msg))
        except Exception as e:
            raise CommandError(e)
