"""
manage.py command for hitting omdbapi for rotten tomatoes data
"""

from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from imdbcrawler.httpworker import HttpWorker

class Command(BaseCommand):
    """ our command class, extending django BaseCommand """
    
    def handle(self, *args, **options):
        """ have http worker hit omdbapi for movies in db """

        self.stdout.write(self.style.SUCCESS('start: %s' % datetime.now()))
        try:
            worker = HttpWorker()
            worker.getMovieInfo()
            self.stdout.write(self.style.SUCCESS('end: %s' % datetime.now()))
        except Exception as e:
            raise CommandError(e)