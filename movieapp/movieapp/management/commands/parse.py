"""
manage.py command for parsing genres.list file
"""
from datetime import datetime
from imdbcrawler.worker import Worker
from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    """ our command class, extending django BaseCommand """
    
    def handle(self, *args, **options):
        """ create worker, parse list file """
        
        self.stdout.write(self.style.SUCCESS('start: %s' % datetime.now()))
        try:
            myWork = Worker()
            numberOfMovies = myWork.parseList()
            self.stdout.write(self.style.SUCCESS('end: %s' % datetime.now()))
            msg = "Number of movies: %s" % numberOfMovies
            self.stdout.write(self.style.SUCCESS(msg))
        except Exception as e:
            raise CommandError(e)
            