"""
manage.py command for getting files via ftp
"""

from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from imdbcrawler.fileworker import FileWorker

class Command(BaseCommand):
    """ our command class, extending django BaseCommand """
    
    def handle(self, *args, **options):
        """ have file worker get files """

        # show start time
        self.stdout.write(self.style.SUCCESS('start: %s' % datetime.now()))

        try:
            worker = FileWorker()

            # get genre and rating files via ftp
            self.stdout.write(self.style.SUCCESS('getting files via ftp...'))
            sizes = worker.getFiles()
            for k, v in sizes.iteritems():
                self.stdout.write(self.style.SUCCESS('%s: %s' % (k, v)))

            # parse file rows into mysql records
            self.stdout.write(self.style.SUCCESS('parsing files into movie database...'))
            numberOfMovies = worker.parseLists()
            msg = "Number of movies: %s" % numberOfMovies
            self.stdout.write(self.style.SUCCESS(msg))

            # show end time
            self.stdout.write(self.style.SUCCESS('end: %s' % datetime.now()))
        except Exception as e:
            raise CommandError(e)