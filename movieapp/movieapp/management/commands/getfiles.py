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

        self.stdout.write(self.style.SUCCESS('start: %s' % datetime.now()))
        try:
            worker = FileWorker()
            sizes = worker.getFiles()
            for k, v in sizes.iteritems():
                self.stdout.write(self.style.SUCCESS('%s: %s' % (k, v)))
            self.stdout.write(self.style.SUCCESS('end: %s' % datetime.now()))
        except Exception as e:
            raise CommandError(e)