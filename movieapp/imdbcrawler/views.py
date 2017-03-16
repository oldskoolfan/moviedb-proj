"""
main views
"""
import logging
from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from imdbcrawler.worker import Worker
from models import *

LOGGER = logging.getLogger(__name__)

def index():
    """ hello world """
    return HttpResponse('<h1>Welcome</h1>')
    