from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
import logging
from worker import Worker
from models import *

logger = logging.getLogger(__name__)

def index(request):
	return HttpResponse('<h1>Welcome</h1>')

def importData(request):
	myWorker = Worker()
	data = {}
	data['status'] = 0
	data['list'] = myWorker.getGenreList()
	return JsonResponse(data)

def parse(request):
	# myWorker = Worker()
	# content = myWorker.parseList()
	content = Movie.objects.all()
	return JsonResponse(content, safe=False)