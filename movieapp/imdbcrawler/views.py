"""
main views
"""
import logging
from django.views.generic import TemplateView
from django.db.models import F
from django.shortcuts import render
from models import Movie

LOGGER = logging.getLogger(__name__)

class IndexView(TemplateView):
    """ home page view """
    
    templateName = 'index.html'

    def get(self, request):
        """ load home page template """
        badGenres = [
            'sex',
            'erotica',
            'hardcore',
            'adult',
            'short',
            'music',
            'musical',
            'experimental',
            'reality-tv',
        ]
        movies = Movie.objects.filter(rating__isnull=False).exclude(genres__name__in=badGenres).order_by(
            F('votes').desc(),
            F('rating').desc(), 
            'title', 
            F('year').desc(),
        )[:20]
        context = { 'movies': movies }
        return render(request, self.templateName, context)