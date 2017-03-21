"""
models for moviedb
"""
from django.db import models

class Genre(models.Model):
    """
    Genre class
    """
    class Meta:
        db_table = 'genre'

    name = models.CharField(max_length=255)

class Movie(models.Model):
    """
    Movie class
    """
    class Meta:
        db_table = 'movie'

    title = models.CharField(max_length=767, db_index=True)
    year = models.PositiveIntegerField()
    rating = models.DecimalField(max_digits=3, decimal_places=1, null=True)
    votes = models.PositiveIntegerField(null=True)
    genres = models.ManyToManyField(Genre, db_table="movies_genres")
    