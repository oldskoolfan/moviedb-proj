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

    title = models.CharField(max_length=1000, db_index=True)
    year = models.PositiveIntegerField()
    genres = models.ManyToManyField(Genre, db_table="movies_genres")
    