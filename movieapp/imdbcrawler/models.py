from django.db import models

class Genre(models.Model):
	class Meta:
		db_table = 'genre'

	name = models.CharField(max_length=255)

class Movie(models.Model):
	class Meta:
		db_table = 'movie'

	title = models.TextField()
	year = models.PositiveIntegerField()
	genres = models.ManyToManyField(Genre, db_table="movies_genres")