# -*- coding: utf-8 -*-
# Generated by Django 1.10.6 on 2017-03-14 18:06
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('imdbcrawler', '0002_auto_20170314_1719'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='genre',
            name='genres',
        ),
        migrations.AddField(
            model_name='movie',
            name='genres',
            field=models.ManyToManyField(db_table=b'movies_genres', to='imdbcrawler.Genre'),
        ),
    ]
