""" 
imdb ftp file config 
"""

from django.conf import settings

FILES = [
    {
        'startstr': 'THE GENRES LIST',
        'zipname': 'genres.list.gz',
        'tmpname': settings.PROJ_DIR + '/tmp/genrefile.list',
        'fileobj': None,
        'fields': [
            {
                'name': 'title',
                'type': 'string',
                'regex': r'(.*)(?=\([0-9]+\))',
            },
            {
                'name': 'genre',
                'type': 'string[]',
                'regex': r'(?<=[\)\}])([^\(\)\{\}]+)$',
            },
            {
                'name': 'year',
                'type': 'int',
                'regex': r'(?<=\()([0-9]+)(?=\))',
            },
        ],
    },
    {
        'startstr': 'MOVIE RATINGS REPORT',
        'zipname': 'ratings.list.gz',
        'tmpname': settings.PROJ_DIR + '/tmp/ratingfile.list',
        'file': None,
        'fields': [
            {
                'name': 'title',
                'type': 'string',
                'regex': r'(?<=[0-9]\.[0-9]\s\s)(.+)(?=\([0-9]+\))',
            },
            {
                'name': 'rating',
                'type': 'float',
                'regex': r'(?<=[0-9]\s\s\s)([0-9]+\.[0-9])',
            },
            {
                'name': 'votes',
                'type': 'int',
                'regex': r'[0-9]+(?=\s\s[0-9\s][0-9]\.[0-9])',
            },
            {
                'name': 'year',
                'type': 'int',
                'regex': r'(?<=\()([0-9]+)(?=\))',
            },
        ],
    },
]