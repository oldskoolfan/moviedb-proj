"""
test Worker class
"""

from imdbcrawler.fileworker import FileWorker, ParseThread

def testParseList():
    """ test parsing logic, regex """

    fileworker = FileWorker()
    
    genreRows = [
    	fileworker.FILES[0]['startstr'],
        '"#Shelfie with Dan Hong" (2016)\tDocumentary',
        '"1002 Momentos de la tele" (2012)\t\tBiography',
        'A Whales Adventure (2009)\t\t\tAnimation',
        'Inception (2010)                   Sci-Fi',
    ]

    thread = ParseThread(fileworker.FILES[0])
    thread.parseList(genreRows)
    movies = thread.movies

    assert len(movies) == 4

    ratingRows = [
        fileworker.FILES[1]['startstr'],
        '      0000000133  1559055   8.8  Inception (2010)',
    ]

    thread = ParseThread(fileworker.FILES[1])
    thread.parseList(ratingRows)
    movies = thread.movies

    assert len(movies) == 1