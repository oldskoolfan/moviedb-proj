"""
test Worker class
"""

from imdbcrawler.fileworker import FileWorker, ParseThread

def testFieldPatterns():
    fileworker = FileWorker()
    thread = ParseThread(fileworker.FILES[0])

    genreRows = [
    	fileworker.FILES[0]['startstr'],
        '"#Shelfie with Dan Hong" (2016)\tDocumentary',
        '"1002 Momentos de la tele" (2012)\t\tBiography',
        'A Whales Adventure (2009)\t\t\tAnimation',
    ]

    thread.parseList(genreRows)
    movies = thread.movies

    assert len(movies) == 3