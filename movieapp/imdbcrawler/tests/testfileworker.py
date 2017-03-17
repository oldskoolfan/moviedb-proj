"""
test Worker class
"""

from imdbcrawler.fileworker import FileWorker

def testGetChunks():
    """
    this tests the getChunks method on Worker
    """
    d = {
        'me:1': 1, 
        'me:2': 2, 
        'me:3': 3, 
        'me:4': 4,
        'me:5': 5,  
        'me:6': 6, 
        'me:7': 7, 
        'me:8': 8, 
        'me:9': 9, 
        'me:10': 10, 
    }
    
    expected = [
        {
            'me:1': 1, 
            'me:2': 2, 
            'me:3': 3, 
        },
        {
            'me:4': 4,
            'me:5': 5,  
            'me:6': 6, 
        },
        {
            'me:7': 7, 
            'me:8': 8, 
            'me:9': 9,
        },
        { 
            'me:10': 10,
        },
    ]

    chunkSize = len(d) / 3
    i = 0
    
    for chunk in FileWorker.getChunks(d, chunkSize):
        assert chunk == expected[i]
        i += 1
