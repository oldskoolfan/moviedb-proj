from imdbcrawler.worker import Worker

def testGetChunks():
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
	worker = Worker()
	i = 0
	for chunk in worker.getChunks(d, chunkSize):
		assert chunk == expected[i]
		i += 1