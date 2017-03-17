""" base worker class """

from itertools import izip

class BaseWorker(object):
    """ base class for other workers to inherit """

    def __init__(self):
        pass

    @staticmethod
    def getChunks(l, n):
        """ break dict into n chunks """

        for i in xrange(0, len(l), n):
            if isinstance(l, dict):
                keys = l.keys()[i:i + n]
                vals = l.values()[i:i + n]
                yield dict(izip(keys, vals))
            else:
                yield l[i:i + n]
