# coding: utf-8
import os, json, sys, pymongo

class ptt(object):
    """
    Process ptt data for sinica demo

    Usage
    =====
    >> from SinicaPTT import SinicaPTT
    >> ptt = SinicaPTT()
    >> ptt.insert(src='articles', dest='HatePolitics')
    """
    def __init__(self, **kwargs):

        ## db config
        mongo_addr = 'doraemon.iis.sinica.edu.tw' if 'mongo_addr' not in kwargs else kwargs['mongo_addr']
        mongo_db = 'ptt' if 'mongo_db' not in kwargs else kwargs['mongo_db']
        
        self.db = pymongo.Connection(mongo_addr)[mongo_db]

    def insert(self, src, dest):
        """
        Parameters
        ==========
        src: str
            the name of the source folder containing `.jso` files

        dest: str
            the destination collection name

        """
        ## source (folder)
        # src = 'articles'
        ## destination (mongodb)
        # dest = 'HatePolitics'

        co = self.db[dest]
        # filter out all files ending with '.jso'
        for fn in filter(lambda x:x.endswith('.jso'), os.listdir(src)):
            # read article from json files
            article = json.load(open(fn))

            # replace the field `_id` with `fn`
            # because `_id` is a special filed in mongodb
            article['fn'] = article['_id']
            del article['_id']

            # insert this document if it is a new one
            if not co.find_one({'fn': article['fn']}):
                co.insert(article)
                print 'insert', fn
            else:
                print 'skip', fn

        co.create_index('fn')


