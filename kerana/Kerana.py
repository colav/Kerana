from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from pymongo import MongoClient
import sys


class Kerana:
    def __init__(self, es_uri: str = "http://localhost:9200", es_basic_auth: tuple = ('elastic', 'colav'), mdb_uri: str = "mongodb://localhost:27017/"):
        self.es = Elasticsearch(es_uri, es_basic_auth)
        self.client = MongoClient(mdb_uri)

    def mdb2es(self, mdb_name: str, mdb_col: str, es_index: str, bulk_size: int = 10, reset_esindex: bool = True, request_timeout: int = 60):
        """
        MongoDb collection to ElasticSearch index.

        Parameters:
        ------------
        mdb_name:str
            Mongo databse name
        mdb_col:str
            Mongo collection name
        es_index:str
            ElasticSearch index name
        bulk_size:int=10
            bulk cache size to insert document in ES.
        reset_esindex:bool = True
            reset de index before insert documents
        """

        if reset_esindex:
            if self.es.indices.exists(index=es_index):
                self.es.indices.delete(index=es_index)

        if not self.es.indices.exists(index=es_index):
            self.es.indices.create(index=es_index)

        data = self.client[mdb_name][mdb_col].find({})
        es_entries = []

        # we will insert using bulk operation, it is more efficient.
        for i in data:
            _id = str(i['_id'])
            del i['_id']
            entry = {"_index": es_index,
                     "_id": _id,
                     "_source": i}

            es_entries.append(entry)
            if len(es_entries) == bulk_size:
                try:
                    helpers.bulk(self.es, es_entries, refresh=True,
                                 request_timeout=request_timeout)
                    es_entries = []
                except Exception as e:
                    # This can happen if the server is restarted or the connection becomes unavilable
                    print(str(e))
                    sys.exit(1)
        if len(es_entries) != 0:
            try:
                helpers.bulk(self.es, es_entries, refresh=True,
                             request_timeout=request_timeout)
                es_entries = []
            except Exception as e:
                # This can happen if the server is restarted or the connection becomes unavilable
                print(str(e))
                sys.exit(1)
