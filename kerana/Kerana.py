from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
from progress.bar import Bar
import sys

import json
from bson import ObjectId

from kerana.completer import person_completer_indexer, affiliations_completer_indexer


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)  # Convierte ObjectId a string
        # Llama al método predeterminado para otros tipos de datos
        return super().default(obj)


def mdb2es_dict(obj):
    return json.loads(json.dumps(obj, cls=CustomJSONEncoder))


class Kerana:
    def __init__(self, es_uri: str = "http://localhost:9200", es_basic_auth: tuple = ('elastic', 'colav'), mdb_uri: str = "mongodb://localhost:27017/"):
        self.es = Elasticsearch(es_uri, basic_auth=es_basic_auth)
        self.client = MongoClient(mdb_uri)

    def __del__(self):
        self.client.close()
        self.es.close()

    def completer(self, entity, mdb_name: str, mdb_col: str, es_index: str,
                  bulk_size: int = 100, reset_esindex: bool = True,
                  request_timeout: int = 60):
        """
        Create an index for person name completion in ElasticSearch.
        Parameters:
        ------------
        mdb_name:str
            Mongo database name
        mdb_col:str
            Mongo collection name
        es_index:str
            ElasticSearch index name
        bulk_size:int=100
            bulk cache size to insert document in ES.
        reset_esindex:bool
            reset the index before insert documents
        request_timeout:int
            request timeout for ElasticSearch
        """
        if entity == "person":
            person_completer_indexer(
                es=self.es,
                es_index=es_index,
                mdb_client=self.client,
                mdb_name=mdb_name,
                mdb_col=mdb_col,
                bulk_size=bulk_size,
                reset_esindex=reset_esindex,
                request_timeout=request_timeout
            )
        elif entity == "institution":
            affiliations_completer_indexer(
                affiliation_type=entity,
                es=self.es,
                es_index=es_index,
                mdb_client=self.client,
                mdb_name=mdb_name,
                mdb_col=mdb_col,
                bulk_size=bulk_size,
                reset_esindex=reset_esindex,
                request_timeout=request_timeout,
            )

    def mdb2es(self, mdb_name: str, mdb_col: str, es_index: str,
               bulk_size: int = 10, reset_esindex: bool = True,
               request_timeout: int = 60, total_fields_limit: int = 10000):
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
        reset_esindex:bool
            reset de index before insert documents
        total_fields_limit:int
            number of fields allowed by ES.
        """

        if reset_esindex:
            if self.es.indices.exists(index=es_index):
                self.es.indices.delete(index=es_index)

        if not self.es.indices.exists(index=es_index):
            self.es.indices.create(index=es_index)
        self.es.indices.put_settings(index=es_index, body={
            "index.mapping.total_fields.limit": total_fields_limit})
        data = self.client[mdb_name][mdb_col].find({})
        count = self.client[mdb_name][mdb_col].count_documents({})
        es_entries = []
        print(
            f"INFO: moving MongoDB {mdb_name}.{mdb_col} to ES index {es_index}, total documents = {count}")
        # we will insert using bulk operation, it is more efficient.
        with Bar('Loading', fill='@', suffix='%(percent).1f%% - %(eta)ds', max=count) as bar:
            for i in data:
                bar.next()
                _id = str(i['_id'])
                del i['_id']
                entry = {"_index": es_index,
                         "_id": _id,
                         "_source": mdb2es_dict(i)}

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
