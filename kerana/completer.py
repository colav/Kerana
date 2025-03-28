from elasticsearch import Elasticsearch
from pymongo import MongoClient
from elasticsearch.helpers import bulk

person_mapping = {"mappings": {"properties": {
    "full_name": {"type": "completion"}}}}


def get_person_weight(updated: list[dict], affiliations: list) -> int:
    """
    Calculate the weight of a person based on the source of the update and the presence of affiliations.
    Sources are:
    - staff
    - scienti
    - minciencias
    - others
    The weight is assigned as follows:
    - staff: 10
    - scienti or minciencias with affiliations: 6
    - scienti or minciencias without affiliations: 4
    - others with affiliations: 2
    - others without affiliations: 0
    Parameters:
    ------------
    updated:list
        List of sources that updated the person
    affiliations:list
        List of affiliations of the person
    Returns:
    ------------
    int
        Weight of the person
    """
    sources = {entry["source"] for entry in updated}
    if "staff" in sources:
        return 10
    elif "scienti" in sources or "minciencias" in sources:
        if affiliations:
            return 6
        else:
            return 4
    else:
        if affiliations:
            return 2
        else:
            return 0


def format_person_documents(index_name: str, docs: list) -> list:
    """
    Create a list of documents to be indexed in ElasticSearch.
    Each document contains a full name and its corresponding weight.
    The full name is a combination of first names and last names.
    The weight is determined by the presence of affiliations and the source of the update.
    Parameters:
    ------------
    index_name:str
        ElasticSearch index name
    docs:list
        List of documents to be indexed
    Returns:
    ------------
    list
        List of formatted documents ready for indexing
    """
    data = []
    for doc in docs:
        names = [doc["full_name"]]
        for name in doc["first_names"]:
            names.append(name + " " + " ".join(doc["last_names"]))
        names.append(" ".join(doc["last_names"]))
        if len(doc["first_names"]) > 0 and len(doc["last_names"]) > 0:
            names.append(f"{doc['last_names'][0]} {doc['first_names'][0]}")
            names.append(f"{doc['first_names'][0]} {doc['last_names'][0]}")
        names = list(set(names))
        data.append(
            {
                "_op_type": "index",
                "_index": index_name,
                "_id": str(doc["_id"]),  # Convertir ObjectId a string
                "_source": {
                    "full_name": {
                        "input": names,
                        "weight": get_person_weight(
                            doc["updated"], doc["affiliations"]
                        ),
                    },
                    "affiliations": doc.get("affiliations", []),
                    "products_count": doc.get("products_count", None),
                },
            }
        )
    return data


def person_completer_indexer(
    es: Elasticsearch,
    es_index: str,
    mdb_client: MongoClient,
    mdb_name: str,
    mdb_col: str,
    bulk_size: int = 100,
    reset_esindex: bool = True,
    request_timeout: int = 60,
) -> None:
    """
    Create an index for person name completion in ElasticSearch.

    Parameters:
    ------------
    es:ElasticSearch
        ElasticSearch client
    es_index:str
        ElasticSearch index name
    mdb_client:MongoClient
        MongoDB client
    mdb_name:str
        Mongo databse name
    mdb_col:str
        Mongo collection name
    bulk_size:int=10
        bulk cache size to insert document in ES.
    reset_esindex:bool
        reset de index before insert documents
    request_timeout:int
        request timeout for ElasticSearch
    """

    if reset_esindex:
        if es.indices.exists(index=es_index):
            es.indices.delete(index=es_index)

    if not es.indices.exists(index=es_index):
        es.indices.create(index=es_index, body=person_mapping)

    col_person = mdb_client[mdb_name][mdb_col]
    pipeline = [
        {
            "$project": {
                "full_name": 1,
                "first_names": 1,
                "last_names": 1,
                "updated": 1,
                "products_count": 1,
                "affiliations": {
                    "$filter": {
                        "input": "$affiliations",
                        "as": "affiliation",
                        "cond": {
                            "$not": {
                                "$gt": [
                                    {
                                        "$size": {
                                            "$filter": {
                                                "input": "$$affiliation.types",
                                                "as": "type",
                                                "cond": {
                                                    "$in": ["$$type.type", ["group", "department", "faculty"]]
                                                }
                                            }
                                        }
                                    },
                                    0
                                ]
                            }
                        }
                    }
                }
            }
        }
    ]

    cursor = col_person.aggregate(pipeline, allowDiskUse=True)

    batch = []
    for i, doc in enumerate(cursor, start=1):
        batch.append(doc)

        if i % bulk_size == 0:
            bulk(
                es,
                format_person_documents(es_index, batch),
                refresh=True,
                request_timeout=request_timeout,
            )
            print(f"Inserted {i} documents...")
            batch = []

    # Insert remaining documents in the last batch
    if batch:
        bulk(
            es,
            format_person_documents(es_index, batch),
            refresh=True,
            request_timeout=request_timeout,
        )
        print(f"Inserted {i} documents in total.")

    print("Process completed.")
