from elasticsearch import Elasticsearch
from pymongo import MongoClient
from elasticsearch.helpers import bulk
from unidecode import unidecode


person_mapping = {"mappings": {"properties": {
    "full_name": {"type": "completion"}}}}

affiliations_mapping = {"mappings": {
    "properties": {"name": {"type": "completion"}}}}


def get_affiliations_weight(affiliation_type: str, addresses: list[dict]) -> int:
    """
    Calculate the weight of an affiliation based on its type and the presence of addresses.
    The weight is assigned as follows:
    - institution: 10 if country_code is CO, otherwise 0

    Parameters:
    ------------
    affiliation_type:str
        Type of the affiliation, options are:
        - institution
        - group
        - department
        - faculty
    addresses:list
        List of addresses of the affiliation
    Returns:
    ------------
    int
        Weight of the affiliation
    """
    if affiliation_type == "institution":
        sources = {entry["country_code"] for entry in addresses}
        if "CO" in sources:
            return 10
        else:
            return 0
    return 0


def format_affiliations_documents(index_name: str, docs: list, affiliation_type: str) -> list:
    """
    Create a list of documents to be indexed in ElasticSearch.
    Each document contains a name and its corresponding weight.

    Parameters:
    ------------
    docs:list
        List of documents to be indexed
    index_name:str
        ElasticSearch index name
    affiliation_type:str
        Type of the affiliation, options are:
        - institution
        - group
        - department
        - faculty
    Returns:
    ------------
    list
        List of formatted documents ready for indexing
    """
    data = []
    connectors = {}
    connectors["es"] = [
        " de ",
        " De ",
        " del ",
        " Del ",
        " la ",
        " La ",
        " las ",
        " Las ",
    ]
    connectors["en"] = [" of ", " Of ", " the ", " The "]
    for doc in docs:
        names = []
        full_name = ""
        for name in doc["names"]:
            names.append(name["name"])
            names.append(unidecode(name["name"]))
            _name = name["name"]
            for con in connectors[name["lang"]]:
                _name = _name.replace(con, " ")
            names = list(set(names))
            _sname = _name.split()
            _suname = unidecode(_name).split()
            names.extend(_sname)
            names.extend(_suname)
            for i in range(len(_sname)):
                names.append(" ".join(_sname[i:]))
                names.append(" ".join(_suname[i:]))
            if name["lang"] == "es":
                full_name = name["name"]
        if "abbreviations" in doc:
            names.extend(doc["abbreviations"])
        country_code = ""
        if "addresses" in doc:
            for address in doc["addresses"]:
                if "country_code" in address:
                    country_code = address["country_code"]
        names = list(set(names))
        rec = {
            "_op_type": "index",
            "_index": index_name,
            "_id": str(doc["_id"]),  # Convertir ObjectId a string
            "_source": {
                "name": {"input": names},  # Estructura para 'completion'
                "types": doc["types"],
                "full_name": full_name,
            },
        }
        if affiliation_type == "institution":
            rec["_source"]["name"]["weight"] = get_affiliations_weight(
                affiliation_type, doc["addresses"]
            )
            rec["_source"]["country_code"] = country_code
        if affiliation_type in ["group", "faculty", "department"]:
            rec["_source"]["relations"] = doc["relations"]
        data.append(rec)
    return data


def affiliations_completer_indexer(
    affiliation_type: str,
    es: Elasticsearch,
    es_index: str,
    mdb_client: MongoClient,
    mdb_name: str,
    mdb_col: str,
    bulk_size: int = 100,
    reset_esindex: bool = True,
    request_timeout: int = 60,
) -> None:
    if reset_esindex:
        if es.indices.exists(index=es_index):
            es.indices.delete(index=es_index)

    if not es.indices.exists(index=es_index):
        es.indices.create(index=es_index, body=affiliations_mapping)

    col_aff = mdb_client[mdb_name][mdb_col]
    pipeline = []
    if affiliation_type == "institution":
        pipeline = [
            {"$match": {"types.type": {
                "$nin": ["group", "faculty", "department"]}}},
            {
                "$project": {
                    "names": 1,
                    "addresses.country_code": 1,
                    "types": 1,
                    "abbreviations": 1,
                }
            },
            {
                "$set": {
                    "names": {
                        "$filter": {
                            "input": "$names",
                            "as": "name",
                            "cond": {"$in": ["$$name.lang", ["es", "en"]]},
                        }
                    }
                }
            },
        ]

    if affiliation_type in ["group", "faculty", "department"]:
        pipeline = [
            {"$match": {"types.type": affiliation_type}},
            {"$project": {"names": 1, "types": 1, "relations": 1}},
            {
                "$addFields": {
                    "relations": {
                        "$map": {"input": "$relations", "as": "rel", "in": "$$rel.name"}
                    }
                }
            },
        ]
    cursor = col_aff.aggregate(pipeline, allowDiskUse=True)

    batch = []
    for i, doc in enumerate(cursor, start=1):
        batch.append(doc)

        if i % bulk_size == 0:
            bulk(
                es,
                format_affiliations_documents(
                    es_index, batch, affiliation_type),
                refresh=True,
                request_timeout=request_timeout,
            )
            print(f"Inserted {i} documents...")
            batch = []

    # Insert remaining documents in the last batch
    if batch:
        bulk(
            es,
            format_affiliations_documents(es_index, batch, affiliation_type),
            refresh=True,
            request_timeout=request_timeout,
        )
        print(f"Inserted {i} documents in total.")

    print("Process completed.")


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
        names = [doc["full_name"], unidecode(doc["full_name"])]

        for name in doc["first_names"]:
            names.append(name + " " + " ".join(doc["last_names"]))
            names.append(unidecode(name) + " " +
                         unidecode(" ".join(doc["last_names"])))
        names.append(" ".join(doc["last_names"]))
        names.append(unidecode(" ".join(doc["last_names"])))
        if len(doc["first_names"]) > 0 and len(doc["last_names"]) > 0:
            names.append(f"{doc['last_names'][0]} {doc['first_names'][0]}")
            names.append(f"{doc['first_names'][0]} {doc['last_names'][0]}")
            names.append(
                unidecode(f"{doc['last_names'][0]} {doc['first_names'][0]}"))
            names.append(
                unidecode(f"{doc['first_names'][0]} {doc['last_names'][0]}"))
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
                                                    "$in": [
                                                        "$$type.type",
                                                        [
                                                            "group",
                                                            "department",
                                                            "faculty",
                                                        ],
                                                    ]
                                                },
                                            }
                                        }
                                    },
                                    0,
                                ]
                            }
                        },
                    }
                },
            }
        },
        {
            "$project": {
                "full_name": 1,
                "first_names": 1,
                "last_names": 1,
                "updated": 1,
                "products_count": 1,
                "affiliations.id": 1,
                "affiliations.name": 1,
            }
        },
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
