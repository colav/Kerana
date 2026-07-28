from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import re

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from elasticsearch.helpers import bulk, scan
from unidecode import unidecode


person_mapping = {"mappings": {"properties": {
    "full_name": {"type": "completion"}}}}

affiliations_mapping = {"mappings": {
    "properties": {"name": {"type": "completion"}}}}

sources_mapping = {
    "mappings": {
        "properties": {
            "name": {"type": "completion"},
            "publisher": {"type": "text"},
            "products_count": {"type": "integer"}
        }
    }
}


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def build_versioned_index_name(alias: str, run_id: str | None = None) -> str:
    """Return a safe physical index name for a versioned build."""
    run_id = run_id or _utc_run_id()
    if not re.fullmatch(r"[a-zA-Z0-9._-]+", run_id):
        raise ValueError("run_id may only contain letters, numbers, '.', '_' and '-'")
    return f"{alias}-{run_id}".lower()


def _source_inputs(doc: dict) -> list[str]:
    """Build unique completion inputs from every valid source name."""
    inputs = []
    seen = set()
    for item in doc.get("names", []):
        value = item.get("name") if isinstance(item, dict) else item
        if not isinstance(value, str):
            continue
        value = value.strip()
        if not value:
            continue
        for candidate in (value, unidecode(value)):
            if candidate and candidate not in seen:
                seen.add(candidate)
                inputs.append(candidate)
    return inputs


def _source_publisher(doc: dict) -> str:
    publisher = doc.get("publisher")
    if isinstance(publisher, dict):
        publisher = publisher.get("name", "")
    return publisher.strip() if isinstance(publisher, str) else ""


def _source_products_count(doc: dict) -> int:
    value = doc.get("products_count", 0)
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def format_source_document(index_name: str, doc: dict) -> dict | None:
    """Format one source or return None when it has no searchable name."""
    inputs = _source_inputs(doc)
    if not inputs:
        return None
    products_count = _source_products_count(doc)
    return {
        "_op_type": "index",
        "_index": index_name,
        "_id": str(doc["_id"]),
        "_source": {
            "name": {"input": inputs},
            "publisher": _source_publisher(doc),
            "products_count": products_count,
        },
    }


def _publish_alias(es: Elasticsearch, alias: str, new_index: str) -> list[str]:
    """Publish a validated index and return the previously aliased indices."""
    previous = []
    actions = []
    if es.indices.exists_alias(name=alias):
        previous = list(es.indices.get_alias(name=alias).keys())
        actions.extend(
            {"remove": {"index": index, "alias": alias}}
            for index in previous
        )
    elif es.indices.exists(index=alias):
        # One-time migration from a concrete index to an alias with the same name.
        previous = [alias]
        actions.append({"remove_index": {"index": alias}})
    actions.append({"add": {"index": new_index, "alias": alias}})
    es.indices.update_aliases(actions=actions)
    return previous


def _run_completer_indexer(
    *,
    entity: str,
    es: Elasticsearch,
    es_index: str,
    mapping: dict,
    mdb_client: MongoClient,
    mdb_name: str,
    mdb_col: str,
    pipeline: list,
    format_documents,
    bulk_size: int,
    reset_esindex: bool,
    request_timeout: int,
    versioned: bool,
    run_id: str | None,
    snapshot_read: bool,
    validate: bool,
) -> dict:
    """Build, reconcile and optionally publish one completer index."""
    if bulk_size <= 0:
        raise ValueError("bulk_size must be greater than zero")

    physical_index = (
        build_versioned_index_name(es_index, run_id)
        if versioned
        else es_index
    )
    if versioned and es.indices.exists(index=physical_index):
        raise ValueError(f"Physical index already exists: {physical_index}")

    if not versioned and reset_esindex and es.indices.exists(index=physical_index):
        es.indices.delete(index=physical_index)

    index_definition = deepcopy(mapping)
    index_definition.setdefault("settings", {})["refresh_interval"] = "-1"
    index_definition["mappings"]["_meta"] = {
        "kerana_entity": entity,
        "source_database": mdb_name,
        "source_collection": mdb_col,
        "run_id": run_id or physical_index.rsplit("-", 1)[-1],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    created = False
    if not es.indices.exists(index=physical_index):
        es.indices.create(index=physical_index, body=index_definition)
        created = True

    processed = 0
    indexed = 0
    skipped = 0
    source_ids = set()
    batch = []

    def insert_batch(documents):
        nonlocal indexed, skipped
        actions = format_documents(physical_index, documents)
        skipped += len(documents) - len(actions)
        if not actions:
            return
        successful, failed = bulk(
            es,
            actions,
            refresh=False,
            request_timeout=request_timeout,
            stats_only=True,
            raise_on_error=True,
        )
        if failed:
            raise RuntimeError(f"Elasticsearch bulk reported {failed} failures")
        indexed += successful
        source_ids.update(action["_id"] for action in actions)

    try:
        collection = mdb_client[mdb_name][mdb_col]
        if snapshot_read:
            collection = collection.with_options(
                read_concern=ReadConcern("snapshot")
            )
        cursor = collection.aggregate(pipeline, allowDiskUse=True)

        for doc in cursor:
            processed += 1
            batch.append(doc)
            if len(batch) == bulk_size:
                insert_batch(batch)
                print(f"Processed {processed} documents...")
                batch = []
        if batch:
            insert_batch(batch)

        es.indices.put_settings(
            index=physical_index,
            body={"index": {"refresh_interval": "1s"}},
        )
        es.indices.refresh(index=physical_index)

        elastic_count = es.count(index=physical_index)["count"]
        if validate:
            elastic_ids = {
                str(hit["_id"])
                for hit in scan(
                    es,
                    index=physical_index,
                    query={"query": {"match_all": {}}},
                    _source=False,
                    request_timeout=request_timeout,
                )
            }
            missing = source_ids - elastic_ids
            orphaned = elastic_ids - source_ids
            if elastic_count != indexed or missing or orphaned:
                raise RuntimeError(
                    f"{entity} index validation failed: "
                    f"expected={indexed}, actual={elastic_count}, "
                    f"missing={len(missing)}, orphaned={len(orphaned)}, "
                    f"missing_sample={sorted(missing)[:10]}, "
                    f"orphaned_sample={sorted(orphaned)[:10]}"
                )

        previous_indices = []
        if versioned:
            previous_indices = _publish_alias(es, es_index, physical_index)

        report = {
            "entity": entity,
            "alias": es_index if versioned else None,
            "physical_index": physical_index,
            "previous_indices": previous_indices,
            "source_database": mdb_name,
            "source_collection": mdb_col,
            "processed": processed,
            "indexed": indexed,
            "skipped": skipped,
            "elastic_count": elastic_count,
            "validated": validate,
            "published": True,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        print("Process completed.")
        return report
    except Exception:
        if versioned and created and es.indices.exists(index=physical_index):
            es.indices.delete(index=physical_index)
        raise


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
    versioned: bool = False,
    run_id: str | None = None,
    snapshot_read: bool = False,
    validate: bool = True,
) -> dict:
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
    return _run_completer_indexer(
        entity=affiliation_type,
        es=es,
        es_index=es_index,
        mapping=affiliations_mapping,
        mdb_client=mdb_client,
        mdb_name=mdb_name,
        mdb_col=mdb_col,
        pipeline=pipeline,
        format_documents=lambda index, documents: format_affiliations_documents(
            index, documents, affiliation_type
        ),
        bulk_size=bulk_size,
        reset_esindex=reset_esindex,
        request_timeout=request_timeout,
        versioned=versioned,
        run_id=run_id,
        snapshot_read=snapshot_read,
        validate=validate,
    )


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
            ascii_last_names = unidecode(" ".join(doc["last_names"]))
            names.append(f"{unidecode(name)} {ascii_last_names}")
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
    versioned: bool = False,
    run_id: str | None = None,
    snapshot_read: bool = False,
    validate: bool = True,
) -> dict:
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

    return _run_completer_indexer(
        entity="person",
        es=es,
        es_index=es_index,
        mapping=person_mapping,
        mdb_client=mdb_client,
        mdb_name=mdb_name,
        mdb_col=mdb_col,
        pipeline=pipeline,
        format_documents=format_person_documents,
        bulk_size=bulk_size,
        reset_esindex=reset_esindex,
        request_timeout=request_timeout,
        versioned=versioned,
        run_id=run_id,
        snapshot_read=snapshot_read,
        validate=validate,
    )


def format_sources_documents(index_name: str, docs: list) -> list:
    """
    Create a list of documents to be indexed in ElasticSearch.
    Each document contains source name, publisher and products_count.

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
    return [
        formatted
        for doc in docs
        if (formatted := format_source_document(index_name, doc)) is not None
    ]


def sources_completer_indexer(
    es: Elasticsearch,
    es_index: str,
    mdb_client: MongoClient,
    mdb_name: str,
    mdb_col: str,
    bulk_size: int = 100,
    reset_esindex: bool = True,
    request_timeout: int = 60,
    versioned: bool = False,
    run_id: str | None = None,
    snapshot_read: bool = False,
    validate: bool = True,
) -> dict:
    """
    Create an index for sources name completion in ElasticSearch.

    Parameters:
    ------------
    es:ElasticSearch
        ElasticSearch client
    es_index:str
        ElasticSearch index name
    mdb_client:MongoClient
        MongoDB client
    mdb_name:str
        Mongo database name
    mdb_col:str
        Mongo collection name (should be 'sources')
    bulk_size:int=100
        bulk cache size to insert document in ES.
    reset_esindex:bool
        reset the index before insert documents
    request_timeout:int
        request timeout for ElasticSearch
    """

    pipeline = [
        {
            "$project": {
                "names.name": 1,
                "publisher.name": 1,
                "products_count": 1,
            }
        }
    ]
    return _run_completer_indexer(
        entity="sources",
        es=es,
        es_index=es_index,
        mapping=sources_mapping,
        mdb_client=mdb_client,
        mdb_name=mdb_name,
        mdb_col=mdb_col,
        pipeline=pipeline,
        format_documents=format_sources_documents,
        bulk_size=bulk_size,
        reset_esindex=reset_esindex,
        request_timeout=request_timeout,
        versioned=versioned,
        run_id=run_id,
        snapshot_read=snapshot_read,
        validate=validate,
    )
