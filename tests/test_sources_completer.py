from unittest.mock import patch

import pytest

from kerana.completer import (
    _publish_alias,
    build_versioned_index_name,
    format_source_document,
    format_sources_documents,
    sources_completer_indexer,
)


def test_format_source_uses_all_names_and_normalizes_fields():
    action = format_source_document(
        "kahi_sources-build",
        {
            "_id": "source-1",
            "names": [
                {"name": "Revista Médica"},
                {"name": "REVISTA MEDICA"},
                {"name": "Revista Médica"},
            ],
            "publisher": {"name": " Editorial Académica "},
            "products_count": "12",
        },
    )

    assert action["_id"] == "source-1"
    assert action["_source"]["name"] == {
        "input": ["Revista Médica", "Revista Medica", "REVISTA MEDICA"],
    }
    assert action["_source"]["publisher"] == "Editorial Académica"
    assert action["_source"]["products_count"] == 12


def test_format_source_skips_invalid_names_and_sanitizes_count():
    assert format_source_document(
        "kahi_sources-build",
        {"_id": "empty", "names": [{"name": "  "}]},
    ) is None

    action = format_source_document(
        "kahi_sources-build",
        {
            "_id": "source-2",
            "names": [{"name": "Valid source"}],
            "publisher": 3.14,
            "products_count": "invalid",
        },
    )
    assert action["_source"]["publisher"] == ""
    assert action["_source"]["products_count"] == 0


def test_format_sources_documents_omits_unsearchable_records():
    actions = format_sources_documents(
        "kahi_sources-build",
        [
            {"_id": "valid", "names": [{"name": "Valid"}]},
            {"_id": "invalid", "names": []},
        ],
    )
    assert [action["_id"] for action in actions] == ["valid"]


def test_versioned_index_name_validation():
    assert build_versioned_index_name(
        "kahi_sources", "20260728T120000"
    ) == "kahi_sources-20260728t120000"
    with pytest.raises(ValueError):
        build_versioned_index_name("kahi_sources", "bad/run")


class FakeIndices:
    def __init__(self, aliases=None, concrete=None):
        self.aliases = aliases or {}
        self.concrete = set(concrete or [])
        self.actions = []
        self.deleted = []

    def exists_alias(self, name):
        return name in self.aliases

    def get_alias(self, name):
        return {index: {} for index in self.aliases[name]}

    def exists(self, index):
        return index in self.concrete

    def update_aliases(self, actions):
        self.actions = actions

    def create(self, index, body):
        self.concrete.add(index)

    def delete(self, index):
        self.deleted.append(index)
        self.concrete.discard(index)

    def put_settings(self, index, body):
        return None

    def refresh(self, index):
        return None


class FakeElasticsearch:
    def __init__(self, indices):
        self.indices = indices
        self.docs = {}

    def count(self, index):
        return {"count": len(self.docs)}


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    def aggregate(self, pipeline, allowDiskUse):
        return iter(self.docs)

    def with_options(self, read_concern):
        return self


class FailingCollection(FakeCollection):
    def aggregate(self, pipeline, allowDiskUse):
        raise RuntimeError("MongoDB unavailable")


class FakeDatabase:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, name):
        return self.collection


class FakeMongo:
    def __init__(self, docs):
        self.database = FakeDatabase(FakeCollection(docs))

    def __getitem__(self, name):
        return self.database


def test_publish_alias_keeps_previous_physical_index():
    indices = FakeIndices(aliases={"kahi_sources": ["kahi_sources-old"]})
    previous = _publish_alias(
        FakeElasticsearch(indices), "kahi_sources", "kahi_sources-new"
    )
    assert previous == ["kahi_sources-old"]
    assert indices.actions == [
        {"remove": {"index": "kahi_sources-old", "alias": "kahi_sources"}},
        {"add": {"index": "kahi_sources-new", "alias": "kahi_sources"}},
    ]
    assert indices.deleted == []


def test_publish_alias_migrates_a_concrete_index_atomically():
    indices = FakeIndices(concrete={"kahi_sources"})
    previous = _publish_alias(
        FakeElasticsearch(indices), "kahi_sources", "kahi_sources-new"
    )
    assert previous == ["kahi_sources"]
    assert indices.actions == [
        {"remove_index": {"index": "kahi_sources"}},
        {"add": {"index": "kahi_sources-new", "alias": "kahi_sources"}},
    ]


def test_versioned_build_validates_before_alias_publication():
    indices = FakeIndices(aliases={"kahi_sources": ["kahi_sources-old"]})
    es = FakeElasticsearch(indices)
    mongo = FakeMongo([
        {"_id": "one", "names": [{"name": "One"}], "products_count": 3},
        {"_id": "two", "names": [{"name": "Two"}], "products_count": 2},
        {"_id": "skip", "names": []},
    ])

    def fake_bulk(client, actions, **kwargs):
        client.docs.update({action["_id"]: action for action in actions})
        return len(actions), 0

    def fake_scan(client, **kwargs):
        return ({"_id": value} for value in client.docs)

    with patch("kerana.completer.bulk", side_effect=fake_bulk), patch(
        "kerana.completer.scan", side_effect=fake_scan
    ):
        report = sources_completer_indexer(
            es,
            "kahi_sources",
            mongo,
            "kahi",
            "sources",
            bulk_size=2,
            versioned=True,
            run_id="test",
            validate=True,
        )

    assert report["physical_index"] == "kahi_sources-test"
    assert report["processed"] == 3
    assert report["indexed"] == 2
    assert report["skipped"] == 1
    assert report["previous_indices"] == ["kahi_sources-old"]
    assert indices.deleted == []


def test_failed_validation_deletes_only_new_index_and_keeps_alias():
    indices = FakeIndices(aliases={"kahi_sources": ["kahi_sources-old"]})
    es = FakeElasticsearch(indices)
    mongo = FakeMongo([
        {"_id": "one", "names": [{"name": "One"}]},
    ])

    def fake_bulk(client, actions, **kwargs):
        client.docs.update({action["_id"]: action for action in actions})
        return len(actions), 0

    with patch("kerana.completer.bulk", side_effect=fake_bulk), patch(
        "kerana.completer.scan", return_value=iter([])
    ), pytest.raises(RuntimeError, match="validation failed"):
        sources_completer_indexer(
            es,
            "kahi_sources",
            mongo,
            "kahi",
            "sources",
            versioned=True,
            run_id="failed",
            validate=True,
        )

    assert indices.deleted == ["kahi_sources-failed"]
    assert indices.actions == []


def test_mongo_failure_deletes_the_unpublished_physical_index():
    indices = FakeIndices(aliases={"kahi_sources": ["kahi_sources-old"]})
    es = FakeElasticsearch(indices)
    mongo = FakeMongo([])
    mongo.database.collection = FailingCollection([])

    with pytest.raises(RuntimeError, match="MongoDB unavailable"):
        sources_completer_indexer(
            es,
            "kahi_sources",
            mongo,
            "kahi",
            "sources",
            versioned=True,
            run_id="mongo-failed",
        )

    assert indices.deleted == ["kahi_sources-mongo-failed"]
    assert indices.actions == []
