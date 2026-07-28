<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kerana 
MongoDB and ElasticSearch tools

# Description

# Installation

## Dependencies
This package requires elasticsearch and MongoDB
for ES you can see [https://github.com/colav/colasticsearch](https://github.com/colav/colasticsearch)
## Package
`pip install Kerana`


# Usage
Example for SIIU
```
kerana --es_index siiu_project --mdb_name siiu --mdb_col project
```

Example for Scienti
```
kerana --es_index scienti_udea_2022_product --mdb_name scienti_udea_2022 --mdb_col product --bulk_size 100
kerana --es_index scienti_udea_2022_project --mdb_name scienti_udea_2022 --mdb_col project --bulk_size 100
kerana --es_index scienti_udea_2022_patent  --mdb_name scienti_udea_2022 --mdb_col patent  --bulk_size 100
kerana --es_index scienti_udea_2022_network --mdb_name scienti_udea_2022 --mdb_col network --bulk_size 100
kerana --es_index scienti_udea_2022_event   --mdb_name scienti_udea_2022 --mdb_col event   --bulk_size 100
```

# Usage Completer
```
kerana_completer --entity person --es_user elastic  --es_pass mypass --es_index kahi_dev_person --mdb_name kahi_dev --mdb_col person --bulk_size 1000
kerana_completer --entity institution --es_user elastic  --es_pass mypass --es_index kahi_dev_institution --mdb_name kahi_dev --mdb_col affiliations --bulk_size 1000
kerana_completer --entity group --es_user elastic  --es_pass mypass --es_index kahi_dev_group --mdb_name kahi_dev --mdb_col affiliations --bulk_size 1000
kerana_completer --entity faculty --es_user elastic  --es_pass mypass --es_index kahi_dev_faculty --mdb_name kahi_dev --mdb_col affiliations --bulk_size 1000
kerana_completer --entity department --es_user elastic  --es_pass mypass --es_index kahi_dev_department --mdb_name kahi_dev --mdb_col affiliations --bulk_size 1000

```

## Safe completer rebuilds

Completers must be indexed after the Kahi workflow has finished all merges,
cleanup, post-calculations, and the definitive MongoDB database has been
promoted. This applies to `person`, `sources`, and every affiliation type.

By default the command builds a physical versioned index, validates its IDs
and count, and only then publishes it through the requested alias:

```
kerana_completer \
  --entity sources \
  --es_uri http://localhost:9200 \
  --es_user elastic \
  --es_pass mypass \
  --es_index kahi_sources \
  --mdb_uri mongodb://localhost:27017/ \
  --mdb_name kahi \
  --mdb_col sources \
  --bulk_size 1000 \
  --versioned \
  --no-snapshot-read \
  --validate \
  --report-file kerana_sources_report.json
```

For people, use the same command with `--entity person`, `--mdb_col person`,
`--es_index kahi_person`, and a different report file.

The physical index is named `<alias>-<UTC timestamp>`. Existing aliases remain
unchanged if loading or validation fails. On the first run, a concrete index
with the alias name is replaced only after the new index validates
successfully. Later runs keep the previously aliased physical index available
for rollback.

The default `--no-snapshot-read` is appropriate for an immutable, already
promoted standalone database. Use `--snapshot-read` only with a MongoDB
deployment that supports read concern `snapshot`.

`--no-versioned` enables the legacy direct-write mode and is not recommended
for a live deployment.

Before accepting a build, Kerana verifies:

* every indexed source has at least one valid name;
* the bulk API reported no failures;
* the Elasticsearch count equals the number of formatted MongoDB documents;
* indexable MongoDB source IDs and Elasticsearch document IDs are identical;
  records without any non-empty name are reported as `skipped`.

# TODO
* selective fields from MongoDB to ES
* Search text in ES and resolve from ids in MongoDB
* parallel load to ES from mongo
* restore/backup ES indexes
* support for config input to execute flows

# ES diagnostic
if the process fails run 

```
curl -u elastic:pass -X GET http://localhost:9200/_cluster/health?pretty
curl -u elastic:pass -X GET http://localhost:9200/_cluster/allocation/explain?pretty
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
