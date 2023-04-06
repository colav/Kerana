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

# TODO
* selective fields from MongoDB to ES
* Search text in ES and resolve from ids in MongoDB
* parallel load to ES from mongo
* restore/backup ES indexes
* support for config input to execute flows

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



