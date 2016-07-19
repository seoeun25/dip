#### Schema Registry

# Intro

Schema Registry is the RESTFul repository to store schema.

The schema registered at Schema Registry is used in the data process such as kakfa, camus.

* register schema
* get schema

It uses jersery-1.9 for the compatibility with hadoop-2.6.

It uses the mysql as a metastore.

# How to build

```
$ mvn clean package assembly:single
$ ls -l schema-registry-repo/target/schema-registry-repo-{version}-distro.tar.gz
```

# How to install

Untar the disto tar such as schema-registry-repo-{version}-distro.tar.gz

SCHEMA_REPO_HOME will be schema-registry-repo-0.9-SNAPSHOT
```
$ tar xzvf schema-registry-repo-0.9-SNAPSHOT-distro.tar.gz
$ cd schema-registry-repo-0.9-SNAPSHOT
$ bin/schemarepo.sh start (|stop)
```

## Setup the config

Before start the schema-repo, It needs to set configuration.
Revise the config in the conf/schema-repo.conf file.

# API

```
GET /schema/ids/{string: id}

Get the schema info identified by the id

Parameters:
- id (string) - the unique identifier of the schema
Response JSON Object:
- id(string) - the id
- created(string) - created time in milliseconds
- schemaStr(string) - schema string identified by the id
Status Codes:
- 404 Not Found
- 500 Internal Server Error
```

```
GET /subjects

Get a list of registered subjects

Response JSON Array Object:
list of schema info
- id(string) - the id
- created(string) - created time in milliseconds
- schemaStr(string) - schema string identified by the id
Status Codes:
- 500 Internal Server Error
```
```
GET /subjects/{string: subject}

Get the latest subject identified by the subject name

Parameters:
- subject (string) - the name of the subject
Response JSON Object:
- id(string) - the id
- created(string) - created time in milliseconds
- schemaStr(string) - schema string identified by the id
Status Codes:
- 404 Not Found
- 500 Internal Server Error
```
```
GET /subjects/{string: subject}/ids/{string: id}

Get the subject identified by the subject name and id

Parameters:
- subject (string) - the name of the subject
- id (string) - the unique identifier
Response JSON Object:
- id(string) - the id
- created(string) - created time in milliseconds
- schemaStr(string) - schema string identified by the id
Status Codes:
- 404 Not Found
- 500 Internal Server Error
```
```
POST /subjects/{string: subject}

Register a new schema under the specified subject. If the schema string is equal to the latest schema, return the latest schema info.

Parameters:
- subject (string) - the name of the subject
- schema(string) - schema string
Response JSON Object:
- id(string) - the id
- created(string) - created time in milliseconds
- schemaStr(string) - schema string identified by the id
Status Codes:
- 404 Not Found
- 500 Internal Server Error
```

# TODO

Use easymock for the test cases.

