#### DIP (Data Integration Platform)

# Intro

DIP is a data transformation service. It provides hive linker to connect with hive partition.
Currently, it support the pipe line from kafka to hive.

# How to Build
```
$ mvn clean package assembly:single -DskipTests
$ ls -l server/target/dip-server-0.9-SNAPSHOT-distro.tar.gz
```

# How to Install

* untar dip-server-XXXXXX-distro.tar.gz
* configuration
* generate the workflow applications and deploy to oozie sharelib
* run DIP Server

## untar distro tar

```
$ tar xzvf dip-server-0.9-SNAPSHOT-distro.tar.gz
$ cd dip-server-0.9-SNAPSHOT
$ ls -l
drwxr-xr-x    7 seoeun  staff   238 Feb  3 16:05 bin
lrwxr-xr-x    1 seoeun  staff    27 Feb  3 16:06 conf
drwxr-xr-x  113 seoeun  staff  3842 Feb  3 16:02 lib
drwxr-xr-x    3 seoeun  staff   102 Feb  3 16:02 oozie-sharelib
```

## configuration

bin/env.sh
```
uncomment following configuration and revise.
## DIP_HADOOP_DEPENDENCIES
# CLASS_PATH="/usr/lib/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.7.1.jar:/usr/lib/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/usr/lib/hadoop//share/hadoop/tools/lib/hadoop-auth-2.7.1.jar"
```

conf/dip.conf

Name  | Example
------------- | -------------
dip.jdbc.url  | jdbc:mysql://aih018.nexr.com/dip?useUnicode=true&characterEncoding=UTF-8
dip.jdbc.username  | sa
dip.jdbc.password  | sa
dip.schemaregistry.url  | http://aih013.nexr.com:18181/repo
dip.kafka.broker  | aih013.nexr.com:9092,aih014.nexr.com:9092,aih015.nexr.com:9092
dip.namenode  | hdfs://ndap:8020
dip.jobtracker  | ndap:8050
dip.hiveserver  | aih012.nexr.com:10000
dip.hiveserver.user  | sdip
dip.hiveserver.passwd  | sdip
dip.oozie  | http://aih018:11000/oozie
dip.avro.topics  | l3_mac_tab,gpx_port,gpe_dsl_cli,gpe_dsl_snmp
dip.text.topics  | tb_dhcp,trap_info,holdingequip

## workflow applications

Generate workflow application corresponding the defined topics.
You should set the configuration of dip.avro.topics and dip.text.topics on dip.conf

```
$ bin/generate-workflow.sh Workflow
$ ls -l apps
amas_main
arp_info
device_main
......
......
......


## list the files in the application
$ ls -l apps/amas_main/
job.properties
lib
workflow.xml

## Deploy all applications on HDFS
$ hdfs dfs -put apps /user/sdip/dip/
```

Deploy dip-camus.jar to oozie sharelib.
```
$ sudo -u oozie hdfs dfs -put oozie-sharelib/dip-camus-${version}-shaded.jar/user/oozie/share/lib/lib_{yyyyMMddHHmmss}/oozie/
$ service oozie restart
```

## Running DIP

OS account: sidp
```
$ bin/dip.sh start
```

# Running Example

Before run the example, You should register the schema to schema-registry for test.
See bin/run-example.sh

```
$ bin/run-employee.sh localhost:9092
```