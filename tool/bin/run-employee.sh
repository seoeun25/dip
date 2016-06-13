#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: run-employee.sh {borker}"

  exit 1
fi

###### insert schema of employee into schemainfo table
# mysql>
# INSERT INTO schemainfo (created, name, schemaStr) VALUES(now(), 'employee', '{"type":"record","name":"employee", "namespace":"com.nexr.dip.avro.schema","fields":[{"name":"name","type":"string"},{"name":"favorite_number","type":["string","null"]},{"name":"favorite_color","type":["string","null"]},{"name":"wrk_dt","type":"long"},{"name":"src_info","type":"string"},{"name":"header","type":{"type":"record","name":"headertype","fields":[{"name":"time","type":"long"}]}}]}');
#
######

## send event(employee) to kafka
BROKER=$1
echo $BROKER
COMMAND=bin/generate-message.sh --config broker=$BROKER
`$COMAND`

## DIP will upload the event of employee on HDFS