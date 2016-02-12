#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: run-employee.sh {borker}, {port}, {topic}, {parition}, {offset}, {maxread}"

  exit 1
fi

## insert schema of employee into schemainfo table
mysql>
INSERT INTO schemainfo (created, name, schemaStr) VALUES(now(), 'employee', '{"type":"record","name":"employee","namespace":"com.nexr.dip.avro.schema","fields":[{"name":"name","type":"string"},{"name":"favorite_number","type":["int","null"]},{"name":"favorite_color","type":["string","null"]},{"name":"wrk_dt","type":"long"},{"name":"srcinfo","type":"string"},{"name":"header","type":{"type":"record","name":"headertype","fields":[{"name":"time","type":"long"}]}}]}');

## send event(employee) to kafka
$ bin/generate-message.sh --config broker=localhost:9092

## DIP will upload the event of employee on HDFS