#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: topic_reset_largest.sh [topic] "
  exit 1
fi

PRG="${0}"
BASEDIR=`dirname ${PRG}`
BIN_DIR=$BASEDIR

topic=$1
echo "topic : $topic"

ETL_EXEC_PATH="/user/ndap/camus/${topic}/exec"

BASE_PATH=`hdfs dfs -ls ${ETL_EXEC_PATH}`
if [[ "$BASE_PATH" != *"Found"* ]]; then
    echo "Execution path is already empty."
    echo "SUCCEEDED!!!"
    echo ""
    exit 0
fi

COMMAND_DEL=`hdfs dfs -rm -r ${ETL_EXEC_PATH}`

BASE_PATH=`hdfs dfs -ls ${ETL_EXEC_PATH}`
if [[ "$BASE_PATH" != *"Found"* ]]; then
    echo "EXECUTION SUCCEEDED!!!"
    echo ""
    exit 0
fi

echo "You are in trouble.... check it out manually!!!"
echo ""
