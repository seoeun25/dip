#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: simple-consumer.sh {borker}, {port}, {topic}, {parition}, {offset}, {maxread}"
  echo "Usage: simple-consumer.sh {localhost}, {9092}, {employee}, {0}, {426817}, {10}"

  exit 1
fi

host=$1
port=$2
topic=$3
partition=$4
offset=$5
maxread=$6
shift

PRG="${0}"
BASEDIR=`dirname ${PRG}`
BIN_DIR=$BASEDIR

source ${BIN_DIR}/env.sh

if [ "$DIP_HOME" == "" ]; then
    DIP_HOME=`cd ${BIN_DIR}/..;pwd`
fi
echo DIP_HOME : ${DIP_HOME}

LIB=${DIP_HOME}/lib
if [ "$LOG_DIR" == "" ]; then
    LOG_DIR=${DIP_HOME}/logs
fi

if [ "$CONF_DIR" == "" ]; then
    CONF_DIR=$DIP_HOME/conf
fi

CLASS_PATH=""
DIP_CONF=$CONF_DIR
# prepend conf dir to classpath
if [ -n "$DIP_CONF" ]; then
  CLASS_PATH="$DIP_CONF:$CLASS_PATH"
fi

CLASS_PATH=${CLASS_PATH}:${LIB}/'*'


STOP_TIMEOUT=${STOP_TIMEOUT:-3}

JAVA_OPT="-Xms2048m -Xmx4096m"

echo starting $command logging to $log
echo LOG_DIR : ${LOG_DIR}
echo "java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} com.nexr.dip.tool.SimpleKafkaConsumer ${host} ${port} ${topic} ${partition} ${offset} ${maxread}"
java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} com.nexr.dip.tool.SimpleKafkaConsumer ${host} ${port} ${topic} ${partition} ${offset} ${maxread}



