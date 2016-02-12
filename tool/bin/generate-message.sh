#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: generate-message.sh --config {a=b,c=d......}"
  echo "Usage: generate-message.sh --config {borker=localhost:9092,topic=employee,maxcount=1000,concurrent=5,schemaregistry=dummy,type=avro,batchsize=16384,buffersize=33554432,file=portinfo.dat}"
  exit 1
fi

configs=$2
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

if [ "$DIP_PID_DIR" = "" ]; then
  DIP_PID_DIR=/tmp
fi
log=$DIP_PID_DIR/gen.out
pid=$DIP_PID_DIR/gen.pid
STOP_TIMEOUT=${STOP_TIMEOUT:-3}

JAVA_OPT="-Xms2048m -Xmx4096m"

echo starting $command logging to $log
echo LOG_DIR : ${LOG_DIR}
java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} com.nexr.dip.tool.MessageGenerator --config ${configs}
