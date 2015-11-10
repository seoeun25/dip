#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: generate-workflow.sh (ToolRunner|Workflow) "
  exit 1
fi

type=$1
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

if [ "$type" = "" ]; then
  type=Workflow
fi
echo "type $type"

if [ "$DIP_PID_DIR" = "" ]; then
  DIP_PID_DIR=/tmp
fi
log=$DIP_PID_DIR/gen-wf.out

STOP_TIMEOUT=${STOP_TIMEOUT:-3}

JAVA_OPT="-Xms2048m -Xmx4096m"

echo starting $command logging to $log
echo LOG_DIR : ${LOG_DIR}
java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} com.nexr.dip.tool.WFGenerator ${type} > "$log" 2>&1 < /dev/null &


