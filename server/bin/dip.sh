#!/bin/bash
#

PRG="${0}"
BASEDIR=`dirname ${PRG}`
BIN_DIR=$BASEDIR

source ${BIN_DIR}/env.sh

if [ "$DIP_HOME" == "" ]; then
    DIP_HOME=`cd ${BIN_DIR}/..;pwd`
fi

LIB=${DIP_HOME}/lib

if [ "$DIP_CONF_DIR" == "" ]; then
    DIP_CONF_DIR=$DIP_HOME/conf
fi

if [ "$DIP_LOG_DIR" == "" ]; then
    DIP_LOG_DIR=${DIP_HOME}/logs
fi

DIP_CONF=$DIP_CONF_DIR
# prepend conf dir to classpath
if [ -n "$DIP_CONF" ]; then
  CLASS_PATH="$DIP_CONF:$CLASS_PATH"
fi

CLASS_PATH=${CLASS_PATH}:${LIB}/'*'

if [ "$DIP_JAVA_OPT" == "" ]; then
    DIP_JAVA_OPT="-Xms2048m -Xmx4096m"
fi

JAVA=$JAVA_HOME/bin/java
exec "$JAVA" ${DIP_JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${DIP_LOG_DIR} com.nexr.dip.server.DipServer start


