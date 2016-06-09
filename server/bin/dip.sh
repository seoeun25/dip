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

if [ "$CONF_DIR" == "" ]; then
    CONF_DIR=$DIP_HOME/conf
fi

if [ "$LOG_DIR" == "" ]; then
    LOG_DIR=${DIP_HOME}/logs
fi

if [ "$LOG_ROOTLOGGER" == "" ]; then
    LOG_ROOTLOGGER="INFO,console"
fi

DIP_CONF=$CONF_DIR
# prepend conf dir to classpath
if [ -n "$DIP_CONF" ]; then
  CLASS_PATH="$DIP_CONF:$CLASS_PATH"
fi

CLASS_PATH=${CLASS_PATH}:${LIB}/'*'

if [ "$JAVA_OPT" == "" ]; then
    JAVA_OPT="-Xms2048m -Xmx4096m"
fi

JAVA=$JAVA_HOME/bin/java
exec "$JAVA" ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} -Dlog.rootLogger=${LOG_ROOTLOGGER} com.nexr.dip.server.DipServer start


