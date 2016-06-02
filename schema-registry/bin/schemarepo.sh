#!/bin/bash
#

PRG="${0}"
BASEDIR=`dirname ${PRG}`
BIN_DIR=$BASEDIR

source ${BIN_DIR}/env.sh

if [ "$SCHEMA_REPO_HOME" == "" ]; then
    SCHEMA_REPO_HOME=`cd ${BIN_DIR}/..;pwd`
fi

LIB=${SCHEMA_REPO_HOME}/lib
if [ "$LOG_DIR" == "" ]; then
    LOG_DIR=${SCHEMA_REPO_HOME}/logs
fi
mkdir -p $LOG_DIR

if [ "$LOG_ROOTLOGGER" == "" ]; then
    LOG_ROOTLOGGER="INFO,console"
fi

if [ "$CONF_DIR" == "" ]; then
    CONF_DIR=$SCHEMA_REPO_HOME/conf
fi

SCHEMA_REPO_CONF=$CONF_DIR
# prepend conf dir to classpath
if [ -n "$SCHEMA_REPO_CONF" ]; then
  CLASS_PATH="$SCHEMA_REPO_CONF:$CLASS_PATH"
fi

CLASS_PATH=${CLASS_PATH}:${LIB}/'*'

if [ "$JAVA_OPT" == "" ]; then
    JAVA_OPT="-Xms2048m -Xmx4096m"
fi

java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} -Dlog.rootLogger=${LOG_ROOTLOGGER} com.nexr.server.DipSchemaRepoServer start

