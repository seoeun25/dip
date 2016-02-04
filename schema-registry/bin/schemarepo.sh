#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: schemarepo.sh (start|stop)"
  exit 1
fi

command=$1
shift

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

if [ "$CONF_DIR" == "" ]; then
    CONF_DIR=$SCHEMA_REPO_HOME/conf
fi

CLASS_PATH=""
SCHEMA_REPO_CONF=$CONF_DIR
# prepend conf dir to classpath
if [ -n "$SCHEMA_REPO_CONF" ]; then
  CLASS_PATH="$SCHEMA_REPO_CONF:$CLASS_PATH"
fi

CLASS_PATH=${CLASS_PATH}:${LIB}/'*'

if [ "$SCHEMA_REPO_PID_DIR" = "" ]; then
  SCHEMA_REPO_PID_DIR=/tmp
fi
log=$LOG_DIR/schemarepo.out
pid=$SCHEMA_REPO_PID_DIR/schemarepo.pid
STOP_TIMEOUT=${STOP_TIMEOUT:-3}

JAVA_OPT="-Xms2048m -Xmx4096m"

module=DipSchemaRepoServer
case $command in

  (start)
    [ -w "$SCHEMA_REPO_PID_DIR" ] ||  mkdir -p "$SCHEMA_REPO_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $module running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    echo starting $module logging to $log
    java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} com.nexr.server.DipSchemaRepoServer ${command} > "$log" 2>&1 < /dev/null &

    echo $! > $pid
    TARGET_PID=`cat $pid`
    echo starting as process $TARGET_PID
    echo ""
    ;;
  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $module
        kill $TARGET_PID
        sleep $STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$module did not stop gracefully after $STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no $module to stop
      fi
      rm -f $pid
    else
      echo no $module to stop
    fi
    echo ""
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
