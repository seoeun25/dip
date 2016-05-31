#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: dip.sh (start|stop)"
  exit 1
fi

actionCmd=$1
shift

PRG="${0}"
BASEDIR=`dirname ${PRG}`
BIN_DIR=$BASEDIR

source ${BIN_DIR}/env.sh

if [ "$DIP_HOME" == "" ]; then
    DIP_HOME=`cd ${BIN_DIR}/..;pwd`
fi

LIB=${DIP_HOME}/lib
if [ "$LOG_DIR" == "" ]; then
    LOG_DIR=${DIP_HOME}/logs
fi
mkdir -p $LOG_DIR

if [ "$CONF_DIR" == "" ]; then
    CONF_DIR=$DIP_HOME/conf
fi

# load hadoop dependencies from external location
#CLASS_PATH="/usr/lib/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.7.1.jar:/usr/lib/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/usr/lib/hadoop//share/hadoop/tools/lib/hadoop-auth-2.7.1.jar"
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
log=$LOG_DIR/dip.out
pid=$DIP_PID_DIR/dip.pid
STOP_TIMEOUT=${STOP_TIMEOUT:-20}

JAVA_OPT="-Xms2048m -Xmx4096m"

module=DipServer
case $actionCmd in

  (start)
    [ -w "$DIP_PID_DIR" ] ||  mkdir -p "$DIP_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $module running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    echo starting $module logging to $log
    java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} com.nexr.dip.server.DipServer ${actionCmd} > "$log" 2>&1 < /dev/null &

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
