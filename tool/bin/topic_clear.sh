#!/bin/bash
#

if [ $# -le 0 ]; then
  echo "Usage: topic_clear.sh [topic] "
  exit 1
fi

PRG="${0}"
BASEDIR=`dirname ${PRG}`
BIN_DIR=$BASEDIR
echo $BIN_DIR

topic=$1
echo "Start clear $topic ......"

KAFKA_HOME="/usr/lib/kafka"
TOPIC_SCRIPT="$KAFKA_HOME/bin/kafka-topics.sh"
ZOOKEEPER_SERVERS="aih012:2181"


if [ -f "$TOPIC_SCRIPT" ]; then
    echo "KAFKA exist. OK"
else
    echo "KAFKA does not exist. Can not clear topic"
    exit 0;
fi

KAFKA_LOGS="/data2/kafka-logs"
TOPIC_LOG_DIR=""
for entry in "$KAFKA_LOGS"/$topic-*
do
    TOPIC_LOG_DIR=$entry
done

FILE_COUNT=`ls -l $TOPIC_LOG_DIR | wc -l`
echo "FILE_COUNT : $FILE_COUNT"

## retention=1
/bin/bash "$TOPIC_SCRIPT" --zookeeper $ZOOKEEPER_SERVERS --alter --config retention.ms=1 --topic $topic

for i in {1..60};
do
    echo "Checking log files......"
    sleep 3;
    FILE_COUNT=`ls -l $TOPIC_LOG_DIR | wc -l`
    if [ "$FILE_COUNT" == 3 ]; then
        break;
    else
        echo "LOG still exists : $FILE_COUNT, CLEARING......"
    fi
    echo `date`
done

TOPIC_LOG_FILE=""
if [ -f $TOPIC_LOG_DIR/*.log ]; then
    TOPIC_LOG_FILE=`basename $TOPIC_LOG_DIR/*.log`
    TOPIC_LOG_FILE=$TOPIC_LOG_DIR/$TOPIC_LOG_FILE
fi


if [ "$TOPIC_LOG_FILE" == "" ]; then
    echo "Already no log file. exit"
    exit 0;
fi
echo "TOPIC_LOG_FILE : $TOPIC_LOG_FILE"

LOG_SIZE=$(wc -c <$TOPIC_LOG_FILE)
if [ $LOG_SIZE == 0 ]; then
    echo "EXECUTION SUCCEEDED!!!"
else
    echo "FAIL!!!"
    echo "You are in trouble.... check it out manually!!!"
    echo ""
fi

RETENTION="345600000"
if [ "$topic" == "trap_info" ]; then
    RETENTION="86400000"
fi
if [ "$topic" == "tb_dhcp" ]; then
    RETENTION="86400000"
fi

## recover retention
/bin/bash "$TOPIC_SCRIPT" --zookeeper $ZOOKEEPER_SERVERS --alter --config retention.ms="$RETENTION" --topic $topic


for i in {1..3};
do
    echo "......"
    sleep 1;
done

## describe retention
/bin/bash "$TOPIC_SCRIPT" --zookeeper $ZOOKEEPER_SERVERS --describe --topic $topic

sleep 1

echo ""
echo "DONE clear topic"
echo ""
