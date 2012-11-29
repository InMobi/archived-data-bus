#!/usr/bin/env bash

# Licensed under the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs a Databus command as a daemon.

usage="Usage: databus.sh [start/stop] [<conf-file>]"
DATABUS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../"
echo "Running Databus from $DATABUS_DIR"

#echo $#
# if no args specified, show usage
if [ $# -gt 3 ]; then
  echo $usage
  exit 1
fi

# get arguments
var1=$1
shift
var2=$1
shift
var3=$1
shift

startStop=$var1
configFile=$var2
envfile=$DATABUS_DIR/conf/databus.env

if [ "$var1" == "start" ] || [ "$var1" == "stop" ] || [ "$var1" == "restart" ]
then
#check config existence
if ! [ -r $configFile ]; then
   echo $configFile " not found."
   echo $usage
   exit 1
fi

#check env existence
if ! [ -r $envfile ]; then
   echo $envfile " not found."
   exit 1
fi

. $envfile

#create PID dir
if [ "$DATABUS_PID_DIR" = "" ]; then
  DATABUS_PID_DIR=/tmp/databus
fi
export _DATABUS_DAEMON_PIDFILE=$DATABUS_PID_DIR/databus.pid

fi

if [ -z $HADOOP_CONF_DIR ]; then
  echo "Please define HADOOP_CONF_DIR to point to hadoop configuration. eg:: /etc/hadoop/conf"
  exit 1
fi

#set classpath
export CLASSPATH=`ls $DATABUS_DIR/lib/*jar | tr "\n" :`;
export CLASSPATH=$DATABUS_DIR/conf:$CLASSPATH:$HADOOP_CONF_DIR:$DATABUS_DIR/bin
#echo setting classPath to $CLASSPATH

case $startStop in

  (start)

    mkdir -p "$DATABUS_PID_DIR"

    if [ -f $_DATABUS_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_DATABUS_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo DATABUS running as process `cat $_DATABUS_DAEMON_PIDFILE`.  Stop it first.
        exit 1
      fi
    fi

    echo starting DATABUS

   nohup java -cp "$CLASSPATH" com.inmobi.databus.Databus $configFile 2>&1 &
   if [ $? -eq 0 ]
    then
      if /bin/echo -n $! > "$_DATABUS_DAEMON_PIDFILE"
      then
        sleep 1
        echo DATABUS STARTED
      else
        echo FAILED TO WRITE PID
        exit 1
      fi
    else
      echo DATABUS DID NOT START
      exit 1
    fi
    ;;
          
  (stop)

    if [ -f $_DATABUS_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_DATABUS_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo -n Please be patient. It may take upto 1 min or more in stopping DATABUS..
        kill -s SIGINT `cat $_DATABUS_DAEMON_PIDFILE`
      while :
        do 
          if kill -0 `cat $_DATABUS_DAEMON_PIDFILE` > /dev/null 2>&1; then
             echo -n "."
             sleep 1
          else
             break
          fi
        done
        rm -rf  $_DATABUS_DAEMON_PIDFILE
        echo DONE
      else
        echo no DATABUS to stop
      fi
    else
      echo no DATABUS to stop
    fi
    ;;

  (restart)
    $0 start $configFile
    $0 stop $configFile
    ;;

  (collapse)

     hdfsName=$var2
     dir=$var3
     java -cp "$CLASSPATH" com.inmobi.databus.utils.CollapseFilesInDir $hdfsName $dir
     ;;

  (*)
    echo $usage
    exit 1
    ;;

esac



