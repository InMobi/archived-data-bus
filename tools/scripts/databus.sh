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


if [ "$var1" == "start" ] || [ "$var1" == "stop" ]
then
#check config existence
if ! [ -r $configFile ]; then
   echo $confgFile " not found."
   echo $usage
   exit 1
fi

#create PID dir
if [ "$DATABUS_PID_DIR" = "" ]; then
  DATABUS_PID_DIR=/tmp/databus
fi
export _DATABUS_DAEMON_PIDFILE=$DATABUS_PID_DIR/databus.pid

#read the configFile
. $configFile

#config file values basic validation
if [ -z $CLUSTERS_TO_PROCESS ]; then
  echo "CLUSTERS_TO_PROCESS not defined in " $configFile
  exit 1
fi
if [ -z $DATABUS_CFG ]; then
  echo "DATABUS_CFG not defined in " $configFile
  exit 1
fi
if [ -z $ZK_CONNECT_STRING ]; then
  echo "ZK_CONNECT_STRING not defined in " $configFile
  exit 1
fi
if [ -z $LOG4J_PROPERTIES ]; then
  echo "LOG4J_PROPERTIES not defined in " $configFile
  exit 1
fi

fi

#set classpath
export CLASSPATH=jars/commons-httpclient-1.0.jar:jars/jackson-core-asl-1.5.5.jar:jars/jackson-mapper-asl-0.9.7.jar:jars/commons-cli-1.1.jar:jars/hadoop-distcp-0.1-SNAPSHOT.jar:jars/databus-1.0.jar:jars/commons-logging-1.1.1.jar:jars/hadoop-core-0.20.2-cdh3u0.jar:jars/log4j-1.2.16.jar:jars/zookeeper-recipes-lock.jar:jars/zookeeper-3.3.3.jar:jars/curator-client-0.6.1.jar:jars/curator-framework-0.6.1.jar:jars/curator-recipes-0.6.1.jar:jars/guava-r09.jar
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

    echo starting DATABUS, logging to logfile defined in $LOG4J_PROPERTIES

   nohup java "-Ddatabus.log4j.properties.file=${LOG4J_PROPERTIES}" \
    -cp "$CLASSPATH" com.inmobi.databus.Databus $CLUSTERS_TO_PROCESS $DATABUS_CFG $ZK_CONNECT_STRING 2>&1 &
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

  (collapse)

     export TEST_CLASSPATH=$CLASSPATH:jars/databus-1.0-tests.jar
     hdfsName=$var2
     dir=$var3
     java -cp "$TEST_CLASSPATH" com.inmobi.databus.CollapseFilesInDir $hdfsName $dir
     ;;

  (*)
    echo $usage
    exit 1
    ;;

esac



