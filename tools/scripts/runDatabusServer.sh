#!/bin/bash

if [ $# -ne 2 ]
then
echo "Usage :: runDatabusServer.sh <cluster-name> <databus.xml>"
exit;
fi

clustername=$1
databusxml=$2
echo "Starting databus server for Cluster " $clustername " and databus config " $databusxml

#java -cp commons-httpclient-1.0.jar:jackson-core-asl-1.5.5.jar:jackson-mapper-asl-0.9.7.jar:commons-cli-1.1.jar:hadoop-distcp-0.1-SNAPSHOT.jar:com.inmobi.databus-1.0.jar:commons-logging-1.1.1.jar:hadoop-core-0.20.2-cdh3u0.jar:log4j-1.2.16.jar com.inmobi.databus.Databus $clustername $databusxml