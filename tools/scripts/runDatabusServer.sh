#!/bin/bash

if [ $# -ne 3 ]
then
echo "Usage :jars/jars/:jars/ runDatabusServer.sh <cluster-name> <databus.xml>
<zkConnectString>"
exit;
fi

clustername=$1
databusxml=$2
zkString=$3
echo "Starting databus server for Cluster " $clustername " databus-config "
$databusxml " zkConnectString " $zkString

jars/commons-httpclient-1.0.jar:jars/jackson-core-asl-1.5.5.jar:jars/jackson-mapper-asl-0.9.7.jar:jars/commons-cli-1.1.jar:jars/hadoop-distcp-0.1-SNAPSHOT.jar:jars/databus-1.0.jar:jars/commons-logging-1.1.1.jar:jars/hadoop-core-0.20.2-cdh3u0.jar:jars/log4j-1.2.16.jar:jars/zookeeper-recipes-lock.jar:jars/zookeeper-3.3.3.jar:jars/curator-client-0.6.1.jar:jars/curator-framework-0.6.1.jar:jars/curator-recipes-0.6.1.jar:jars/guava-r09.jar


