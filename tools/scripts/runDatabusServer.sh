#!/bin/bash

if [ $# -ne 3 ]
then
echo "Usage :: runDatabusServer.sh <cluster-name> <databus.xml>
<zkConnectString>"
exit;
fi

clustername=$1
databusxml=$2
zkString=$3
echo "Starting databus server for Cluster " $clustername " databus-config "
$databusxml " zkConnectString " $zkString

java -cp commons-httpclient-1.0.jar:jackson-core-asl-1.5.5.jar:jackson-mapper-asl-0.9.7.jar:commons-cli-1.1.jar:hadoop-distcp-0.1-SNAPSHOT.jar:com.inmobi.databus-1.0.jar:commons-logging-1.1.1.jar:hadoop-core-0.20.2-cdh3u0.jar:log4j-1.2.16.jar:zookeeper-recipes-lock.jar:zookeeper-3.3.3.jar:curator-client-0.6.1.jar:curator-framework-0.6.1.jar:curator-recipes-0.6.1.jar:guava-r09.jar com.inmobi.databus.Databus $clustername $databusxml $zkString

