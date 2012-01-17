package com.inmobi.databus;

import java.util.Map;

public class DatabusConfig {

  private final Map<String, Cluster> clusters;
  private final Map<String, SourceStream> streams;
  private final String zkConnectionString;

  public DatabusConfig( 
      String zkConnectionString, Map<String, SourceStream> streams,
                       Map<String, Cluster> clusterMap) {
    this.zkConnectionString = zkConnectionString;
    this.streams = streams;
    this.clusters = clusterMap;
  }

  public String getZkConnectionString() {
    return zkConnectionString;
  }

  public Cluster getPrimaryClusterForDestinationStream(String streamName) {
   for(Cluster cluster : getClusters().values()) {
     if (cluster.getDestinationStreams().containsKey(streamName)) {
       DestinationStream consumeStream = cluster.getDestinationStreams().get(streamName);
       if (consumeStream.isPrimary())
         return cluster;
     }
   }
    return null;
  }

  public Map<String, Cluster> getClusters() {
    return clusters;
  }

  public Map<String, SourceStream> getSourceStreams() {
    return streams;
  }

}
