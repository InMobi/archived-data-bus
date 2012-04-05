package com.inmobi.databus.consumer;

import com.inmobi.databus.Cluster;

public class PartitionId {

  private final Cluster cluster;
  private final String collector;
  
  PartitionId(Cluster cluster, String collector) {
    this.cluster = cluster;
    this.collector = collector;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public String getCollector() {
    return collector;
  }
}
