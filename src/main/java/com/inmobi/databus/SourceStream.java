package com.inmobi.databus;

import java.util.Map;
import java.util.Set;

public class SourceStream {
  private final String name;
  //Map of ClusterName, Retention for a stream
  private final Map<String, Integer> sourceClusters;


  public SourceStream(String name, Map<String, Integer> sourceClusters) {
    super();
    this.name = name;
    this.sourceClusters = sourceClusters;
  }

  public int getRetentionInDays(String clusterName) {
    int clusterRetention = sourceClusters.get(clusterName).intValue();
    return clusterRetention;
  }

  public Set<String> getSourceClusters() {
    return sourceClusters.keySet();
  }

  public String getName() {
    return name;
  }


}
