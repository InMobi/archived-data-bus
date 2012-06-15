package com.inmobi.databus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Stream {
  
  private enum STREAM_TYPE {
    SOURCE, DESTINATION;
  }
  private final String streamName;
  private final Map<STREAM_TYPE, Set<StreamCluster>> clusters = new HashMap<STREAM_TYPE, Set<StreamCluster>>();

  public class StreamCluster {
    private final int retentionInHours;
    private final Cluster cluster;
    
    StreamCluster(int retentionInHours, Cluster cluster) {
      this.retentionInHours = retentionInHours;
      this.cluster = cluster;
    }

    public int getRetentionPeriod() {
      return retentionInHours;
    }

    public Cluster getCluster() {
      return cluster;
    }
  }

  public class SourceStreamCluster extends StreamCluster {
    SourceStreamCluster(int retentionInHours, Cluster cluster) {
      super(retentionInHours, cluster);
    }
  }
  
  public class DestinationStreamCluster extends StreamCluster {
    private final Boolean isPrimary;

    DestinationStreamCluster(int retentionInHours, Cluster cluster,
        Boolean isPrimary) {
      super(retentionInHours, cluster);
      this.isPrimary = isPrimary;
    }

    public boolean isPrimary() {
      return isPrimary;
    }
  }

  public Stream(String name) {
    this.streamName = name;
  }
  
  public String getName() {
    return streamName;
  }

  public void addSourceCluster(int retentionInHours, Cluster cluster) {
    Set<StreamCluster> clusterSet = clusters.get(STREAM_TYPE.SOURCE);

    StreamCluster newStreamCluster = new SourceStreamCluster(retentionInHours,
        cluster);
    if (clusterSet != null) {
      clusterSet.add(newStreamCluster);
    } else {
      clusterSet = new HashSet<StreamCluster>();
      clusterSet.add(newStreamCluster);
      clusters.put(STREAM_TYPE.SOURCE, clusterSet);
    }
    cluster.addSourceStream(getName());
  }

  public void addDestinationCluster(int retentionInHours, Cluster cluster,
      Boolean isPrimary) {
    Set<StreamCluster> clusterSet = clusters.get(STREAM_TYPE.DESTINATION);

    StreamCluster newStreamCluster = new DestinationStreamCluster(
        retentionInHours, cluster, isPrimary);
    if (clusterSet != null) {
      clusterSet.add(newStreamCluster);
    } else {
      clusterSet = new HashSet<StreamCluster>();
      clusterSet.add(newStreamCluster);
      clusters.put(STREAM_TYPE.DESTINATION, clusterSet);
    }
    cluster.addDestinationStream(getName());
  }

  public Set<StreamCluster> getDestinationStreamClusters() {
    return clusters.get(STREAM_TYPE.DESTINATION);
  }

  public Set<StreamCluster> getSourceStreamClusters() {
    return clusters.get(STREAM_TYPE.SOURCE);
  }

  public Cluster getPrimaryDestinationCluster() {
    Set<StreamCluster> primaryDestinationCluster = getDestinationStreamClusters();
    if(primaryDestinationCluster!=null) {
      for (StreamCluster cluster : primaryDestinationCluster) {
        DestinationStreamCluster streamCluster = (DestinationStreamCluster) cluster;
        if (streamCluster.isPrimary()) {
          return streamCluster.getCluster();
        }
      }
    }
    return null;
  }
  
  public Set<DestinationStreamCluster> getMirroredClusters() {
    Set<DestinationStreamCluster> destStreamClusters = new HashSet<DestinationStreamCluster>();
    for (StreamCluster destStreamClustersItr : getDestinationStreamClusters()) {
      DestinationStreamCluster streamCluster = (DestinationStreamCluster) destStreamClustersItr;
      if (!streamCluster.isPrimary())
        destStreamClusters.add(streamCluster);
      }
    return destStreamClusters;
  }
}