package com.inmobi.databus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Stream {
  
  private enum STREAM_TYPE {
    SOURCE, DESTINATION;
  }
  private final String streamName;
  private final Map<STREAM_TYPE, Set<StreamCluster>> Clusters = new HashMap<STREAM_TYPE, Set<StreamCluster>>();

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
    Set<StreamCluster> clusterSet = Clusters.get(STREAM_TYPE.SOURCE);

    StreamCluster newStreamCluster = new SourceStreamCluster(retentionInHours,
        cluster);
    if (clusterSet != null) {
      clusterSet.add(newStreamCluster);
    } else {
      clusterSet = new HashSet<StreamCluster>();
      clusterSet.add(newStreamCluster);
      Clusters.put(STREAM_TYPE.SOURCE, clusterSet);
    }
    cluster.addSourceStream(getName());
  }

  public void addDestinationCluster(int retentionInHours, Cluster cluster,
      Boolean isPrimary) {
    Set<StreamCluster> clusterSet = Clusters.get(STREAM_TYPE.DESTINATION);

    StreamCluster newStreamCluster = new DestinationStreamCluster(
        retentionInHours, cluster, isPrimary);
    if (clusterSet != null) {
      clusterSet.add(newStreamCluster);
    } else {
      clusterSet = new HashSet<StreamCluster>();
      clusterSet.add(newStreamCluster);
      Clusters.put(STREAM_TYPE.DESTINATION, clusterSet);
    }
    cluster.addDestinationStream(getName());
  }

  public Set<StreamCluster> getDestinationStreamClusters() {
    return Clusters.get(STREAM_TYPE.DESTINATION);
  }

  public Set<StreamCluster> getSourceStreamClusters() {
    return Clusters.get(STREAM_TYPE.SOURCE);
  }

  public Cluster getPrimaryDestinationCluster() {
    Set<StreamCluster> primaryDestinationCluster = getDestinationStreamClusters();
    if(primaryDestinationCluster!=null) {
      Iterator<StreamCluster> cluster = primaryDestinationCluster.iterator();
      while (cluster.hasNext()) {
        DestinationStreamCluster streamCluster = (DestinationStreamCluster) cluster
            .next();
        if (streamCluster.isPrimary()) {
          return streamCluster.getCluster();
        }
      }
    }
    return null;
  }
  
  public Set<DestinationStreamCluster> getMirroredClusters() {
    Set<DestinationStreamCluster> destStreamClusters = new HashSet<DestinationStreamCluster>();
    for (Iterator<StreamCluster> destStreamClustersItr = getDestinationStreamClusters()
        .iterator(); destStreamClustersItr.hasNext();) {
      DestinationStreamCluster streamCluster = (DestinationStreamCluster) destStreamClustersItr
          .next();
      if (!streamCluster.isPrimary())
        destStreamClusters.add(streamCluster);
      }
    return destStreamClusters;
  }
}