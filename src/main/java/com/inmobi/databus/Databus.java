package com.inmobi.databus;

import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.DatabusConfig.ConsumeStream;
import com.inmobi.databus.consume.DataConsumer;
import com.inmobi.databus.distcp.RemoteCopier;
import org.apache.log4j.Logger;

import java.util.*;

public class Databus {
  private static Logger LOG = Logger.getLogger(Databus.class);
  private DatabusConfig config;
  private final Set<String> clustersToProcess;
  private final List<AbstractCopier> copiers = new ArrayList<AbstractCopier>();

  public Databus(DatabusConfig config, Set<String> clustersToProcess) {
    this.config = config;
    this.clustersToProcess = clustersToProcess;
  }

  public void init() throws Exception {
    for (Cluster cluster : config.getClusters().values()) {
      if (!clustersToProcess.contains(cluster.getName())) {
        continue;
      }
      if (cluster.getSourceStreams().size() > 0) {
        copiers.add(new DataConsumer(config, cluster));
      }
      List<Cluster> remoteClustersToFetch = new ArrayList<Cluster>();
      for (ConsumeStream cStream : cluster.getConsumeStreams().values()) {
        for (String cName : config.getStreams().get(cStream.getName())
            .getSourceClusters()) {
          if (!cName.equals(cluster.getName())) {
            remoteClustersToFetch.add(config.getClusters().get(cName));
          }
        }
      }
      for (Cluster remote : remoteClustersToFetch) {
        copiers.add(new RemoteCopier(config, remote, cluster));
      }
    }
  }

  public synchronized void start() {
    for (AbstractCopier copier : copiers) {
      copier.start();
    }
  }

  public synchronized void stop() {
    for (AbstractCopier copier : copiers) {
      copier.stop();
    }
    for (AbstractCopier copier : copiers) {
      copier.join();
    }
  }

  public static void main(String[] args) throws Exception {
		try {
    if (args.length != 1 && args.length != 2 ) {
      LOG.warn("Usage: com.inmobi.databus.Databus <clustersToProcess> <configFile>");
      return;
    }
    String clustersStr = args[0].trim();
    String[] clusters = clustersStr.split(",");
    
    String databusconfigFile = null;
    if (args.length == 2) {
      databusconfigFile = args[1].trim();
    }
    DatabusConfigParser configParser = 
        new DatabusConfigParser(databusconfigFile);
    Map<String, Cluster> clusterMap = configParser.getClusterMap();
    DatabusConfig config = new DatabusConfig(configParser.getRootDir(),
        configParser.getStreamMap(), clusterMap);
    
    Set<String> clustersToProcess = new HashSet<String>();
    if (clusters.length == 1 && "ALL".equalsIgnoreCase(clusters[0])) {
      for (Cluster c : config.getClusters().values()) {
        clustersToProcess.add(c.getName());
      }
    } else {
      for (String c : clusters) {
        if (config.getClusters().get(c) == null) {
          LOG.warn("Cluster name is not found in the config - " + c);
          return;
        }
        clustersToProcess.add(c);
      }
    }
    Databus databus = new Databus(config, clustersToProcess);
    
    databus.init();
    databus.start();
  }
		catch (Exception e) {
			LOG.warn(e.getMessage());
			LOG.warn(e);
			throw new Exception(e);
		}
	}

}
