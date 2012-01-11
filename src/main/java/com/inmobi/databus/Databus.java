package com.inmobi.databus;

import com.inmobi.databus.DatabusConfig.*;
import com.inmobi.databus.consume.*;
import com.inmobi.databus.distcp.*;
import com.inmobi.databus.purge.*;
import com.inmobi.databus.zookeeper.*;
import org.apache.log4j.*;

import java.util.*;

public class Databus {
  private static Logger LOG = Logger.getLogger(Databus.class);
  private DatabusConfig config;

  public Set<String> getClustersToProcess() {
    return clustersToProcess;
  }

  private final Set<String> clustersToProcess;
  private final List<AbstractCopier> copiers = new ArrayList<AbstractCopier>();


  public Databus(DatabusConfig config, Set<String> clustersToProcess) {
    this.config = config;
    this.clustersToProcess = clustersToProcess;
  }

  public DatabusConfig getConfig() {
    return config;
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
          remoteClustersToFetch.add(config.getClusters().get(cName));
        }
      }
      for (Cluster remote : remoteClustersToFetch) {
        copiers.add(new RemoteCopier(config, remote, cluster));
      }
    }
    Iterator it = clustersToProcess.iterator();
    while(it.hasNext()) {
      String  clusterName = (String) it.next();
      Cluster cluster =  config.getClusters().get(clusterName);
      LOG.info("Starting Purger for Cluster [" + clusterName + "]");
      //Start a purger per cluster
      copiers.add(new DataPurger(config, cluster));
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

  public void startDatabusWork() throws Exception{
    startDatabus();
    //Block this method to avoid losing leadership
    //of current work
    for (AbstractCopier copier : copiers) {
      copier.join();
    }
  }

  private void startDatabus() throws Exception{
    init();
    start();
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
      DatabusConfig config = new DatabusConfig(configParser.getRootDir(), configParser.getZkConnectString(),
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
      if (clustersToProcess.size() == 1 &&
              !"ALL".equalsIgnoreCase(clusters[0])) {
        //Elect a leader and then start
        LOG.info("Starting CuratorLeaderManager for eleader election ");
        CuratorLeaderManager curatorLeaderManager =  new CuratorLeaderManager(databus);
        curatorLeaderManager.becomeLeader();
      }
      else {
        //Running in simulated mode don't use ZK
        databus.startDatabus();
      }

    }
    catch (Exception e) {
      LOG.warn(e.getMessage());
      LOG.warn(e);
      throw new Exception(e);
    }
  }

}
