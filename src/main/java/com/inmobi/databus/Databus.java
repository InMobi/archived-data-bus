package com.inmobi.databus;

import com.inmobi.databus.distcp.MergedStreamService;
import com.inmobi.databus.distcp.MirrorStreamService;
import com.inmobi.databus.local.LocalStreamService;
import com.inmobi.databus.purge.DataPurgerService;
import com.inmobi.databus.zookeeper.CuratorLeaderManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Databus implements Service {
  private static Logger LOG = Logger.getLogger(Databus.class);
  private DatabusConfig config;

  public Set<String> getClustersToProcess() {
    return clustersToProcess;
  }

  private final Set<String> clustersToProcess;
  private final List<AbstractService> services = new ArrayList<AbstractService>();


  public Databus(DatabusConfig config, Set<String> clustersToProcess) {
    this.config = config;
    this.clustersToProcess = clustersToProcess;
  }

  public DatabusConfig getConfig() {
    return config;
  }

  private void init() throws Exception {
    for (Cluster cluster : config.getClusters().values()) {
      if (!clustersToProcess.contains(cluster.getName())) {
        continue;
      }
      //Start LocalStreamConsumerService for this cluster if it's the source of any stream
      if (cluster.getSourceStreams().size() > 0) {
        services.add(new LocalStreamService(config, cluster));
      }

      List<Cluster> mergedStreamRemoteClusters = new ArrayList<Cluster>();
      List<Cluster> mirroredRemoteClusters = new ArrayList<Cluster>();
      for (DestinationStream cStream : cluster.getDestinationStreams().values()) {
        //Start MergedStreamConsumerService instances for this cluster for each cluster
        //from where it has to fetch a partial stream and is hosting a primary stream
        //Start MirroredStreamConsumerService instances for this cluster for each cluster
        //from where it has to mirror mergedStreams

        for (String cName : config.getSourceStreams().get(cStream.getName())
                .getSourceClusters()) {
          if (cStream.isPrimary())
            mergedStreamRemoteClusters.add(config.getClusters().get(cName));
        }
        if (!cStream.isPrimary())  {
          Cluster primaryCluster = config.getPrimaryClusterForDestinationStream(cStream.getName());
          if (primaryCluster != null)
            mirroredRemoteClusters.add(primaryCluster);
        }
      }


      for (Cluster remote : mergedStreamRemoteClusters) {
        services.add(new MergedStreamService(config, remote, cluster));
      }
      for (Cluster remote : mirroredRemoteClusters) {
        services.add(new MirrorStreamService(config, remote, cluster));
      }
    }

    //Start a DataPurgerService for this Cluster/Clusters to process
    Iterator<String> it = clustersToProcess.iterator();
    while(it.hasNext()) {
      String  clusterName = it.next();
      Cluster cluster =  config.getClusters().get(clusterName);
      LOG.info("Starting Purger for Cluster [" + clusterName + "]");
      //Start a purger per cluster
      services.add(new DataPurgerService(config, cluster));
    }
  }

  @Override
  public synchronized void stop() throws Exception {
    for (AbstractService service : services) {
      service.stop();
    }
  }

  @Override
  public synchronized void join() throws Exception {
    for (AbstractService service : services) {
      service.join();
    }
  }

  @Override
  public synchronized void start() throws Exception {
    init();
    for (AbstractService service : services) {
      service.start();
    }
    //Block this method to avoid losing leadership
    //of current work
    for (AbstractService service : services) {
      service.join();
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      if (args.length != 3 ) {
        LOG.warn("Usage: com.inmobi.databus.Databus <clustersToProcess> <configFile> <zkconnectstring>");
        return;
      }
      String clustersStr = args[0].trim();
      String[] clusters = clustersStr.split(",");
      String databusconfigFile = args[1].trim();
      String zkConnectString = args[2].trim();
      DatabusConfigParser configParser =
              new DatabusConfigParser(databusconfigFile);
      DatabusConfig config = configParser.getConfig();
      StringBuffer databusClusterId = new StringBuffer();
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
          databusClusterId.append(c);
          databusClusterId.append("_");
        }
      }
      Databus databus = new Databus(config, clustersToProcess);
      LOG.info("Starting CuratorLeaderManager for eleader election ");
      CuratorLeaderManager curatorLeaderManager =  
          new CuratorLeaderManager(databus, databusClusterId.toString(), 
              zkConnectString);
      curatorLeaderManager.start();

    }
    catch (Exception e) {
      LOG.warn("Error in starting Databus daemon", e);
      throw new Exception(e);
    }
  }

}
