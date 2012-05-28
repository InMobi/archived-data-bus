/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.inmobi.databus;

import com.inmobi.databus.distcp.MergedStreamService;
import com.inmobi.databus.distcp.MirrorStreamService;
import com.inmobi.databus.local.LocalStreamService;
import com.inmobi.databus.purge.DataPurgerService;
import com.inmobi.databus.utils.SecureLoginUtil;
import com.inmobi.databus.zookeeper.CuratorLeaderManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Databus implements Service, DatabusConstants {
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
        services.add(new LocalStreamService(config, cluster,
         new FSCheckpointProvider(cluster.getCheckpointDir())));
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
  public void stop() throws Exception {
    for (AbstractService service : services) {
      LOG.info("Stopping [" + service.getName() + "]");
      service.stop();
    }
    LOG.info("Databus Shutdown complete..");
  }

  @Override
  public void join() throws Exception {
    for (AbstractService service : services) {
      LOG.info("Waiting for [" + service.getName() + "] to finish");
      service.join();
    }
  }

  @Override
  public void start() throws Exception{
    try {
      init();
      for (AbstractService service : services) {
        service.start();
      }
    }
    catch (Exception e) {
      LOG.warn("Error is starting service", e);
    }
    //Block this method to avoid losing leadership of current work
    join();
    //If all threads are finished release leadership
    System.exit(0);
  }

  public static void main(String[] args) throws Exception {
    try {
      if (args.length != 1 ) {
        LOG.error("Usage: com.inmobi.databus.Databus <databus.cfg>");
        System.exit(-1);
      }
      String cfgFile = args[0].trim();
      Properties prop = new Properties();
      prop.load(new FileReader(cfgFile));

      String log4jFile = prop.getProperty(LOG4J_FILE);
      if (log4jFile != null && new File(log4jFile).exists()) {
        PropertyConfigurator.configureAndWatch(log4jFile);
        LOG.info("Log4j Property File [" + log4jFile + "]");
      }

      String clustersStr = prop.getProperty(CLUSTERS_TO_PROCESS);
      String[] clusters = clustersStr.split(",");
      String databusConfigFile = prop.getProperty(DATABUS_XML);
      String zkConnectString = prop.getProperty(ZK_ADDR);
      String principal = prop.getProperty(KRB_PRINCIPAL);
      String keytab = prop.getProperty(KEY_TAB_FILE);

      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Security enabled, trying kerberoes login principal [" +
        principal + "] keytab [" + keytab + "]");
        //krb enabled
        if (principal != null && keytab != null) {
          SecureLoginUtil.login(KRB_PRINCIPAL, principal, KEY_TAB_FILE, keytab);
         }
        else  {
          LOG.error("Kerberoes principal/keytab not defined properly in " +
          "databus.cfg");
          System.exit(-1);
        }
      }

      DatabusConfigParser configParser =
              new DatabusConfigParser(databusConfigFile);
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
      final Databus databus = new Databus(config, clustersToProcess);
      LOG.info("Starting CuratorLeaderManager for eleader election ");
      CuratorLeaderManager curatorLeaderManager =
              new CuratorLeaderManager(databus, databusClusterId.toString(),
                      zkConnectString);
      curatorLeaderManager.start();
      Signal.handle(new Signal("INT"), new SignalHandler() {
        @Override
        public void handle(Signal signal) {
          try {
            LOG.info("Starting to stop databus...");
            databus.stop();
          }
          catch (Exception e) {
            LOG.warn("Error in shutting down databus", e);
          }
        }
      });
    }
    catch (Exception e) {
      LOG.warn("Error in starting Databus daemon", e);
      throw new Exception(e);
    }
  }

}
