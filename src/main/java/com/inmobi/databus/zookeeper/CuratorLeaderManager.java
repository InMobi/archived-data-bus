
package com.inmobi.databus.zookeeper;


import com.inmobi.databus.*;
import com.netflix.curator.framework.*;
import com.netflix.curator.framework.recipes.leader.*;
import com.netflix.curator.framework.state.*;
import com.netflix.curator.retry.*;
import org.apache.commons.logging.*;

import java.net.*;
import java.util.*;

public class CuratorLeaderManager implements LeaderSelectorListener{
  private static final Log LOG = LogFactory.getLog(CuratorLeaderManager.class);
  private Databus databus;
  CuratorFramework client = null;
  LeaderSelector leaderSelector = null;
  String zkConnectString;
  String zkPath;
  String myClusterName;

  public CuratorLeaderManager(Databus databus) throws Exception{
    this.databus = databus;
    setClusterName();
    zkPath = getZkPath();
    DatabusConfig.Cluster myCluster = databus.getConfig().getClusters().get(myClusterName);
    //Use Cluster specific ZKConnection String
    //if it's not present then use the global Config
    zkConnectString = myCluster.getZkConnectionString() != null ? myCluster.getZkConnectionString() :
            databus.getConfig().getZkConnectionString();
    if (zkConnectString == null)
      throw new Exception("ZooKeeperConnectionString not defined in databus.xml");
  }

  private void setClusterName() {
    Iterator it = databus.getClustersToProcess().iterator();
    //TODO: better way to get current clusterName
    myClusterName = (String) it.next();
    LOG.info("MyClusterName [" + myClusterName + "]");
  }

  private String getZkPath() {
    String address;
    String zkPath = "/databus";
    zkPath = zkPath + "/" + myClusterName;
    return zkPath;
  }

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    LOG.info("Became Leader..starting to do work");
    doWork();
  }

  private void doWork() throws Exception{
    //This method shouldn't return till you want to release leadership
    databus.startDatabusWork();

  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
    if (connectionState == ConnectionState.LOST) {
      releaseLeaderShip();
      LOG.info("Releasing leadership..connection LOST");
      try {
        LOG.info("Trying to become leader");
        becomeLeader();
      }
      catch (Exception e) {
        LOG.warn("Error in attempting to becoming leader after releasing leadership");
        LOG.warn(e.getMessage());
        LOG.warn(e);
      }
    }
  }

  private void releaseLeaderShip(){
    databus.stop();
  }

  private CuratorFramework getCuratorInstance() throws  Exception{
    if (client == null) {

      synchronized (this) {
        client =  CuratorFrameworkFactory.newClient(zkConnectString,
                new RetryOneTime(3));
        return  client;
      }
    }
    else
      return client;
  }

  private LeaderSelector getLeaderSelector() throws  Exception{
    if (leaderSelector == null)
      leaderSelector = new LeaderSelector(client, zkPath , new CuratorLeaderManager(databus));
    return  leaderSelector;

  }

  public void becomeLeader() throws Exception{
    client  =  getCuratorInstance();
    client.start();// connect to ZK
    LOG.info("becomeLeader :: connect to ZK [" + zkConnectString + "]");
    leaderSelector = getLeaderSelector();
    leaderSelector.setId(InetAddress.getLocalHost().getHostName());
    leaderSelector.start();
    LOG.info("started the LeaderSelector"); }


  public static void main(String[] args) {
    /*
    try {
    LOG.info("Starting CuratorLeaderManager for eleader election ");

    CuratorLeaderManager curatorLeaderManager =  new CuratorLeaderManager();
    curatorLeaderManager.becomeLeader();
    }
    catch (Exception e) {
      LOG.warn(e.getMessage());
      LOG.warn(e);

    }
    */
  }

}

