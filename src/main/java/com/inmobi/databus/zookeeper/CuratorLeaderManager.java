package com.inmobi.databus.zookeeper;

import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.Service;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryOneTime;

public class CuratorLeaderManager implements LeaderSelectorListener {
  private static final Log LOG = LogFactory.getLog(CuratorLeaderManager.class);
  private final Service databus;
  private final String databusClusterId;
  private final String zkConnectString;
  private CuratorFramework client;
  private LeaderSelector leaderSelector;

  public CuratorLeaderManager(Service databus, String databusClusterId, 
      String zkConnectString){
    this.databus = databus;
    this.databusClusterId = databusClusterId;
    this.zkConnectString = zkConnectString;
  }

  public void start() throws Exception {
    String zkPath = "/databus/" + databusClusterId; 
    this.client = CuratorFrameworkFactory.newClient(zkConnectString,
        new RetryOneTime(3));
    this.leaderSelector = new LeaderSelector(client, zkPath, this);
    leaderSelector.setId(InetAddress.getLocalHost().getHostName());
    connect();
  }

  private void connect() throws Exception {
    client.start();// connect to ZK
    LOG.info("becomeLeader :: connect to ZK [" + zkConnectString + "]");
    leaderSelector.start();
    LOG.info("started the LeaderSelector");
  }

  @Override
  public void takeLeadership(CuratorFramework curatorFramework)
      throws Exception {
    LOG.info("Became Leader..starting to do work");
 // This method shouldn't return till you want to release leadership
    databus.start();
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework,
      ConnectionState connectionState) {
    if (connectionState == ConnectionState.LOST) {
      try {
        databus.stop();
      } catch (Exception e1) {
        LOG.warn("Error while stopping databus service", e1);
      }
      LOG.info("Releasing leadership..connection LOST");
      try {
        LOG.info("Trying reconnect..");
        connect();
      } catch (Exception e) {
        LOG.warn("Error in attempting to connect to ZK after releasing leadership", e);
      }
    }
  }

  

}
