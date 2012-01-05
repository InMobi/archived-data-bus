package com.inmobi.databus.zookeeper;


import com.netflix.curator.framework.*;
import com.netflix.curator.framework.recipes.leader.*;
import com.netflix.curator.framework.state.*;
import com.netflix.curator.retry.*;
import org.apache.commons.logging.*;

import java.net.*;

public class CuratorLeaderManager implements LeaderSelectorListener{
  private static final Log LOG = LogFactory.getLog(CuratorLeaderManager.class);
  private volatile Thread     ourThread;
  private volatile boolean isLeader = false;

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    ourThread = Thread.currentThread();
    isLeader = true;
    LOG.info("Became Leader..starting to do work");
    doWork();
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
    if (connectionState == ConnectionState.LOST) {
      //shutdown current work
     // curatorFramework.close();
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
    isLeader = false;
  }

  private void doWork() {
    while(!ourThread.interrupted() && isLeader) {
      LOG.info("CuratorLeaderManager:: Doing some work");
      try {
        Thread.sleep(10000);
      }
      catch (InterruptedException e) {
        LOG.info("i am interrupted");
        break;
      }
    }
  }

  private void becomeLeader() throws Exception{

    CuratorFramework client  = CuratorFrameworkFactory.newClient("gs1104.grid.corp.inmobi.com:2181",
            new RetryOneTime(3));
    client.start();// connect to ZK
    LOG.info("becomeLeader :: connect to ZK");
    LeaderSelector leaderSelector = new LeaderSelector(client, "/databus", new CuratorLeaderManager());
    leaderSelector.setId(InetAddress.getLocalHost().getHostName());
    leaderSelector.start();
    LOG.info("started the LeaderSelector");
  }

  public static void main(String[] args) {
    try {
    LOG.info("Starting CuratorLeaderManager for eleader election ");
    CuratorLeaderManager curatorLeaderManager =  new CuratorLeaderManager();
    curatorLeaderManager.becomeLeader();
    }
    catch (Exception e) {
      LOG.warn(e.getMessage());
      LOG.warn(e);

    }
  }

}
