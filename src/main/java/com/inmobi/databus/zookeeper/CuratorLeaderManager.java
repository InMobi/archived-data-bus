
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


  public CuratorLeaderManager(Databus databus) {
    this.databus = databus;
    zkPath = getZkPath();
  }

  private String getZkPath() {
    String address;
    String zkPath = "/databus";
    Iterator it = databus.getClustersToProcess().iterator();
    String clusterName = (String) it.next();
    zkPath = zkPath + "/" + clusterName;
    return  zkPath;
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
        client =  CuratorFrameworkFactory.newClient(databus.getConfig().getZkConnectionString(),
                new RetryOneTime(3));
        return  client;
      }
    }
    else
      return client;
  }

  private LeaderSelector getLeaderSelector() {
    if (leaderSelector == null)
      leaderSelector = new LeaderSelector(client, zkPath , new CuratorLeaderManager(databus));
    return  leaderSelector;

  }

  public void becomeLeader() throws Exception{
    client  =  getCuratorInstance();
    client.start();// connect to ZK
    LOG.info("becomeLeader :: connect to ZK [" + databus.getConfig().getZkConnectionString() + "]");
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


/*

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

 CuratorFramework client  = CuratorFrameworkFactory.newClient("gs1104.grid.corp.inmobi.com:2188",
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
*/