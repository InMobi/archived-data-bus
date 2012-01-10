/*
package com.inmobi.databus.zookeeper;


import org.apache.zookeeper.recipes.lock.*;

public class LeaderManager implements LockListener {
  private static final Log LOG = LogFactory.getLog(LeaderManager.class);
  ZooKeeper zooKeeper;
  WriteLock leaderLock;

  public void setZooKeeper(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
  }

  public void setLeaderLock(WriteLock leaderLock) {
    this.leaderLock = leaderLock;
  }

  @Override
  public void lockAcquired() {
    LOG.info("Lock Acquired..starting all Threads");

  }

  @Override
  public void lockReleased() {
    LOG.info("Lock Released...releasing all Threads");

  }

  private void doWork() {
    while(!Thread.interrupted() && leaderLock.isOwner()) {
      LOG.info("Doing some work");
      try {
        Thread.sleep(10000);
      }
      catch (InterruptedException e) {
        LOG.info("i am interrupted");
        break;
      }
    }
    //wait to become leader
    stopThreads();
    waitToBecomeLeader();
  }

  private void startThreads() {

    doWork();
  }

  private void stopThreads() {

  }

  private void waitToBecomeLeader(){
    while (!leaderLock.isOwner())    {
      LOG.info("Waiting to become Leader, sleep 2 seconds before next retry");
      try {
        Thread.sleep(2000);
      }
      catch (Exception e) {
        LOG.info(e);
      }
    }
    if(leaderLock.isOwner())
      startThreads();
  }

  private void becomeLeader() throws  Exception{

    leaderLock.lock();
    if(leaderLock.isOwner())    {
      LOG.info("I am the leader starting all Threads");
      startThreads();
    }
    else  {
      LOG.info("Couldn't become leader doing nothing, waiting to become leader");
      try {
        waitToBecomeLeader();
      }
      catch (Exception e) {
        LOG.warn(e);
        //retry becoming leader
        waitToBecomeLeader();
      }
    }

  }
  public static void main(String[] args) throws Exception{
    ZooKeeper zooKeeper = new ZooKeeper("gs1104.grid.corp.inmobi.com:2181", 3000, null);
    LeaderManager leaderManager = new LeaderManager();
    Stat stat = zooKeeper.exists("/databus", false);
    if (stat == null)
      zooKeeper.create("/databus", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    WriteLock leaderLock = new WriteLock(zooKeeper, "/databus", ZooDefs.Ids.OPEN_ACL_UNSAFE, leaderManager);
    leaderManager.setZooKeeper(zooKeeper);
    leaderManager.setLeaderLock(leaderLock);
    leaderManager.becomeLeader();



  }


}
*/
