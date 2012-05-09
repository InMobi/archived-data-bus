package com.inmobi.databus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.testng.annotations.AfterSuite;

public abstract class TestMiniClusterUtil {
  
  private static MiniDFSCluster dfscluster = null;

  private static MiniMRCluster mrcluster = null;

  private static Configuration CONF = new Configuration();

  // Number of datanodes in the cluster

  private static final int DATANODE_COUNT = 2;

  private static final int TASKTRACKER_COUNT = 1;

  private static final int NUM_MR_DIRS = 1;

  public void setup() throws Exception{
    //Set the Test Directory as MiniClusterUtil so as to have everything in common place
    System.setProperty("test.build.data", "build/test/data/MiniClusterUtil");
    System.setProperty("hadoop.log.dir", "build/test/test-logs"); 
    
    if (dfscluster == null) {
      dfscluster = new MiniDFSCluster(CONF, DATANODE_COUNT, true, null);
      dfscluster.waitActive();
    }

    if (mrcluster == null) {
    mrcluster = new MiniMRCluster(TASKTRACKER_COUNT, 
        dfscluster.getFileSystem().getUri().toString(), 
        NUM_MR_DIRS);
    }
  }

  @AfterSuite
  public void cleanup() throws Exception {
    if(dfscluster!=null)
      dfscluster.shutdown();
    
    if(mrcluster!=null)
      mrcluster.shutdown();	
    
    dfscluster=null;
    mrcluster=null;
  }
  
  public JobConf CreateJobConf() {
    return mrcluster.createJobConf();
  }
  
  public FileSystem GetFileSystem() throws IOException{
    return dfscluster.getFileSystem();
  }
  
  public void RunJob(JobConf conf) throws IOException{
    JobClient.runJob(conf);
  }

}
