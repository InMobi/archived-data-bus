package com.inmobi.databus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

public class TestFSCheckpointProvider {
  private static final Path testDir = new Path("/tmp/fscheckpoint"); 
  private String cpKey ="mykey";
  private String checkpoint = "my Checkpoint";
  FileSystem localFs;
  FileSystem dfs;
  MiniDFSCluster dfsCluster;
  Configuration conf = new Configuration();

  @BeforeSuite
  public void setup() throws IOException {
    localFs = FileSystem.getLocal(conf);
    dfsCluster = new MiniDFSCluster(conf, 1, true,null);
    dfs = dfsCluster.getFileSystem();
  }

  @AfterSuite
  public void cleanup() throws IOException {
    // delete test dir
    localFs.delete(testDir, true);
    dfs.delete(testDir, true);
    dfsCluster.shutdown();
  }
  
  @Test
  public void testWithLocal() throws IOException {
    testWithFS(localFs);
  }
  
  @Test
  public void testWithHDFS() throws IOException {
    testWithFS(dfs);
  }

  public void testWithFS(FileSystem fs) throws IOException {
     FSCheckpointProvider cpProvider = new FSCheckpointProvider(
         testDir.makeQualified(fs).toString());
     Assert.assertTrue(fs.exists(testDir));
     Assert.assertNull(cpProvider.read(cpKey));
     cpProvider.checkpoint(cpKey, checkpoint.getBytes());
     Assert.assertEquals(new String(cpProvider.read(cpKey)), checkpoint);
     cpProvider.close();
  }

}
