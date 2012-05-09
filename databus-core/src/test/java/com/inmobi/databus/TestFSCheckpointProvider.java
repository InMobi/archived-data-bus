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

public class TestFSCheckpointProvider extends TestMiniClusterUtil{
  private static final Path testDir = new Path("/tmp/fscheckpoint"); 
  private String cpKey ="mykey";
  private String checkpoint = "my Checkpoint";
  FileSystem localFs;
  
  @BeforeSuite
  public void setup() throws Exception {
    super.setup();
    localFs = FileSystem.getLocal(new Configuration());
  }
  
  @Test
  public void testWithLocal() throws IOException {
    testWithFS(localFs);
    localFs.delete(testDir, true);
  }
  
  @Test
  public void testWithHDFS() throws IOException {
    FileSystem dfs = GetFileSystem();
    testWithFS(dfs);
    dfs.delete(testDir, true);
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
