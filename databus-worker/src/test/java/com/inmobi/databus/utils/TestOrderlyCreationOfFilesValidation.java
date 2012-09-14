package com.inmobi.databus.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.inmobi.databus.Cluster;

public class TestOrderlyCreationOfFilesValidation  {
  private static Logger LOG = Logger.getLogger(
      TestOrderlyCreationOfFilesValidation.class);
  private static String className = TestOrderlyCreationOfFilesValidation.class
      .getSimpleName();

  FileSystem fs;
  Set<Path> outoforderExpectedResults = new HashSet<Path>();
  List<Path> inorderExpectedResults = new ArrayList<Path>();
  String rootDirs [] = ("file:///tmp/test/" + className
    + "/1/,file:///tmp/test/" + className +"/2/").split(",");
  String baseDirs[] = "streams,streams_local".split(",");
  String [] emptyStream = "empty".split(",");
  String [] inorderStream = "inorder".split(",");
  String [] outoforderStream = "outoforder".split(",");
  long temptime = System.currentTimeMillis();
  
  @BeforeTest
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    createTestData();
  }
  
  @AfterTest
  public void cleanup() throws Exception {
    fs.delete(new Path(rootDirs[0]).getParent(), true);
  }
  
  public void createMinDirs(String listPath, boolean outofOrder, int dirNumber) 
      throws Exception {
    int milliseconds;
    if (outofOrder) {
      milliseconds = -60000;
    } else {
      milliseconds = 60000;
    }
    String date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + 
        dirNumber * milliseconds);
    int filesCount = 2;
    Path minDir = new Path(listPath, date);
    fs.mkdirs(minDir);
    for (int i =0; i < filesCount; i++) {
      createFilesData(fs, minDir, i );
    }
  if (outofOrder) {
    outoforderExpectedResults.add(minDir);
  }
}
  
  public void createTestData() throws Exception {
    String  listPath ;
    int dirCount =2;
    for (int j = 1; j <= dirCount; ++j) {
      for ( String rootdir : rootDirs) {
        for (String basedir : baseDirs) {
          for (String inorderstream : inorderStream) {
            listPath = rootdir + File.separator + basedir + File.separator +
                inorderstream + File.separator;
            createMinDirs(listPath, false, j );
          }
          for (String outoforderstream : outoforderStream) {
            listPath = rootdir + File.separator + basedir + File.separator +
                outoforderstream + File.separator;
            createMinDirs(listPath, false, j);
            Thread.sleep(1000);
            createMinDirs(listPath, true, j);
          }
        }
      }
      Thread.sleep(1000);
    }
  }

  public static Path createFilesData(FileSystem fs,
      Path minDir, int fileNum) throws Exception {
    Path filenameStr = new Path("file" + fileNum );
    Path path = new Path(minDir, filenameStr);
    LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
    FSDataOutputStream streamout = fs.create(path);
    streamout.writeBytes("Creating Test data for teststream "
        + path);
    streamout.close(); 
    return path;   
  }
  
  @Test
  public void TestOrderlyCreation() throws Exception {
    OrderlyCreationOfDirs obj = new OrderlyCreationOfDirs();
    Assert.assertEquals( inorderExpectedResults , obj.pathConstruction(rootDirs, 
        baseDirs , emptyStream));
    Assert.assertEquals( inorderExpectedResults , obj.pathConstruction(rootDirs, 
        baseDirs , inorderStream));
    List<Path> outOfOderDirs =  obj.pathConstruction(rootDirs, baseDirs, 
        outoforderStream);
    Iterator it = outoforderExpectedResults.iterator();
    while (it.hasNext()) {
      Assert.assertTrue(outOfOderDirs.contains(it.next()));
    }
    Assert.assertEquals(outOfOderDirs.size(), outoforderExpectedResults.size());
    String [] totalStreams ="empty,inorder,outoforder".split(",");
    List<Path> totalOutOfOderDirs = obj.pathConstruction(rootDirs, baseDirs,
        totalStreams );
    LOG.info(totalOutOfOderDirs.size());
    Iterator it1 = outoforderExpectedResults.iterator();
    while (it.hasNext()) {
      Assert.assertTrue(totalOutOfOderDirs.contains(it1.next()));
    }
    Assert.assertEquals(totalOutOfOderDirs.size(), outoforderExpectedResults.
        size());
  }
}

