package com.inmobi.databus.utils;


import org.apache.log4j.Logger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import com.inmobi.databus.Cluster;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestOrderlyCreationOfFilesValidation  {

  private static Logger LOG = Logger.getLogger(
      TestOrderlyCreationOfFilesValidation.class);
  private static String className = TestOrderlyCreationOfFilesValidation.class
      .getSimpleName();

  FileSystem fs;
  List<Path> expectedResults = new ArrayList<Path>();
  String rootDirs [] = ("file:///tmp/test/" + className
    + "/1/,file:///tmp/test/" + className +"/2/").split(",");
  String baseDirs[] = "streams,streams_local".split(",");
  String [] emptyStream = "empty".split(",");
  String [] inorderStream = "inorder".split(",");
  String [] outoforderStream = "outoforder".split(",");
  
  @BeforeTest
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    createTestData();
  }
  
  @AfterTest
  public void cleanup() throws Exception {
    fs.delete(new Path(rootDirs[0]).getParent(), true);
  }
  
  public void createMinDirs(String listPath, boolean outofOrder) 
      throws Exception {
    int milliseconds;
    if (outofOrder) {
      milliseconds = -60000;
    } else {
      milliseconds = 60000;
    }
    for ( int i = 0; i < 5; i++) {
      String date = Cluster.getDateAsYYYYMMDDHHMNPath(System.currentTimeMillis()
          + i*milliseconds);
      createFilesData(fs, listPath + date, 2 );
    }  
  }
  
  public void createTestData() throws Exception {
    String  listPath = rootDirs[0] + baseDirs[0] + File.separator + 
        inorderStream[0] + File.separator ;
    createMinDirs(listPath, false);
    listPath = rootDirs[0] + baseDirs[0] + File.separator + outoforderStream[0]
        + File.separator ;
    createMinDirs(listPath, false);
    createMinDirs(listPath, true);
  }

  public static List<String> createFilesData(FileSystem fs,
      String pathName, int filesCount) throws Exception {
    Path createPath = new Path(pathName);
    fs.mkdirs(createPath);
    List<String> filesList = new ArrayList<String>();

    for (int j = 0; j < filesCount; ++j) {
      Thread.sleep(1000);
      String filenameStr = new String("file" + j);
      filesList.add(j, filenameStr);
      Path path = new Path(createPath, filesList.get(j));
      LOG.debug("Creating Test Data with filename [" + filesList.get(j) + "]");
      FSDataOutputStream streamout = fs.create(path);
      streamout.writeBytes("Creating Test data for teststream "
          + filesList.get(j));
      streamout.close();
    }
    return filesList;
  }
  
  public void createOutputData() {
    String date;
    for (int i = 4; i >=1; i-- ) {
      date = Cluster.getDateAsYYYYMMDDHHMNPath(System.currentTimeMillis()
          - i * 60000);
      expectedResults.add(new Path( "file:/tmp/test/" +
      		"TestOrderlyCreationOfFilesValidation/1/streams/outoforder/" +
      		date));
    }
  }

  @Test
  public void TestOrderlyCreation() throws Exception {
    OrderlyCreationOfDirsForDiffStreams obj = new 
        OrderlyCreationOfDirsForDiffStreams();
    Assert.assertEquals( expectedResults , obj.pathConstruction(rootDirs, 
        baseDirs , emptyStream));
    Assert.assertEquals( expectedResults , obj.pathConstruction(rootDirs, 
        baseDirs , inorderStream));
    createOutputData();
    List<Path> outOfOderDirs =  obj.pathConstruction(rootDirs, baseDirs, 
        outoforderStream);
    LOG.info(expectedResults.size());
    for (int i = 0; i <= expectedResults.size()-1; i++) {
      Assert.assertEquals( expectedResults.get(i) ,outOfOderDirs.get(i));
    }
  }
}

