package com.inmobi.databus.local;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.FSCheckpointProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestCreateListing {
  private static Logger LOG = Logger.getLogger(TestCreateListing.class);
  Path rootDir = new Path
  ("/tmp/test-databus/databus/");

  FileSystem localFs;

  @BeforeTest
  private void setUP() throws Exception{

   localFs = FileSystem.getLocal(new Configuration());
  }

  @AfterTest
  private void cleanup() throws Exception{
    localFs.delete(rootDir, true);

  }

  @Test
  public void testCreateListing1() throws Exception{


    Path collectorPath = new Path(rootDir, "data/stream1/collector1");
    localFs.mkdirs(collectorPath);
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", localFs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "databusCluster");
    clusterConf.put("jobqueuename", "default");
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add("stream1");
    Cluster cluster = new Cluster(clusterConf, rootDir.toString(), null, sourceNames);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Map<String, FileStatus> checkpointPaths = new HashMap<String,
    FileStatus>();

    LocalStreamService service = new LocalStreamService(null, cluster,
    new FSCheckpointProvider("/tmp/test-databus/databus/checkpoint"));
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);

    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;

    //create only current file and scribe_stats
    localFs.create(new Path(collectorPath, "stream1_current"));
    localFs.create(new Path(collectorPath, "scribe_stats"));

    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);

    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;

    //create one data file
    localFs.create(new Path(collectorPath, "datafile1"));
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);

    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;

    //sleep for 10 msec
    Thread.sleep(10);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths, 10);

    //0 bytes file found should be deleted and not current file
    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;


    FSDataOutputStream out = localFs.create(new Path(collectorPath,
    "datafile2"));
    out.writeBytes("this is a testcase");
    out.close();

    //sleep for 10 msec
    Thread.sleep(10);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths, 10);

    // this is not the current file and hence should be processed
    assert results.size() == 1;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;

     out = localFs.create(new Path(collectorPath,
    "datafile3"));
    out.writeBytes("this is a testcase");
    out.close();

    //don't sleep now
    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
       LOG.info("File Name [" + file.getPath() + "]");
    }
    assert results.size() == 1;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;


    out = localFs.create(new Path(collectorPath,
    "datafile4"));
    out.writeBytes("this is a testcase");
    out.close();
    out = localFs.create(new Path(collectorPath,
    "datafile5"));
    out.writeBytes("this is a testcase");
    out.close();
    out = localFs.create(new Path(collectorPath,
    "datafile6"));
    out.writeBytes("this is a testcase");
    out.close();
    out = localFs.create(new Path(collectorPath,
    "datafile7"));
    out.writeBytes("this is a testcase");
    out.close();

    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
      LOG.info("File Name [" + file.getPath() + "]");
    }

    assert results.size() == 5;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;

    out = localFs.create(new Path(collectorPath,
    "datafile8"));
    out.writeBytes("this is a testcase");
    out.close();

    out = localFs.create(new Path(collectorPath,
    "datafile9"));
    out.writeBytes("this is a testcase");
    out.close();

    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
      LOG.info("File Name [" + file.getPath() + "]");
    }
    assert results.size() == 7;
    assert trashSet.size() == 1;
    assert checkpointPaths.size() == 1;

  }

  public void clearLists(Map<FileStatus, String> results, 
    Set<FileStatus> trashSet, Map<String, FileStatus> checkpointPaths){
	results.clear();
    trashSet.clear();
    checkpointPaths.clear();
  }
}
