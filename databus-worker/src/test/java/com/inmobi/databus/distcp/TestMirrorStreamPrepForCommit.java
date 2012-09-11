package com.inmobi.databus.distcp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.local.TestCreateListing;
import com.inmobi.databus.utils.DatePathComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestMirrorStreamPrepForCommit {

  private static Logger LOG = Logger.getLogger(TestCreateListing.class);
  Random randomGenerator = new Random();
  String databusRoot =  new Long(Math.abs(randomGenerator.nextLong())).toString();

  // since tests can run in parallel use a seperate databusRoot for this test
  Set<Path> testPaths = new TreeSet<Path>();
  Path rootDir = new Path("/tmp/");
  FileSystem localFs;
  Path tmpOut = new Path(rootDir,
  databusRoot + "/system/tmp/distcp_mirror_databusCluster_databusCluster/");
  Path streamRoot = new Path(tmpOut, "tmp/streams/metric_billing/");
  MirrorStreamService service = null;
  TreeSet<Path> finalExpectedPaths = new TreeSet<Path>();

  @BeforeTest
  private void setUP() throws Exception{
    //create fs
    localFs = FileSystem.getLocal(new Configuration());
    localFs.mkdirs(tmpOut);

    //create test data
    createData();

    //create cluster
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", localFs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "databusCluster");
    clusterConf.put("jobqueuename", "default");
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add("stream1");
    Cluster cluster = new Cluster(clusterConf, rootDir.toString(), null, sourceNames);

    //create service
    service = new MirrorStreamService(null, cluster,
    cluster);

    //createFinalExpectedPath
    finalExpectedPaths.add(new Path
    ("/tmp/streams/metric_billing/2012/01/13/15/04/gs1104.grid.corp.inmobi.com-metric_billing-2012-01-15-04-24_00000.gz"));
    finalExpectedPaths.add(new Path
    ("/tmp/streams/metric_billing/2012/01/13/15/06"));
    finalExpectedPaths.add(new Path
    ("/tmp/streams/metric_billing/2012/01/13/15/07/gs1104.grid.corp.inmobi" +
    ".com-metric_billing-2012-01-16-07-21_00000.gz"));
    finalExpectedPaths.add(new
    Path("/tmp/streams/metric_billing/2012/01/13/15/07" +
    "/gs1104.grid.corp.inmobi.com-metric_billing-2012-01-16-07-23_00000.gz"));
    finalExpectedPaths.add(new Path
    ("/tmp/streams/metric_billing/2012/01/13/15/07/gs1104.grid.corp.inmobi.com-metric_billing-2012-01-16-07-24_00000.gz"));

  }

  @AfterTest
  private void cleanup() throws Exception{
    localFs.delete(tmpOut, true);
  }

  private void createData() throws IOException {
    Path p = new Path(tmpOut,
    "tmp/streams/metric_billing/2012/01/13/15/07/gs1104.grid.corp.inmobi" +
    ".com-metric_billing-2012-01-16-07-21_00000.gz");
    testPaths.add(p);
    localFs.create(p);

    p = new Path(tmpOut,
    "tmp/streams/metric_billing/2012/01/13/15/07/" +
    "gs1104.grid.corp.inmobi.com-metric_billing-2012-01-16-07-23_00000.gz");
    testPaths.add(p);
    localFs.create(p);

    p = new Path(tmpOut,
    "tmp/streams/metric_billing/2012/01/13/15/07/" +
    "gs1104.grid.corp.inmobi.com-metric_billing-2012-01-16-07-24_00000.gz");
    testPaths.add(p);
    localFs.create(p);

    //one path without any data to simulate publish missing path
    p = new Path(tmpOut,
    "tmp/streams/metric_billing/2012/01/13/15/06/");
    testPaths.add(p);
    localFs.mkdirs(p);

    p = new Path(tmpOut,
    "tmp/streams/metric_billing/2012/01/13/15/04/gs1104.grid.corp.inmobi" +
    ".com-metric_billing-2012-01-15-04-24_00000.gz");
    testPaths.add(p);
    localFs.create(p);
  }

  @Test
  public void testPrepareForCommit() {
    try {

      //call the api of service
      LinkedHashMap commitPaths = service.prepareForCommit
      (tmpOut);
      //print the commitPaths
      Set<FileStatus> keys = commitPaths.keySet();
      Set<Path> results = new TreeSet<Path>();
      //remove file://// from values before assertion
      for (FileStatus key : keys) {
        String p = commitPaths.get(key).toString().replaceAll("file:", "");
        LOG.debug("Adding path [" +p + "] to resultSet");
        results.add(new Path(p));
      }
      assert results.containsAll(finalExpectedPaths);
    } catch (Exception e) {
      LOG.error("Error in test", e);
      assert false;
    }

  }

  @Test
  public void testCreateListing() {
    ArrayList<FileStatus> streamPaths = new ArrayList<FileStatus>();
    try {
      service.createListing(localFs, localFs.getFileStatus(streamRoot), streamPaths);
      Collections.sort(streamPaths, new DatePathComparator());
      TreeSet<Path> testResults = new TreeSet<Path>();

      for(FileStatus fileStatus : streamPaths) {
        //since ls on local filesystem adds a file: to paths we need to remove
        //that to do the validation
        Path p = new Path(fileStatus.getPath().toString().replaceAll
        ("file:", ""));
        testResults.add(p);
        LOG.debug("TestResult [" + p + "]");
      }
      assert testResults.size() == 5;
      assert testPaths.containsAll(testResults);

    } catch (Exception e) {
      LOG.error(e);
      assert false;
    }

  }


}
