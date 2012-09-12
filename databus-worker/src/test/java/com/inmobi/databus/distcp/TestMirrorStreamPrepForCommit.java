package com.inmobi.databus.distcp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.local.TestCreateListing;
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

  // since tests can run in parallel use a seperate databusRoot for this test
  FileSystem localFs;
  Set<String> testPaths = new TreeSet<String>();
  Path rootDir = new Path("/tmp/");
  String databusRoot =  this.getClass().getName();
  Path testRoot = new Path(rootDir, databusRoot);
  Path tmpOut = new Path(testRoot,
      "system/tmp/distcp_mirror_databusCluster_databusCluster/");
  Path streamRoot = new Path(tmpOut, "tmp/streams/stream1/");
  MirrorStreamService service = null;
  List<Path> finalExpectedPaths = new ArrayList<Path>();

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
        ("/tmp/streams/stream1/2012/01/13/15/04/localhost-stream1-2012-01-15-04-24_00000.gz"));
    finalExpectedPaths.add(new Path
        ("/tmp/streams/stream1/2012/01/13/15/06"));
    finalExpectedPaths.add(new Path
        ("/tmp/streams/stream1/2012/01/13/15/07/localhost-stream1-2012-01-16" +
            "-07-21_00000.gz"));
    finalExpectedPaths.add(new
        Path("/tmp/streams/stream1/2012/01/13/15/07/localhost-stream1-2012-01-16-07-23_00000.gz"));
    finalExpectedPaths.add(new Path
        ("/tmp/streams/stream1/2012/01/13/15/07/localhost-stream1-2012-01-16-07-24_00000.gz"));

  }

  @AfterTest
  private void cleanup() throws Exception{
    localFs.delete(testRoot, true);
  }

  private void createData() throws IOException {
    Path p = new Path(tmpOut,
        "tmp/streams/stream1/2012/01/13/15/04/localhost-stream1-2012-01-15-04-24_00000.gz");
    testPaths.add(p.toString());
    localFs.create(p);

    //one path without any data to simulate publish missing path
    p = new Path(tmpOut,
        "tmp/streams/stream1/2012/01/13/15/06/");
    testPaths.add(p.toString());
    localFs.mkdirs(p);

    p = new Path(tmpOut,
        "tmp/streams/stream1/2012/01/13/15/07/" +
            "localhost-stream1-2012-01-16-07-21_00000.gz");
    testPaths.add(p.toString());
    localFs.create(p);

    p = new Path(tmpOut,
        "tmp/streams/stream1/2012/01/13/15/07/" +
            "localhost-stream1-2012-01-16-07-23_00000.gz");
    testPaths.add(p.toString());
    localFs.create(p);

    p = new Path(tmpOut,
        "tmp/streams/stream1/2012/01/13/15/07/" +
            "localhost-stream1-2012-01-16-07-24_00000.gz");
    testPaths.add(p.toString());
    localFs.create(p);


     }

  @Test
  public void testPrepareForCommit() {
    try {
      //call the api of service
      LinkedHashMap<FileStatus, Path> commitPaths = service.prepareForCommit
          (tmpOut);
      //print the commitPaths
      validateResults(commitPaths.values(), finalExpectedPaths);

    } catch (Exception e) {
      LOG.error("Error in test", e);
      assert false;
    }

  }

  private void validateResults(Collection<Path> keys,
                               Collection<Path> finalExpectedPaths) {
    // remove file://// from values before assertion
    // assert that results is in order as that of expectedFinalResults
    Iterator it = finalExpectedPaths.iterator();
    for (Path key : keys) {
      Path expectedPath = (Path)it.next();
      String p = key.toUri().getPath();
      LOG.debug("Comparing  path [" + p + "] to resultSet [" + expectedPath
          + "]");
      assert p.trim().equals(expectedPath.toString().trim());
    }
  }

  @Test
  public void testCreateListing() {
    ArrayList<FileStatus> streamPaths = new ArrayList<FileStatus>();

    try {
      service.createListing(localFs, localFs.getFileStatus(streamRoot), streamPaths);
    //  Collections.sort(streamPaths, new DatePathComparator());
      for (FileStatus fileStatus : streamPaths) {

       assert testPaths.contains(fileStatus.getPath().toUri().getPath()
           .toString());
      }
    } catch (Exception e) {
      LOG.error(e);
      assert false;
    }

  }


}
