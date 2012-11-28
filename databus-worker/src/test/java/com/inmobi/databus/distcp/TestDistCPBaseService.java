package com.inmobi.databus.distcp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.inmobi.databus.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestDistCPBaseService  {
  private static Logger LOG = Logger.getLogger(TestDistCPBaseService.class);
  Path testRoot = new Path("/tmp/", this.getClass().getName());
  FileSystem localFs;
  Cluster cluster;
  MirrorStreamService service = null;
  String expectedFileName1 = "/tmp/com.inmobi.databus.distcp" +
      ".TestDistCPBaseService/data-file1";
  String expectedFileName2 = "/tmp/com.inmobi.databus.distcp" +
      ".TestDistCPBaseService/data-file2";
  Set<String> expectedConsumePaths = new HashSet<String>();

  @BeforeTest
  public void setUP() throws Exception {
    //create fs
    localFs = FileSystem.getLocal(new Configuration());
    localFs.mkdirs(testRoot);

    //create cluster
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", localFs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "databusCluster");
    clusterConf.put("jobqueuename", "default");
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add("stream1");
    Cluster cluster = new Cluster(clusterConf, testRoot.toString(), null,
        sourceNames);

    //create service
    service = new MirrorStreamService(null, cluster,
        cluster);

    //create data
    createValidData();

    //expectedConsumePaths
    expectedConsumePaths.add("file:/tmp/com.inmobi.databus.distcp" +
        ".TestDistCPBaseService/system/mirrors/databusCluster/file-with-valid-data");
    expectedConsumePaths.add("file:/tmp/com.inmobi.databus.distcp" +
        ".TestDistCPBaseService/system/mirrors/databusCluster/file-with-junk-data");
    expectedConsumePaths.add("file:/tmp/com.inmobi.databus.distcp" +
        ".TestDistCPBaseService/system/mirrors/databusCluster/file1-empty");

  }

  //@AfterTest
  public void cleanUP() throws IOException{
    //cleanup testRoot
    localFs.delete(testRoot, true);
  }

  private void createInvalidData() throws IOException{
    localFs.mkdirs(testRoot);
    Path dataRoot = new Path(testRoot, service.getInputPath());
    localFs.mkdirs(dataRoot);
    //one empty file
    Path p = new Path(dataRoot, "file1-empty");
    localFs.create(p);

    //one file with data put invalid paths
    p = new Path(dataRoot, "file-with-junk-data");
    FSDataOutputStream out = localFs.create(p);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write("junkfile-1\n");
    writer.write("junkfile-2\n");
    writer.close();

  }

  private void createValidData() throws IOException{
    Path dataRoot = new Path(testRoot, service.getInputPath());
    localFs.mkdirs(dataRoot);
    //create invalid data
    createInvalidData();

    //one valid & invalid data file
    Path data_file = new Path(testRoot, "data-file1");
    localFs.create(data_file);

    Path data_file1 = new Path(testRoot, "data-file2");
    localFs.create(data_file1);

    //one file with data and one valid path and one invalid path
    Path p = new Path(dataRoot, "file-with-valid-data");
    FSDataOutputStream  out = localFs.create(p);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write(data_file.toString() +"\n");
    writer.write("some-junk-path\n");
    writer.write(data_file1.toString() + "\n");
    writer.close();
  }

  @Test(priority = 1)
  public void testPositive() throws Exception {
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    Path p =  service.getDistCPInputFile(consumePaths, testRoot);
    LOG.info("distcp input [" + p + "]");
    FSDataInputStream in = localFs.open(p);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String result = reader.readLine();
    //assert that the minuteFileName inside the valid file with data
    //matches our expectedFileName1
    assert result.equals(expectedFileName1);

    //second line was junkpath which would be skipped instead the next valid
    // path in input should be present
    result = reader.readLine();
    assert result.equals(expectedFileName2);
    reader.close();

    Set<String> resultSet = new HashSet<String>();
    //compare consumePaths with expectedOutput
    for (Path consumePath : consumePaths.keySet()) {
      //cant compare the path generated using timestamp
      //The final path on destinationCluster which contains all valid
      // minutefileNames has a suffix of timestamp to it
      if (!consumePath.toString().contains("file:/tmp/com.inmobi.databus" +
          ".distcp.TestDistCPBaseService/databusCluster")) {
        LOG.info("Path to consume [" + consumePath + "]");
        resultSet.add(consumePath.toString());
      }
    }
    assert resultSet.containsAll(expectedConsumePaths);
    assert consumePaths.size() == resultSet.size() + 1;
  }


  @Test(priority = 2)
  public void testNegative() throws Exception {
    cleanUP();
    createInvalidData();
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    Path p =  service.getDistCPInputFile(consumePaths, testRoot);
    //since all data is invalid
    //output of this function should be null
    assert p == null;

  }

  @Test
  public void testSplitFileName() throws Exception {
    Set<String> streamsSet = new HashSet<String>();
    streamsSet.add("test-stream");
    streamsSet.add("test_stream");
    streamsSet.add("test_streams");
    streamsSet.add("test_stream_2");
    // file name in which collector name has hyphen
    String fileName1 = "databus-test-test_stream-2012-11-27-21-20_00000.gz";
    // file name in which stream name has hyphen
    String fileName2 = "databus_test-test-stream-2012-11-27-21-20_00000.gz";
    // file name in which stream name is subset of another stream name in the
    // streamsSet
    String fileName3 = "databus_test-test_streams-2012-11-27-21-20_00000.gz";
    String fileName4 = "databus_test-test_stream_2-2012-11-27-21-20_00000.gz";
    //file name in which stream name is not in streamsSet passed
    String fileName5 = "databus_test-test_stream-2-2012-11-27-21-20_00000.gz";
    // get stream names from file name
    String expectedStreamName1 = MergedStreamService.getCategoryFromFileName(
        fileName1, streamsSet);
    String expectedStreamName2 = MergedStreamService.getCategoryFromFileName(
        fileName2, streamsSet);
    String expectedStreamName3 = MergedStreamService.getCategoryFromFileName(
        fileName3, streamsSet);
    String expectedStreamName4 = MergedStreamService.getCategoryFromFileName(
        fileName4, streamsSet);
    String expectedStreamName5 = MergedStreamService.getCategoryFromFileName(
        fileName5, streamsSet);
    assert expectedStreamName1.compareTo("test_stream") == 0;
    assert expectedStreamName2.compareTo("test-stream") == 0;
    assert expectedStreamName3.compareTo("test_streams") == 0;
    assert expectedStreamName4.compareTo("test_stream_2") == 0;
    assert expectedStreamName5 == null;
  }
}
