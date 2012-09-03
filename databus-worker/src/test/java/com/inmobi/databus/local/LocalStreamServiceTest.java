/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inmobi.databus.local;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.ClusterTest;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.TestMiniClusterUtil;
import com.inmobi.databus.local.LocalStreamService.CollectorPathFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalStreamServiceTest extends TestMiniClusterUtil {
  private static Logger LOG = Logger.getLogger(LocalStreamServiceTest.class);
  private final static int number_files = 9;

  Set<String> expectedResults = new LinkedHashSet<String>();
  Set<String> expectedTrashPaths = new LinkedHashSet<String>();
  Map<String, String> expectedCheckPointPaths = new HashMap<String, String>();

  @BeforeSuite
  public void setup() throws Exception {
    super.setup(2, 6, 1);
    createExpectedOutput();
  }

  @AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }

  private void createExpectedOutput() {
    createExpectedResults();
    createExpectedTrash();
    createExpectedCheckPointPaths();
  }

  private void createExpectedCheckPointPaths() {
    expectedCheckPointPaths.put("stream1collector1", "file8");
    expectedCheckPointPaths.put("stream1collector2", "file8");
    expectedCheckPointPaths.put("stream2collector1", "file8");
    expectedCheckPointPaths.put("stream2collector2", "file8");
  }

  private void createExpectedResults() {
    expectedResults.add("/databus/data/stream1/collector2/file1");
    expectedResults.add("/databus/data/stream1/collector2/file2");
    expectedResults.add("/databus/data/stream1/collector2/file3");
    expectedResults.add("/databus/data/stream1/collector2/file4");
    expectedResults.add("/databus/data/stream1/collector2/file5");
    expectedResults.add("/databus/data/stream1/collector2/file6");
    expectedResults.add("/databus/data/stream1/collector2/file7");
    expectedResults.add("/databus/data/stream1/collector2/file8");
    expectedResults.add("/databus/data/stream2/collector1/file1");
    expectedResults.add("/databus/data/stream2/collector1/file2");
    expectedResults.add("/databus/data/stream2/collector1/file3");
    expectedResults.add("/databus/data/stream2/collector1/file4");
    expectedResults.add("/databus/data/stream2/collector1/file5");
    expectedResults.add("/databus/data/stream2/collector1/file6");
    expectedResults.add("/databus/data/stream2/collector1/file7");
    expectedResults.add("/databus/data/stream2/collector1/file8");
    expectedResults.add("/databus/data/stream2/collector2/file1");
    expectedResults.add("/databus/data/stream2/collector2/file2");
    expectedResults.add("/databus/data/stream2/collector2/file3");
    expectedResults.add("/databus/data/stream2/collector2/file4");
    expectedResults.add("/databus/data/stream2/collector2/file5");
    expectedResults.add("/databus/data/stream2/collector2/file6");
    expectedResults.add("/databus/data/stream2/collector2/file7");
    expectedResults.add("/databus/data/stream2/collector2/file8");
    expectedResults.add("/databus/data/stream1/collector1/file1");
    expectedResults.add("/databus/data/stream1/collector1/file2");
    expectedResults.add("/databus/data/stream1/collector1/file3");
    expectedResults.add("/databus/data/stream1/collector1/file4");
    expectedResults.add("/databus/data/stream1/collector1/file5");
    expectedResults.add("/databus/data/stream1/collector1/file6");
    expectedResults.add("/databus/data/stream1/collector1/file7");
    expectedResults.add("/databus/data/stream1/collector1/file8");
  }

  private void createExpectedTrash() {
    expectedTrashPaths.add("/databus/data/stream2/collector2/file2");
    expectedTrashPaths.add("/databus/data/stream2/collector2/file1");
    expectedTrashPaths.add("/databus/data/stream1/collector1/file1");
    expectedTrashPaths.add("/databus/data/stream2/collector1/file1");
    expectedTrashPaths.add("/databus/data/stream2/collector1/file2");
    expectedTrashPaths.add("/databus/data/stream1/collector1/file2");
    expectedTrashPaths.add("/databus/data/stream1/collector2/file1");
    expectedTrashPaths.add("/databus/data/stream1/collector2/file2");
  }

  private void validateExpectedOutput(Set<String> results,
      Set<String> trashPaths, Map<String, String> checkPointPaths) {
    assert results.equals(expectedResults);
    assert trashPaths.equals(expectedTrashPaths);
    assert checkPointPaths.equals(expectedCheckPointPaths);
  }

  private void createMockForFileSystem(FileSystem fs, Cluster cluster)
      throws Exception {
    FileStatus[] files = createTestData(2, "/databus/data/stream", true);

    FileStatus[] stream1 = createTestData(2, "/databus/data/stream1/collector",
        true);

    FileStatus[] stream3 = createTestData(number_files,
        "/databus/data/stream1/collector1/file", true);

    FileStatus[] stream4 = createTestData(number_files,
        "/databus/data/stream1/collector2/file", true);

    FileStatus[] stream2 = createTestData(2, "/databus/data/stream2/collector",
        true);

    FileStatus[] stream5 = createTestData(number_files,
        "/databus/data/stream2/collector1/file", true);

    FileStatus[] stream6 = createTestData(number_files,
        "/databus/data/stream2/collector2/file", true);

    when(fs.getWorkingDirectory()).thenReturn(new Path("/tmp/"));
    when(fs.getUri()).thenReturn(new URI("localhost"));
    when(fs.listStatus(cluster.getDataDir())).thenReturn(files);
    when(fs.listStatus(new Path("/databus/data/stream1"))).thenReturn(stream1);

		when(
		    fs.listStatus(new Path("/databus/data/stream1/collector1"),
		        any(CollectorPathFilter.class))).thenReturn(stream3);
    when(fs.listStatus(new Path("/databus/data/stream2"))).thenReturn(stream2);
		when(
		    fs.listStatus(new Path("/databus/data/stream1/collector2"),
		        any(CollectorPathFilter.class))).thenReturn(stream4);
		when(
		    fs.listStatus(new Path("/databus/data/stream2/collector1"),
		        any(CollectorPathFilter.class))).thenReturn(stream5);
		when(
		    fs.listStatus(new Path("/databus/data/stream2/collector2"),
		        any(CollectorPathFilter.class))).thenReturn(stream6);

    Path file = mock(Path.class);
    when(file.makeQualified(any(FileSystem.class))).thenReturn(
        new Path("/databus/data/stream1/collector1/"));
  }

	private void testCreateListing() {
    try {
      Cluster cluster = ClusterTest.buildLocalCluster();
      FileSystem fs = mock(FileSystem.class);
      createMockForFileSystem(fs, cluster);

      Map<FileStatus, String> results = new TreeMap<FileStatus, java.lang.String>();
      Set<FileStatus> trashSet = new HashSet<FileStatus>();
      Map<String, FileStatus> checkpointPaths = new HashMap<String, FileStatus>();
      fs.delete(cluster.getDataDir(), true);
      FileStatus dataDir = new FileStatus(20, false, 3, 23823, 2438232,
          cluster.getDataDir());
      fs.delete(new Path(cluster.getRootDir() + "/databus-checkpoint"), true);

      TestLocalStreamService service = new TestLocalStreamService(null,
          cluster, new FSCheckpointProvider(cluster.getRootDir()
              + "/databus-checkpoint"));
      service.createListing(fs, dataDir, results, trashSet, checkpointPaths);

      Set<String> tmpResults = new LinkedHashSet<String>();
      // print the results
      for (FileStatus status : results.keySet()) {
        tmpResults.add(status.getPath().toString());
        LOG.debug("Results [" + status.getPath().toString() + "]");
      }

      // print the trash
      Iterator<FileStatus> it = trashSet.iterator();
      Set<String> tmpTrashPaths = new LinkedHashSet<String>();
      while (it.hasNext()) {
        FileStatus trashfile = it.next();
        tmpTrashPaths.add(trashfile.getPath().toString());
        LOG.debug("trash file [" + trashfile.getPath());
      }

      Map<String, String> tmpCheckPointPaths = new TreeMap<String, String>();
      // print checkPointPaths
      for (String key : checkpointPaths.keySet()) {
        tmpCheckPointPaths.put(key, checkpointPaths.get(key).getPath()
            .getName());
        LOG.debug("CheckPoint key [" + key + "] value ["
            + checkpointPaths.get(key).getPath().getName() + "]");
      }
      validateExpectedOutput(tmpResults, tmpTrashPaths, tmpCheckPointPaths);
      fs.delete(new Path(cluster.getRootDir() + "/databus-checkpoint"), true);
      fs.delete(cluster.getDataDir(), true);
      fs.close();
    } catch (Exception e) {
      LOG.debug("Error in running testCreateListing", e);
      assert false;
    }
  }

  private FileStatus[] createTestData(int count, String path, boolean useSuffix) {
    FileStatus[] files = new FileStatus[count];
    for (int i = 1; i <= count; i++) {
      files[i - 1] = new FileStatus(20, false, 3, 23232, 232323, new Path(path
          + ((useSuffix == true) ? (new Integer(i).toString()) : (""))));
    }
    return files;
  }

  private FileStatus[] createTestData(int count, String path) {
    return createTestData(count, path, false);
  }

  private DatabusConfig buildTestDatabusConfig() throws Exception {
    JobConf conf = super.CreateJobConf();
    return buildTestDatabusConfig(conf.get("mapred.job.tracker"),
        "file:///tmp", "databus", "48", "24");
  }

  public static DatabusConfig buildTestDatabusConfig(String jturl,
      String hdfsurl, String rootdir, String retentioninhours,
      String trashretentioninhours) throws Exception {

    Map<String, Integer> sourcestreams = new HashMap<String, Integer>();

    sourcestreams.put("cluster1", new Integer(retentioninhours));

    Map<String, SourceStream> streamMap = new HashMap<String, SourceStream>();
    streamMap.put("stream1", new SourceStream("stream1", sourcestreams));

    sourcestreams.clear();

    Map<String, DestinationStream> deststreamMap = new HashMap<String, DestinationStream>();
    deststreamMap.put("stream1",
        new DestinationStream("stream1", Integer.parseInt(retentioninhours),
            Boolean.TRUE));

    sourcestreams.clear();

    /*
     * sourcestreams.put("cluster2", new Integer(2)); streamMap.put("stream2",
     * new SourceStream("stream2", sourcestreams));
     */

    Set<String> sourcestreamnames = new HashSet<String>();

    for (Map.Entry<String, SourceStream> stream : streamMap.entrySet()) {
      sourcestreamnames.add(stream.getValue().getName());
    }
    Map<String, Cluster> clusterMap = new HashMap<String, Cluster>();

    clusterMap.put("cluster1", ClusterTest.buildLocalCluster(rootdir,
        "cluster1", hdfsurl, jturl, sourcestreamnames, deststreamMap));

    Map<String, String> defaults = new HashMap<String, String>();

    defaults.put(DatabusConfigParser.ROOTDIR, rootdir);
    defaults.put(DatabusConfigParser.RETENTION_IN_HOURS, retentioninhours);
    defaults.put(DatabusConfigParser.TRASH_RETENTION_IN_HOURS,
        trashretentioninhours);

    /*
     * clusterMap.put( "cluster2", ClusterTest.buildLocalCluster("cluster2",
     * "file:///tmp", conf.get("mapred.job.tracker")));
     */

    return new DatabusConfig(streamMap, clusterMap, defaults);
  }
  
  @Test
  public void testPopulateTrashPaths() throws Exception {
    FileStatus[] status = new FileStatus[10];
    String[] expectedstatus = new String[10];
    
    status[0] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster1/test1-2012-08-29-07-09_00000"));
    status[1] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster1/test1-2012-08-29-07-04_00000"));
    status[2] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test2/testcluster1/test2-2012-08-29-07-09_00003"));
    status[3] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster2/test1-2012-08-13-07-09_00000"));
    status[4] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster1/test1-2012-08-29-07-09_00009"));
    status[5] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster1/test1-2012-08-29-07-12_00000"));
    status[6] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster1/test1-2012-08-29-07-10_00000"));
    status[7] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test2/testcluster1/test2-2012-08-29-07-45_00000"));
    status[8] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster2/test1-2012-08-29-07-09_00078"));
    status[9] = new FileStatus(20, false, 3, 23823, 2438232, new Path(
        "/databus/data/test1/testcluster2/test1-2012-08-29-07-04_00034"));
    
    expectedstatus[0] = "/databus/data/test1/testcluster1/test1-2012-08-29-07-04_00000";
    expectedstatus[1] = "/databus/data/test1/testcluster1/test1-2012-08-29-07-09_00000";
    expectedstatus[2] = "/databus/data/test1/testcluster1/test1-2012-08-29-07-09_00009";
    expectedstatus[3] = "/databus/data/test1/testcluster1/test1-2012-08-29-07-10_00000";
    expectedstatus[4] = "/databus/data/test1/testcluster1/test1-2012-08-29-07-12_00000";
    
    expectedstatus[5] = "/databus/data/test1/testcluster2/test1-2012-08-13-07-09_00000";
    expectedstatus[6] = "/databus/data/test1/testcluster2/test1-2012-08-29-07-04_00034";
    expectedstatus[7] = "/databus/data/test1/testcluster2/test1-2012-08-29-07-09_00078";
    
    
    expectedstatus[8] = "/databus/data/test2/testcluster1/test2-2012-08-29-07-09_00003";
    expectedstatus[9] = "/databus/data/test2/testcluster1/test2-2012-08-29-07-45_00000";

    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    for (int i = 0; i < 10; ++i) {
      trashSet.add(status[i]);
    }
    
    Cluster cluster = ClusterTest.buildLocalCluster();
    TestLocalStreamService service = new TestLocalStreamService(
        buildTestDatabusConfig(), cluster, new FSCheckpointProvider(
            cluster.getCheckpointDir()));
    
    Map<Path, Path> trashCommitPaths = service
        .populateTrashCommitPaths(trashSet);

    Set<Path> srcPaths = trashCommitPaths.keySet();
    
    Iterator<Path> it = srcPaths.iterator();
    int i = 0;
    
    while (it.hasNext()) {
      String actualPath = it.next().toString();
      String expectedPath = expectedstatus[i];
      
      LOG.debug("Comparing Trash Paths Actual [" + actualPath + "] Expected ["
          + expectedPath + "]");
      Assert.assertEquals(actualPath, expectedPath);
      
      i++;
    }
  }
  
  @Test
  public void testMapReduce() throws Exception {
    LOG.info("Running LocalStreamIntegration for filename test-lss-databus.xml");
    testMapReduce("test-lss-databus.xml", 1);
  }
  
  @Test(groups = { "integration" })
  public void testMultipleStreamMapReduce() throws Exception {
    LOG.info("Running LocalStreamIntegration for filename test-lss-multiple-databus.xml");
    testMapReduce("test-lss-multiple-databus.xml", 1);
    LOG.info("Running LocalStreamIntegration for filename test-lss-multiple-databus.xml, Running Twice");
    testMapReduce("test-lss-multiple-databus.xml", 2);
  }
  
  private void testMapReduce(String fileName, int timesToRun) throws Exception {
    DatabusConfigParser parser = new DatabusConfigParser(fileName);
    DatabusConfig config = parser.getConfig();
    
    Set<String> clustersToProcess = new HashSet<String>();
    Set<TestLocalStreamService> services = new HashSet<TestLocalStreamService>();
    
    for (SourceStream sStream : config.getSourceStreams().values()) {
      for (String cluster : sStream.getSourceClusters()) {
        clustersToProcess.add(cluster);
      }
    }
    
    for (String clusterName : clustersToProcess) {
      Cluster cluster = config.getClusters().get(clusterName);
      cluster.getHadoopConf().set("mapred.job.tracker",
          super.CreateJobConf().get("mapred.job.tracker"));
      services.add(new TestLocalStreamService(config, cluster,
          new FSCheckpointProvider(cluster.getCheckpointDir())));
    }
    
    for (TestLocalStreamService service : services) {
      for (int i = 0; i < timesToRun; ++i) {
        service.preExecute();
        service.execute();
        service.postExecute();
        Thread.sleep(60000);
      }
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    }
    
  }

}
