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

import java.text.NumberFormat;

import java.util.TreeSet;

import java.io.IOException;

import java.io.BufferedInputStream;
import java.io.File;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.ClusterTest;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.TestMiniClusterUtil;
import com.inmobi.databus.local.LocalStreamService.CollectorPathFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
  public void testPublishMissingPaths() throws Exception {
    DatabusConfigParser configParser = new DatabusConfigParser(
        "test-lss-pub-databus.xml");

    DatabusConfig config = configParser.getConfig();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    ArrayList<Cluster> clusterList = new ArrayList<Cluster>(config
        .getClusters().values());
    Cluster cluster = clusterList.get(0);
    TestLocalStreamService service = new TestLocalStreamService(config,
        cluster, new FSCheckpointProvider(cluster.getCheckpointDir()));

    ArrayList<SourceStream> sstreamList = new ArrayList<SourceStream>(config
        .getSourceStreams().values());

    SourceStream sstream = sstreamList.get(0);

    Calendar behinddate = new GregorianCalendar();
    Calendar todaysdate = new GregorianCalendar();
    behinddate.add(Calendar.HOUR_OF_DAY, -2);
    behinddate.set(Calendar.SECOND, 0);

    String basepublishPaths = cluster.getLocalFinalDestDirRoot()
        + sstream.getName() + File.separator;
    String publishPaths = basepublishPaths
        + getDateAsYYYYMMDDHHMMPath(behinddate.getTime());

    fs.mkdirs(new Path(publishPaths));

    service.publishMissingPaths(fs);

    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths);

    todaysdate.add(Calendar.HOUR_OF_DAY, 2);

    service.publishMissingPaths(fs);

    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths);

    fs.delete(new Path(cluster.getRootDir()), true);

    fs.close();
  }

  private String getDateAsYYYYMMDD(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    return dateFormat.format(date);
  }

  public static String getDateAsYYYYMMDDHHPath(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy" + File.separator + "MM"
        + File.separator + "dd" + File.separator + "HH" + File.separator);
    return dateFormat.format(date);
  }

  public static String getDateAsYYYYMMDDHHMMSS(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return dateFormat.format(date);
  }

  public static String getDateAsYYYYMMDDHHMMPath(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy" + File.separator + "MM"
        + File.separator + "dd" + File.separator + "HH" + File.separator + "mm");
    return dateFormat.format(date);
  }
  
  public static void testPublishMissingPaths(TestService service)
      throws Exception {
    
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Calendar behinddate = new GregorianCalendar();
    Calendar todaysdate = new GregorianCalendar();
    String basepublishPaths = service.getCluster().getFinalDestDirRoot()
        + "streams_publish" + File.separator;
    String publishPaths = basepublishPaths
        + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
    
    fs.mkdirs(new Path(publishPaths));
    
    service.publishMissingPaths(fs);
    
    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths);
    
    todaysdate.add(Calendar.HOUR_OF_DAY, 2);
    
    service.publishMissingPaths(fs);
    
    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths);
    
    fs.delete(new Path(basepublishPaths), true);
  }
  
  public static void VerifyMissingPublishPaths(FileSystem fs, long todaysdate,
      Calendar behinddate, String basepublishPaths) throws Exception {
    long diff = todaysdate - behinddate.getTimeInMillis();
    while (diff > 60000) {
      String checkcommitpath = basepublishPaths
          + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
      LOG.debug("Checking for Created Missing Path: " + checkcommitpath);
      fs.exists(new Path(checkcommitpath));
      behinddate.add(Calendar.MINUTE, 1);
      diff = todaysdate - behinddate.getTimeInMillis();
    }
  }

  @Test
  public void testMapReduce() throws Exception {
    LOG.info("Running LocalStreamIntegration for filename test-lss-databus.xml");
    testMapReduce("test-lss-databus.xml", true);
  }
  
  @Test(groups = { "integration" })
  public void testMultipleStreamMapReduce() throws Exception {
    LOG.info("Running LocalStreamIntegration for filename test-lss-multiple-databus.xml");
    testMapReduce("test-lss-multiple-databus.xml", true);
    LOG.info("Running LocalStreamIntegration for filename test-lss-multiple-databus.xml, Running Twice");
    testMapReduce("test-lss-multiple-databus.xml", false);
    testMapReduce("test-lss-multiple-databus.xml", true);
  }
  
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }

  public static List<String> createLocalStreamData(FileSystem fs,
      String streamName,
      String pathName, int filesCount) throws Exception {
    
    Path createPath = new Path(pathName);
    fs.mkdirs(createPath);
    List<String> filesList = new ArrayList<String>();
    
    for (int j = 0; j < filesCount; ++j) {
      Thread.sleep(1000);
      String filenameStr = new String(streamName + "-"
          + getDateAsYYYYMMDDHHMMSS(new Date()) + "_" + idFormat.format(j));
      filesList.add(j, filenameStr);
      Path path = new Path(createPath, filesList.get(j));
      
      LOG.debug("Creating Test Data with filename [" + filesList.get(j) + "]");
      FSDataOutputStream streamout = fs.create(path);
      streamout.writeBytes("Creating Test data for teststream "
          + filesList.get(j));
      
      streamout.close();

      Assert.assertTrue(fs.exists(path));
    }
    
    return filesList;
  }
  
  private void testMapReduce(String filename, boolean runOnce) throws Exception {

    final int NUM_OF_FILES = 25;

    DatabusConfigParser configParser = new DatabusConfigParser(filename);
    DatabusConfig config = configParser.getConfig();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    List<TestLocalStreamService> services = new ArrayList<TestLocalStreamService>();

    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      cluster
          .getValue()
          .getHadoopConf()
          .set("mapred.job.tracker",
              super.CreateJobConf().get("mapred.job.tracker"));

      services.add(new TestLocalStreamService(config, cluster.getValue(),
          new FSCheckpointProvider(cluster.getValue().getCheckpointDir())));
    }
    
    Iterator<TestLocalStreamService> serviceItr = services.iterator();

    for (Cluster cluster : config.getClusters().values()) {
      TestLocalStreamService service = serviceItr.next();
      testPublishMissingPaths(service);
      String testRootDir = cluster.getRootDir();
      if (!runOnce)
        fs.delete(new Path(testRootDir), true);

      Calendar behinddate = new GregorianCalendar();
      behinddate.add(Calendar.HOUR_OF_DAY, -2);
      Map<String, List<String>> files = new HashMap<String, List<String>>();
      Map<String, Set<String>> prevfiles = new HashMap<String, Set<String>>();

      for (Map.Entry<String, SourceStream> sstream : config.getSourceStreams()
          .entrySet()) {
        
        Set<String> prevfilesList = new TreeSet<String>();
        String pathName = cluster.getDataDir() + File.separator
            + sstream.getValue().getName() + File.separator + cluster.getName()
            + File.separator;
        
        FileStatus[] fStats = fs.listStatus(new Path(pathName));
        
        LOG.debug("Adding Previous Run Files in Path: " + pathName);
        for (FileStatus fStat : fStats) {
          LOG.debug("Previous File: " + fStat.getPath().getName());
          prevfilesList.add(fStat.getPath().getName());
        }

        List<String> filesList = createLocalStreamData(fs, sstream.getValue()
            .getName(), pathName, NUM_OF_FILES);
        
        files.put(sstream.getValue().getName(), filesList);
        prevfiles.put(sstream.getValue().getName(), prevfilesList);

        String dummycommitpath = cluster.getLocalFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + getDateAsYYYYMMDDHHMMPath(behinddate.getTime());
        fs.mkdirs(new Path(dummycommitpath));
      }

      {
        List<String> tmpFilesList = null;
        
        LOG.debug("Creating Tmp Files for LocalStream");
        String pathName = cluster.getTmpPath() + File.separator
              + service.getName() + File.separator;
        tmpFilesList = createLocalStreamData(fs, service.getName(), pathName,
              NUM_OF_FILES);
        
        service.runOnce();
        
        LOG.debug("Verifying Tmp Files for LocalStream");
        for (int j = 0; j < tmpFilesList.size(); ++j) {
          Path tmppath = new Path(pathName + File.separator
              + tmpFilesList.get(j));
          Assert.assertFalse(fs.exists(tmppath));
        }

      }

      LOG.info("Tmp Path does not exist for cluster " + cluster.getName());

      for (Map.Entry<String, SourceStream> sstream : config.getSourceStreams()
          .entrySet()) {
        
        List<String> filesList = files.get(sstream.getValue().getName());
        Set<String> prevfilesList = prevfiles.get(sstream.getValue().getName());

          Date todaysdate = new Date();
          Path trashpath = cluster.getTrashPathWithDateHour();
          String commitpath = cluster.getLocalFinalDestDirRoot()
              + sstream.getValue().getName() + File.separator
              + getDateAsYYYYMMDDHHPath(todaysdate);
          FileStatus[] mindirs = fs.listStatus(new Path(commitpath));

          FileStatus mindir = mindirs[0];

          for (FileStatus minutedir : mindirs) {
            if (mindir.getPath().getName()
                .compareTo(minutedir.getPath().getName()) < 0) {
              mindir = minutedir;
            }
          }
          // Make sure all the paths from dummy to mindir are created
        VerifyMissingPublishPaths(fs, todaysdate.getTime(), behinddate,
            cluster.getLocalFinalDestDirRoot() + sstream.getValue().getName());

          try {
            Integer.parseInt(mindir.getPath().getName());
            String streams_local_dir = commitpath + mindir.getPath().getName()
              + File.separator + cluster.getName();

            LOG.debug("Checking in Path for mapred Output: "
                + streams_local_dir);
          
          // First check for the previous current file
            if(!prevfilesList.isEmpty()) {
              String prevcurrentFile = (String) prevfilesList.toArray()[prevfilesList
                                                                        .size() - 1];
            LOG.debug("Checking Previous Run Current file in mapred Output ["
                + prevcurrentFile + "]");
              Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                  + prevcurrentFile + ".gz")));
            }
          
          
            for (int j = 0; j < NUM_OF_FILES - 1; ++j) {
            LOG.debug("Checking file in mapred Output [" + filesList.get(j)
                + "]");
              Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                + filesList.get(j) + ".gz")));
            }

          CheckpointProvider provider = service.getCheckpointProvider();

          String checkpoint = new String(provider.read(sstream.getValue()
              .getName() + cluster.getName()));

          LOG.debug("Checkpoint for " + sstream.getValue().getName()
              + cluster.getName() + " is " + checkpoint);

            LOG.debug("Comparing Checkpoint " + checkpoint + " and "
              + filesList.get(NUM_OF_FILES - 2));
          Assert.assertTrue(checkpoint.compareTo(filesList
              .get(NUM_OF_FILES - 2)) == 0);
          
          LOG.debug("Verifying Collector Paths");
          
          Path collectorPath = new Path(cluster.getDataDir(), sstream
              .getValue().getName() + File.separator + cluster.getName());
          
          for (int j = NUM_OF_FILES - 7; j < NUM_OF_FILES; ++j) {
            LOG.debug("Verifying Collector Path " + collectorPath
                + " Previous File " + filesList.get(j));
            Assert.assertTrue(fs.exists(new Path(collectorPath, filesList
                .get(j))));
          }

          LOG.debug("Verifying Trash Paths");

          for (String trashfile : prevfilesList) {
            String trashfilename = cluster.getName() + "-" + trashfile;
            LOG.debug("Verifying Trash Path " + trashpath + " Previous File "
                + trashfilename);
            Assert.assertTrue(fs.exists(new Path(trashpath, trashfilename)));
          }

          // Here 6 is the number of files - trash paths which are excluded
          for (int j = 0; j < NUM_OF_FILES - 7; ++j) {
            if (filesList.get(j).compareTo(checkpoint) <= 0) {
              String trashfilename = cluster.getName() + "-" + filesList.get(j);
              LOG.debug("Verifying Trash Path " + trashpath + "File "
                  + trashfilename);
              Assert.assertTrue(fs.exists(new Path(trashpath, trashfilename)));
            } else
              break;
          }

          } catch (NumberFormatException e) {

          }
      }
      fs.delete(cluster.getTrashPathWithDateHour(), true);
      if (runOnce)
        fs.delete(new Path(testRootDir), true);
    }

    fs.close();
  }
  
  public static interface TestService {
    Cluster getCluster();
    
    void publishMissingPaths(FileSystem fs) throws Exception;
  }

  public static class TestLocalStreamService extends LocalStreamService
      implements TestService {
		private Cluster srcCluster = null;
    private CheckpointProvider provider = null;

    public TestLocalStreamService(DatabusConfig config, Cluster cluster,
        CheckpointProvider provider) {
      super(config, cluster, provider);
			this.srcCluster = cluster;
      this.provider = provider;
    }

    public void runOnce() throws Exception {
      super.execute();
    }

    public void publishMissingPaths(FileSystem fs) throws Exception {
      super.publishMissingPaths(fs, srcCluster.getLocalFinalDestDirRoot());
    }

    @Override
    public Cluster getCluster() {
      return srcCluster;
    }
    
    public CheckpointProvider getCheckpointProvider() {
      return provider;
    }
  }
  

}
