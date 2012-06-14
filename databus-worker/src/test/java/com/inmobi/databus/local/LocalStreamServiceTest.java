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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.ClusterTest;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.Stream;
import com.inmobi.databus.Stream.SourceStreamCluster;
import com.inmobi.databus.Stream.StreamCluster;
import com.inmobi.databus.TestMiniClusterUtil;

@Test
public class LocalStreamServiceTest extends TestMiniClusterUtil {
  private static Logger LOG = Logger.getLogger(LocalStreamServiceTest.class);
  private final int number_files = 9;

  Set<String> expectedResults = new LinkedHashSet<String>();
  Set<String> expectedTrashPaths = new LinkedHashSet<String>();
  Map<String, String> expectedCheckPointPaths = new HashMap<String, String>();

  @BeforeSuite
  public void setup() throws Exception {
    super.setup(2, 2, 1);
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
    when(fs.listStatus(new Path("/databus/data/stream1/collector1")))
        .thenReturn(stream3);
    when(fs.listStatus(new Path("/databus/data/stream2"))).thenReturn(stream2);
    when(fs.listStatus(new Path("/databus/data/stream1/collector2")))
        .thenReturn(stream4);

    when(fs.listStatus(new Path("/databus/data/stream2/collector1")))
        .thenReturn(stream5);
    when(fs.listStatus(new Path("/databus/data/stream2/collector2")))
        .thenReturn(stream6);

    Path file = mock(Path.class);
    when(file.makeQualified(any(FileSystem.class))).thenReturn(
        new Path("/databus/data/stream1/collector1/"));
  }

  public void testCreateListing() {
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

  public static DatabusConfig buildTestDatabusConfig(String jturl,
      String hdfsurl, String rootdir, String retentioninhours,
      String trashretentioninhours) throws Exception {

    /*
     * sourcestreams.put("cluster2", new Integer(2)); streamMap.put("stream2",
     * new SourceStream("stream2", sourcestreams));
     */

    Map<String, Stream> Streams = new HashMap<String, Stream>();
    Map<String, Cluster> Clusters = new HashMap<String, Cluster>();
    Map<String, String> defaults = new HashMap<String, String>();

    defaults.put(DatabusConfigParser.ROOTDIR, rootdir);
    defaults.put(DatabusConfigParser.RETENTION_IN_HOURS, retentioninhours);
    defaults.put(DatabusConfigParser.TRASH_RETENTION_IN_HOURS,
        trashretentioninhours);

    Map<String, String> clusterconfiguration = new HashMap<String, String>();

    clusterconfiguration.put(DatabusConfigParser.NAME, "defaultCluster");
    clusterconfiguration.put(DatabusConfigParser.ROOTDIR, rootdir);
    clusterconfiguration.put(DatabusConfigParser.JT_URL, jturl);
    clusterconfiguration.put(DatabusConfigParser.HDFS_URL, hdfsurl);
    clusterconfiguration.put(DatabusConfigParser.JOB_QUEUE_NAME, "default");

    Cluster cluster = new Cluster(clusterconfiguration);
    Clusters.put("cluster1", cluster);

    Stream stream = new Stream("testStream");
    stream.addSourceCluster(24, cluster);
    Streams.put("testStream", stream);

    /*
     * clusterMap.put( "cluster2", ClusterTest.buildLocalCluster("cluster2",
     * "file:///tmp", conf.get("mapred.job.tracker")));
     */

    return new DatabusConfig(Streams, Clusters, defaults);
  }

  @Test
  public void testPublishMissingPaths() throws Exception {
    DatabusConfigParser configParser = new DatabusConfigParser(
        "test-lss-pub-databus.xml");

    DatabusConfig config = configParser.getConfig();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    ArrayList<Cluster> clusterList = new ArrayList<Cluster>(config
        .getAllClusters().values());
    Cluster cluster = clusterList.get(0);
    TestLocalStreamService service = new TestLocalStreamService(config,
        cluster, new FSCheckpointProvider(cluster.getCheckpointDir()));


    ArrayList<Stream> sstreams = new ArrayList<Stream>(config.getAllStreams()
        .values());

    Stream sstream = sstreams.get(0);
    SourceStreamCluster sourcestreamCluster = null;
    for (Iterator<StreamCluster> sstreamCluster = sstream
        .getSourceStreamClusters().iterator(); sstreamCluster.hasNext();) {
      StreamCluster scluster = sstreamCluster.next();
      if (scluster.getCluster().getName()
          .compareTo(cluster.getName()) == 0)
        sourcestreamCluster = (SourceStreamCluster) scluster;
    }

    Calendar behinddate = new GregorianCalendar();
    Calendar todaysdate = new GregorianCalendar();
    behinddate.add(Calendar.HOUR_OF_DAY, -2);
    behinddate.set(Calendar.SECOND, 0);

    String basepublishPaths = cluster.getLocalFinalDestDirRoot()
        + sstream.getName() + File.separator;
    String publishPaths = basepublishPaths
        + getDateAsYYYYMMDDHHMMPath(behinddate.getTime());

    fs.mkdirs(new Path(publishPaths));

    int retentioninhours = sourcestreamCluster.getRetentionPeriod();

    service.publishMissingPaths(fs, todaysdate.getTimeInMillis(),
        sstream.getName());

    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths, retentioninhours);

    todaysdate.add(Calendar.HOUR_OF_DAY, 2);

    service.publishMissingPaths(fs, todaysdate.getTimeInMillis(),
        sstream.getName());

    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths, retentioninhours);

    fs.delete(new Path(cluster.getRootDir()), true);

    fs.close();
  }

  private void VerifyMissingPublishPaths(FileSystem fs, long todaysdate,
      Calendar behinddate, String basepublishPaths, int retentioninhours)
      throws Exception {
    long diff = todaysdate - behinddate.getTimeInMillis();
    while (diff > 60000) {
      String checkcommitpath = basepublishPaths
          + getDateAsYYYYMMDDHHMMPath(behinddate.getTime());
      LOG.debug("Checking for Created Missing Path: " + checkcommitpath);
      if (diff < (retentioninhours * 60 * 60 * 1000))
        fs.exists(new Path(checkcommitpath));
      else
        LOG.debug("Skipping because of outside retentionperiod");
      behinddate.add(Calendar.MINUTE, 1);
      diff = todaysdate - behinddate.getTimeInMillis();
    }
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
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss_SSSSS");
    return dateFormat.format(date);
  }

  public static String getDateAsYYYYMMDDHHMMPath(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy" + File.separator + "MM"
        + File.separator + "dd" + File.separator + "HH" + File.separator + "mm");
    return dateFormat.format(date);
  }

  @Test(groups = { "integration" })
  public void testMapReduce() throws Exception {

    final int NUM_OF_FILES = 35;

    DatabusConfigParser configParser = new DatabusConfigParser(
        "test-lss-databus.xml");
    DatabusConfig config = configParser.getConfig();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    List<TestLocalStreamService> services = new ArrayList<TestLocalStreamService>();

    for (Map.Entry<String, Cluster> cluster : config.getAllClusters()
        .entrySet()) {
      services.add(new TestLocalStreamService(config, cluster.getValue(),
          new FSCheckpointProvider(cluster.getValue().getCheckpointDir())));
    }

    for (Map.Entry<String, Stream> sstream : config.getAllStreams()
        .entrySet()) {

      for (Cluster cluster : config.getAllClusters().values()) {

        String[] files = new String[NUM_OF_FILES];
        String testRootDir = cluster.getRootDir();

        fs.delete(new Path(testRootDir), true);
        Path createPath = new Path(cluster.getDataDir(), sstream.getValue()
            .getName() + File.separator + cluster.getName() + File.separator);
        fs.mkdirs(createPath);
        for (int j = 0; j < NUM_OF_FILES; ++j) {
          files[j] = new String(sstream.getValue().getName() + "-"
              + getDateAsYYYYMMDDHHMMSS(new Date()));
          Path path = new Path(createPath, files[j]);

          FSDataOutputStream streamout = fs.create(path);
          streamout.writeBytes("Creating Test data for teststream " + files[j]);
          streamout.close();

          Assert.assertTrue(fs.exists(path));
          /*
           * fs.mkdirs(new Path("/tmp/databus/data/stream2/cluster2/"));
           * fs.create(new Path("/tmp/databus/data/stream2/cluster2/" +
           * files[i])) .close();
           */
        }

        SourceStreamCluster sourcestreamCluster = null;
        for (Iterator<StreamCluster> sstreamCluster = sstream.getValue()
            .getSourceStreamClusters().iterator(); sstreamCluster.hasNext();) {
          StreamCluster scluster = sstreamCluster.next();
          if (scluster.getCluster().getName()
              .compareTo(cluster.getName()) == 0)
            sourcestreamCluster = (SourceStreamCluster) scluster;
        }

        int retentioninhours = sourcestreamCluster.getRetentionPeriod();

        Calendar behinddate = new GregorianCalendar();
        behinddate.add(Calendar.HOUR_OF_DAY, -2);
        String dummycommitpath = cluster.getLocalFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + getDateAsYYYYMMDDHHMMPath(behinddate.getTime());
        fs.mkdirs(new Path(dummycommitpath));

        for (TestLocalStreamService service : services) {
          for (int j = 0; j < NUM_OF_FILES; ++j) {
            Path tmppath = new Path(cluster.getTmpPath(), service.getName()
                + File.separator + getDateAsYYYYMMDDHHMMSS(new Date()));
            FSDataOutputStream tmpstreamout = fs.create(tmppath);
            tmpstreamout.writeBytes("Creating Tmp Test data for teststream "
                + files[j]);
            tmpstreamout.close();

            Assert.assertTrue(fs.exists(tmppath));

          }
          service.runOnce();

          for (int j = 0; j < NUM_OF_FILES; ++j) {
            Path tmppath = new Path(cluster.getTmpPath(), service.getName()
                + File.separator + getDateAsYYYYMMDDHHMMSS(new Date()));
            Assert.assertFalse(fs.exists(tmppath));
          }
          LOG.info("Tmp Path does not exist for cluster " + cluster.getName());
        }



        for (int dates = 0; dates < services.size(); ++dates) {

          Date todaysdate = new Date();
          Path trashpath = cluster.getTrashPathWithDateHour();
          String commitpath = cluster.getLocalFinalDestDirRoot()
              + sstream.getValue().getName() + File.separator
              + getDateAsYYYYMMDDHHPath(todaysdate);
          String checkpointpath = cluster.getCheckpointDir();
          FileStatus[] mindirs = fs.listStatus(new Path(commitpath));

          FileStatus mindir = mindirs[0];

          for (FileStatus minutedir : mindirs) {
            if (mindir.getPath().getName()
                .compareTo(minutedir.getPath().getName()) < 0) {
              mindir = minutedir;
            }
          }
          // Make sure all the paths from dummy to mindir are created
          long diff = todaysdate.getTime() - behinddate.getTimeInMillis();
          while (diff > 60000) {
            String checkcommitpath = cluster.getLocalFinalDestDirRoot()
                + sstream.getValue().getName() + File.separator
                + getDateAsYYYYMMDDHHMMPath(behinddate.getTime());
            LOG.debug("Checking for Created Missing Path: " + checkcommitpath);
            if (diff < (retentioninhours * 60 * 60 * 1000))
              fs.exists(new Path(checkcommitpath));
            else
              LOG.debug("Skipping because of outside retentionperiod");
            behinddate.add(Calendar.MINUTE, 1);
            diff = todaysdate.getTime() - behinddate.getTimeInMillis();
            ;
          }

          try {
            Integer.parseInt(mindir.getPath().getName());
            String streams_local_dir = commitpath + mindir.getPath().getName()
                + File.separator + cluster.getName();

            LOG.debug("Checking in Path for mapred Output: "
                + streams_local_dir);

            for (int j = 0; j < NUM_OF_FILES; ++j) {
              Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                  + files[j] + ".gz")));
            }

            Path checkpointfile = new Path(checkpointpath + File.separator
                + sstream.getValue().getName() + cluster.getName() + ".ck");

            LOG.debug("Checking for Checkpoint File: " + checkpointfile);

            Assert.assertTrue(fs.exists(checkpointfile));

            BufferedInputStream in = new BufferedInputStream(
                fs.open(checkpointfile));
            byte[] buffer = new byte[in.available()];
            in.read(buffer);
            String checkpoint = new String(buffer);
            in.close();

            LOG.debug("Checkpoint for " + checkpointfile + " is " + checkpoint);

            LOG.debug("Comparing Checkpoint " + checkpoint + " and "
                + files[NUM_OF_FILES - 1]);
            Assert
                .assertTrue(checkpoint.compareTo(files[NUM_OF_FILES - 1]) == 0);

            LOG.debug("Verifying Trash Paths");

            // Here 6 is the number of files - trash paths which are excluded
            for (int j = 0; j < NUM_OF_FILES - 6; ++j) {
              if (files[j].compareTo(checkpoint) <= 0) {
                String trashfilename = cluster.getName() + "-" + files[j];
                LOG.debug("Verifying Trash Path " + trashpath + "File "
                    + trashfilename);
                Assert
                    .assertTrue(fs.exists(new Path(trashpath, trashfilename)));
              } else
                break;
            }

            break;
          } catch (NumberFormatException e) {

          }
        }
        fs.delete(new Path(testRootDir), true);
      }
    }

    fs.close();
  }

  private class TestLocalStreamService extends LocalStreamService {

    public TestLocalStreamService(DatabusConfig config, Cluster cluster,
        CheckpointProvider provider) {
      super(config, cluster, provider);
    }

    public void runOnce() throws Exception {
      super.execute();
    }

    @Override
    protected String getCurrentFile(FileSystem fs, FileStatus[] files)
        throws IOException {
      return new String("file" + new Integer(number_files).toString());
    }

    public void publishMissingPaths(FileSystem fs, long commitTime,
        String categoryName) throws Exception {
      super.publishMissingPaths(fs, commitTime, categoryName);
    }
  }
}
