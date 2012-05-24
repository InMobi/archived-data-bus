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
package com.inmobi.databus;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
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

import com.inmobi.databus.local.LocalStreamService;
import com.inmobi.databus.utils.CalendarHelper;

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
      FileStatus dataDir = new FileStatus(20, false, 3, 23823, 2438232,
          cluster.getDataDir());

      TestLocalStreamService service = new TestLocalStreamService(null,
          cluster, new FSCheckpointProvider("/tmp/databus-checkpoint"));
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

    Map<String, Integer> sourcestreams = new HashMap<String, Integer>();

    sourcestreams.put("cluster1", new Integer(2));

    Map<String, SourceStream> streamMap = new HashMap<String, SourceStream>();
    streamMap.put("stream1", new SourceStream("stream1", sourcestreams));

    sourcestreams.clear();

    /*
     * sourcestreams.put("cluster2", new Integer(2)); streamMap.put("stream2",
     * new SourceStream("stream2", sourcestreams));
     */

    JobConf conf = super.CreateJobConf();

    Map<String, Cluster> clusterMap = new HashMap<String, Cluster>();

    clusterMap.put(
        "cluster1",
        ClusterTest.buildLocalCluster("cluster1", "file:///tmp",
            conf.get("mapred.job.tracker")));

    /*
     * clusterMap.put( "cluster2", ClusterTest.buildLocalCluster("cluster2",
     * "file:///tmp", conf.get("mapred.job.tracker")));
     */

    return new DatabusConfig(streamMap, clusterMap);
  }

  private String getDateAsYYYYMMDD(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    return dateFormat.format(date);
  }

  private String getDateAsYYYYMMDDHHPath(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy" + File.separator + "MM"
        + File.separator + "dd" + File.separator + "HH" + File.separator);
    return dateFormat.format(date);
  }

  private String getDateAsYYYYMMDDHHMMSS(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss_SSSSS");
    return dateFormat.format(date);
  }

  @Test(groups = { "integration" })
  public void testMapReduce() throws Exception {

    final int NUM_OF_FILES = 35;
    FileSystem fs = FileSystem.getLocal(new Configuration());

    fs.delete(new Path("/tmp/databus"), true);
    DatabusConfig config = buildTestDatabusConfig();

    Set<LocalStreamService> services = new HashSet<LocalStreamService>();

    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      services.add(new LocalStreamService(config, cluster.getValue(),
          new FSCheckpointProvider(cluster.getValue().getCheckpointDir())));
    }

    for (int i = 0; i < 1; ++i) {

      String[] files = new String[NUM_OF_FILES];
      fs.mkdirs(new Path("/tmp/databus/data/stream1/cluster1/"));
      for (int j = 0; j < NUM_OF_FILES; ++j) {
        files[j] = new String("stream1-" + getDateAsYYYYMMDDHHMMSS(new Date()));
        Path path = new Path("/tmp/databus/data/stream1/cluster1/" + files[j]);

        FSDataOutputStream streamout = fs.create(path);
        streamout.writeBytes("Creating Test data for teststream " + files[j]);
        streamout.close();

        Assert.assertTrue(fs.exists(path));
        /*
         * fs.mkdirs(new Path("/tmp/databus/data/stream2/cluster2/"));
         * fs.create(new Path("/tmp/databus/data/stream2/cluster2/" + files[i]))
         * .close();
         */
      }

      for (LocalStreamService service : services) {
        service.start();
      }

      // Wait for 15 seconds to make sure atleast one run has been done
      Thread.sleep(15000);

      for (LocalStreamService service : services) {
        service.stop();
      }
      for (int dates = 0; dates < services.size(); ++dates) {

        Date todaysdate = new Date();
        String trashpath = "/tmp/databus/system/trash/"
            + CalendarHelper.getCurrentDateAsString();
        String commitpath = "/tmp/databus/streams_local/stream" + (dates + 1)
            + "/" + getDateAsYYYYMMDDHHPath(todaysdate);
        String checkpointpath = "/tmp/databus/system/checkpoint/";
        FileStatus[] mindirs = fs.listStatus(new Path(commitpath));

        for (FileStatus mindir : mindirs) {

          try {
            Integer.parseInt(mindir.getPath().getName());
            String streams_local_dir = commitpath + mindir.getPath().getName()
                + "/cluster" + (dates + 1);

            LOG.debug("Checking in Path for mapred Output: "
                + streams_local_dir);

            for (int j = 0; j < NUM_OF_FILES; ++j) {
              Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                  + files[j] + ".gz")));
            }

            Path checkpointfile = new Path(checkpointpath + "stream"
                + (dates + 1) + "cluster" + (dates + 1) + ".ck");

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
                String TrashFile = trashpath + File.separator + "cluster"
                    + (dates + 1) + "-" + files[j];
                LOG.debug("Verifying Trash Path " + TrashFile);
                Assert.assertTrue(fs.exists(new Path(TrashFile)));
              } else
                break;
            }

            break;
          } catch (NumberFormatException e) {

          }
        }
      }

    }

    fs.delete(new Path("/tmp/databus"), true);
    fs.close();
  }

  class TestLocalStreamService extends LocalStreamService {

    public TestLocalStreamService(DatabusConfig config, Cluster cluster,
        CheckpointProvider provider) {
      super(config, cluster, provider);
    }

    @Override
    protected String getCurrentFile(FileSystem fs, FileStatus[] files)
        throws IOException {
      return new String("file" + new Integer(number_files).toString());
    }

  }
}