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

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.databus.local.LocalStreamService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

@Test
public class LocalStreamServiceTest {
  private static Logger LOG = Logger.getLogger(LocalStreamServiceTest.class);
  private final int number_files = 9;

  Set<String> expectedResults = new LinkedHashSet<String>();
  Set<String> expectedTrashPaths = new LinkedHashSet<String>();
  Map<String, String> expectedCheckPointPaths = new HashMap<String, String>();

  public LocalStreamServiceTest() {
    createExceptedOutput();
  }

  private void createExceptedOutput() {
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
                                      Set<String> trashPaths, Map<String,
  String> checkPointPaths) {
    assert results.equals(expectedResults);
    assert trashPaths.equals(expectedTrashPaths);
    assert checkPointPaths.equals(expectedCheckPointPaths);
  }

  private void createMockForFileSystem(FileSystem fs, Cluster cluster) throws
  Exception{
    FileStatus[] files = createTestData(2, "/databus/data/stream", true);

    FileStatus[] stream1 = createTestData(2,
    "/databus/data/stream1/collector", true);

    FileStatus[] stream3 = createTestData(number_files,
    "/databus/data/stream1/collector1/file", true);

    FileStatus[] stream4 = createTestData(number_files,
    "/databus/data/stream1/collector2/file", true);

    FileStatus[] stream2 = createTestData(2,
    "/databus/data/stream2/collector", true);


    FileStatus[] stream5 = createTestData(number_files,
    "/databus/data/stream2/collector1/file", true);

    FileStatus[] stream6 = createTestData(number_files,
    "/databus/data/stream2/collector2/file", true);

    when(fs.getWorkingDirectory()).thenReturn(new Path("/tmp/"));
    when(fs.getUri()).thenReturn(new URI("localhost"));
    when(fs.listStatus(cluster.getDataDir())).thenReturn(files);
    when(fs.listStatus(new Path("/databus/data/stream1"))).thenReturn
    (stream1);
    when(fs.listStatus(new Path("/databus/data/stream1/collector1")))
    .thenReturn
    (stream3);
    when(fs.listStatus(new Path("/databus/data/stream2"))).thenReturn
    (stream2);
    when(fs.listStatus(new Path("/databus/data/stream1/collector2")))
    .thenReturn(stream4);

    when(fs.listStatus(new Path("/databus/data/stream2/collector1")))
    .thenReturn(stream5);
    when(fs.listStatus(new Path("/databus/data/stream2/collector2")))
    .thenReturn(stream6);

    Path file = mock(Path.class);
    when(file.makeQualified(any(FileSystem.class))).thenReturn(new Path
    ("/databus/data/stream1/collector1/"));
  }

  public void testCreateListing() {
    try {
      Cluster cluster = ClusterTest.buildLocalCluster();
      FileSystem fs = mock(FileSystem.class);
      createMockForFileSystem(fs, cluster);

      Map<FileStatus, String> results = new TreeMap<FileStatus, java.lang.String>();
      Set<FileStatus> trashSet = new HashSet<FileStatus>();
      Map<String, FileStatus> checkpointPaths = new HashMap<String,
      FileStatus>();
      FileStatus dataDir = new FileStatus(20, false, 3, 23823, 2438232,
      cluster.getDataDir());

      TestLocalStreamService service = new TestLocalStreamService(null,
      cluster, new FSCheckpointProvider("/tmp/databus-checkpoint"));
      service.createListing(fs, dataDir, results, trashSet, checkpointPaths);

      Set<String> tmpResults = new LinkedHashSet<String>();
      //print the results
      for (FileStatus status : results.keySet()) {
        tmpResults.add(status.getPath().toString());
        LOG.debug("Results [" + status.getPath().toString() + "]");
      }

      //print the trash
      Iterator<FileStatus> it = trashSet.iterator();
      Set<String> tmpTrashPaths = new LinkedHashSet<String>();
      while (it.hasNext()) {
        FileStatus trashfile = it.next();
        tmpTrashPaths.add(trashfile.getPath().toString());
        LOG.debug("trash file [" + trashfile.getPath());
      }

      Map<String, String> tmpCheckPointPaths = new TreeMap<String, String>();
      //print checkPointPaths
      for (String key : checkpointPaths.keySet()) {
        tmpCheckPointPaths.put(key, checkpointPaths
        .get(key).getPath().getName());
        LOG.debug("CheckPoint key [" + key + "] value [" + checkpointPaths
        .get(key).getPath().getName() +"]");
      }
      validateExpectedOutput(tmpResults, tmpTrashPaths, tmpCheckPointPaths);
    }
    catch (Exception e) {
      LOG.debug("Error in running testCreateListing", e);
      assert false;
    }
  }

  private FileStatus[] createTestData(int count, String path,
                                      boolean useSuffix) {
    FileStatus[] files = new FileStatus[count];
    for(int i=1; i<=count; i++) {
      if (useSuffix)
        files[i-1] = new FileStatus(20, false, 3, 23232, 232323,
        new Path(path + new Integer(i).toString()));
      else
        files[i-1] = new FileStatus(20, false, 3, 232323, 232323,
        new Path(path));


    }
    return files;
  }

  private FileStatus[] createTestData(int count, String path) {
    return createTestData(count, path, false);
  }

  class TestLocalStreamService extends LocalStreamService {

    public TestLocalStreamService(DatabusConfig config, Cluster cluster, CheckpointProvider provider) {
      super(config, cluster, provider);
    }

    @Override
    protected String getCurrentFile(FileSystem fs, FileStatus[] files) throws
    IOException {
      return new String("file" + new Integer(number_files).toString());
    }


  }
}

