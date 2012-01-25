package com.inmobi.databus.distcp;
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
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* Assumption - Mirror is always of a merged Stream.There is only 1 instance of a merged Stream
 * (i)   1 Mirror Thread per src DatabusConfig.Cluster from where streams need to be mirrored on destCluster
 * (ii)  Mirror stream and mergedStream can't coexist on same Cluster
 * (iii) Mirror stream and merged Stream threads don't race with each other as they work on different
 * streams based on assumption(ii)
 */

public class MirrorStreamService extends DistcpBaseService {
  private static final Log LOG = LogFactory.getLog(MirrorStreamService.class);

  public MirrorStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster) throws Exception {
    super(config, MirrorStreamService.class.getName(), srcCluster,
        destinationCluster);
  }

  @Override
  protected Path getInputPath() throws IOException {
    return getSrcCluster().getMirrorConsumePath(getDestCluster());
  }

  @Override
  protected void execute() throws Exception {

    try {
      boolean skipCommit = false;
      Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();

      Path tmpOut = new Path(getDestCluster().getTmpPath(), "distcp_mirror_"
          + getSrcCluster().getName() + "_" + getDestCluster().getName())
          .makeQualified(getDestFs());
      // CleanuptmpOut before every run
      if (getDestFs().exists(tmpOut))
        getDestFs().delete(tmpOut, true);
      if (!getDestFs().mkdirs(tmpOut)) {
        LOG.warn("Cannot create [" + tmpOut + "]..skipping this run");
        return;
      }
      Path tmp = new Path(tmpOut, "tmp");
      if (!getDestFs().mkdirs(tmp)) {
        LOG.warn("Cannot create [" + tmp + "]..skipping this run");
        return;
      }

      Path inputFilePath = getInputFilePath(consumePaths, tmp);
      if (inputFilePath == null) {
        LOG.warn("No data to pull from " + "Cluster ["
            + getSrcCluster().getHdfsUrl() + "]" + " to Cluster ["
            + getDestCluster().getHdfsUrl() + "]");
        return;
      }

      LOG.warn("Starting a Mirrored distcp pull from ["
          + inputFilePath.toString() + "] " + "Cluster ["
          + getSrcCluster().getHdfsUrl() + "]" + " to Cluster ["
          + getDestCluster().getHdfsUrl() + "] " + " Path ["
          + tmpOut.toString() + "]");

      String[] args = { "-preserveSrcPath", "-f", inputFilePath.toString(),
          tmpOut.toString() };
      try {
        int exitCode = DistCp.runDistCp(args, getDestCluster().getHadoopConf());
        if (exitCode != DISTCP_SUCCESS)
          skipCommit = true;
      } catch (Throwable e) {
        LOG.warn("Problem in Mirrored distcp..skipping commit for this run",
                e);
        skipCommit = true;
      }
      if (!skipCommit) {
        Map<Path, Path> commitPaths = prepareForCommit(tmpOut);
        doLocalCommit(commitPaths);
        doFinalCommit(consumePaths);
      }
      getDestFs().delete(tmpOut, true);
      LOG.debug("Cleanup [" + tmpOut + "]");
    } catch (Exception e) {
      LOG.warn(e);
      LOG.warn("Error in MirrorStream Service..skipping RUN ", e);
     }
  }

  void doLocalCommit(Map<Path, Path> commitPaths) throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    for (Map.Entry<Path, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
      getDestFs().mkdirs(entry.getValue().getParent());
      getDestFs().rename(entry.getKey(), entry.getValue());
    }
  }

  /*
   * @param Map<Path, Path> commitPaths - srcPath, destPath
   * 
   * @returns Path - tmpOut
   */
  Map<Path, Path> prepareForCommit(Path tmpOut) throws Exception {
    /*
     * tmpOut would be like -
     * /databus/system/tmp/distcp_mirror_<srcCluster>_<destCluster>/ After
     * distcp paths inside tmpOut would be eg:
     * /databus/system/distcp_mirror_ua1_uj1
     * /databus/streams/metric_billing/2012/1/13/15/7/
     * gs1104.grid.corp.inmobi.com-metric_billing-2012-01-16-07-21_00000.gz
     * tmpStreamRoot eg: /databus/system/distcp_mirror_ua1_uj1/databus/streams/
     */
    Path tmpStreamRoot = new Path(tmpOut.makeQualified(getDestFs()).toString()
        + File.separator + getSrcCluster().getUnqaulifiedFinalDestDirRoot());
    LOG.debug("tmpStreamRoot [" + tmpStreamRoot + "]");
    // tmpStreamRoot eg -
    // /databus/system/tmp/distcp_mirror_ua1_uj1/databus/streams/
    // multiple streams can get mirrored from the same cluster
    FileStatus[] fileStatuses = getDestFs().listStatus(tmpStreamRoot);

    Map<Path, Path> commitPaths = new HashMap<Path, Path>();
    if (fileStatuses != null) {
      for (FileStatus streamRoot : fileStatuses) {
        List<FileStatus> streamPaths = new ArrayList<FileStatus>();
        createListing(getDestFs(), streamRoot, streamPaths);
        orderPathsByTime(streamPaths);
        createCommitPaths(commitPaths, streamPaths);
      }
    }
    return commitPaths;
  }

  class PathComparator implements Comparator<FileStatus> {

    @Override
    public int compare(FileStatus fileStatus, FileStatus fileStatus1) {
      /*
       * Path eg:
       * /databus/system/distcp_mirror_ua1_uj1/databus/streams/metric_billing
       * /2012/1/13/15/7/
       * gs1104.grid.corp.inmobi.com-metric_billing-2012-01-16-07-21_00000.gz
       */
      String path1 = fileStatus.getPath().toString();
      String path2 = fileStatus1.getPath().toString();
      String[] pathList1 = path1.split(File.separator);
      String[] pathList2 = path2.split(File.separator);
      int min1 = new Integer(pathList1[pathList1.length - 2]).intValue();
      int hr1 = new Integer(pathList1[pathList1.length - 3]).intValue();
      int day1 = new Integer(pathList1[pathList1.length - 4]).intValue();
      int month1 = new Integer(pathList1[pathList1.length - 5]).intValue();
      int year1 = new Integer(pathList1[pathList1.length - 6]).intValue();

      int min2 = new Integer(pathList2[pathList1.length - 2]).intValue();
      int hr2 = new Integer(pathList2[pathList1.length - 3]).intValue();
      int day2 = new Integer(pathList2[pathList1.length - 4]).intValue();
      int month2 = new Integer(pathList2[pathList1.length - 5]).intValue();
      int year2 = new Integer(pathList2[pathList1.length - 6]).intValue();

      int retVal = compareVal(year1, year2);
      if (retVal != 0)
        return retVal;
      retVal = compareVal(month1, month2);
      if (retVal != 0)
        return retVal;
      retVal = compareVal(day1, day2);
      if (retVal != 0)
        return retVal;
      retVal = compareVal(hr1, hr2);
      if (retVal != 0)
        return retVal;
      retVal = compareVal(min1, min2);
      return retVal;

    }

    private int compareVal(int val1, int val2) {
      if (val1 > val2)
        return 1;
      else if (val1 < val2)
        return -1;
      else
        return 0;
    }

  }

  private void createCommitPaths(Map<Path, Path> commitPaths,
      List<FileStatus> streamPaths) {
    // Path eg in streamPaths -
    // /databus/system/distcp_mirror_ua1_uj1/databus/streams/metric_billing/2012/1/13/15/7
    // gs1104.grid.corp.inmobi.com-metric_billing-2012-01-16-07-21_00000.gz
    for (FileStatus fileStatus : streamPaths) {
      String filePath = fileStatus.getPath().toString();
      String[] path = filePath.split(File.separator);
      String fileName = path[path.length - 1];
      String min = path[path.length - 2];
      String hr = path[path.length - 3];
      String day = path[path.length - 4];
      String month = path[path.length - 5];
      String year = path[path.length - 6];
      String streamName = path[path.length - 7];
      String finalPath = getDestCluster().getFinalDestDirRoot()
          + File.separator + streamName + File.separator + year
          + File.separator + month + File.separator + day + File.separator + hr
          + File.separator + min + File.separator + fileName;
      commitPaths.put(fileStatus.getPath(), new Path(finalPath));
    }

  }

  private void orderPathsByTime(List<FileStatus> streamPaths) {
    Collections.sort(streamPaths, new PathComparator());

  }

  private void createListing(FileSystem fs, FileStatus fileStatus,
      List<FileStatus> results) throws IOException {
    if (fileStatus.isDir()) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
        createListing(fs, stat, results);
      }
    } else {
      LOG.debug("createListing :: Adding [" + fileStatus.getPath().toString()
          + "]");
      results.add(fileStatus);
    }
  }
}
