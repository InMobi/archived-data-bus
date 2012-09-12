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
package com.inmobi.databus.distcp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.utils.DatePathComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;

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
      LinkedHashMap<Path, FileSystem> consumePaths = new LinkedHashMap<Path,
      FileSystem>();

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

      DistCpOptions options = getDistCpOptions(inputFilePath, tmpOut);
      options.setPreserveSrcPath(true);

      try {
        if (!executeDistCp(options))
          skipCommit = true;
      } catch (Throwable e) {
        LOG.warn("Problem in Mirrored distcp..skipping commit for this run",
        e);
        skipCommit = true;
      }
      if (!skipCommit) {
        LinkedHashMap<FileStatus, Path> commitPaths = prepareForCommit(tmpOut);
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

  void doLocalCommit(Map<FileStatus, Path> commitPaths) throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    for (Map.Entry<FileStatus, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming [" + entry.getKey() + "] to [" + entry.getValue()
      +"]");
      if (entry.getKey().isDir()) {
        getDestFs().mkdirs(entry.getValue());
      } else {
        getDestFs().mkdirs(entry.getValue().getParent());
        if (getDestFs().rename(entry.getKey().getPath(), entry.getValue()) == false) {
          LOG.warn("Failed to rename.Aborting transaction COMMIT to avoid "
          + "data loss. Partial data replay could happen in next run");
          throw new Exception("Rename failed from [" + entry.getKey() + "] to "
          + "[" + entry.getValue() + "]");
        }
      }
    }
  }

  /*
   * @returns Map<Path, Path> commitPaths - srcPath, destPath
   * 
   * @param Path - tmpOut
   */
  LinkedHashMap<FileStatus, Path> prepareForCommit(Path tmpOut) throws Exception {
    /*
     * tmpOut would be like -
     * /databus/system/tmp/distcp_mirror_<srcCluster>_<destCluster>/ After
     * distcp paths inside tmpOut would be eg:
     *
     * /databus/system/distcp_mirror_ua1_uj1
     * /databus/streams/<streamName>/2012/1/13/15/7/
     * <hostname>-<streamName>-2012-01-16-07-21_00000.gz
     *
     * tmpStreamRoot eg: /databus/system/distcp_mirror_<srcCluster>_
     * <destCluster>/databus/streams/
     */

    Path tmpStreamRoot = new Path(tmpOut.makeQualified(getDestFs()).toString()
    + File.separator + getSrcCluster().getUnqaulifiedFinalDestDirRoot());
    LOG.debug("tmpStreamRoot [" + tmpStreamRoot + "]");

     /* tmpStreamRoot eg -
      * /databus/system/tmp/distcp_mirror_<srcCluster>_<destCluster>/databus
      * /streams/
      *
      * multiple streams can get mirrored from the same cluster
      * streams can get processed in any order but we have to retain order
      * of paths within a stream*/
    FileStatus[] fileStatuses = getDestFs().listStatus(tmpStreamRoot);

    //Retain the order of commitPaths
    LinkedHashMap<FileStatus, Path> commitPaths = new LinkedHashMap<FileStatus, Path>();
    if (fileStatuses != null) {
      for (FileStatus streamRoot : fileStatuses) {
        //for each stream : list the path in order of YYYY/mm/DD/HH/MM
        LOG.debug("StreamRoot [" + streamRoot.getPath() + "] streamName [" +
        streamRoot.getPath().getName() + "]");
        List<FileStatus> streamPaths = new ArrayList<FileStatus>();
        createListing(getDestFs(), streamRoot, streamPaths);
        Collections.sort(streamPaths, new DatePathComparator());
        LOG.debug("createListing size: [" + streamPaths.size() +"]");
        createCommitPaths(commitPaths, streamPaths);
      }
    }
    return commitPaths;
  }




  private void createCommitPaths(LinkedHashMap<FileStatus, Path> commitPaths,
                                 List<FileStatus> streamPaths) {
   /*  Path eg in streamPaths -
    *  /databus/system/distcp_mirror_<srcCluster>_<destCluster>/databus/streams
    *  /<streamName>/2012/1/13/15/7/<hostname>-<streamName>-2012-01-16-07
    *  -21_00000.gz
    *
    * or it could be an emptyDir like
    *  /* Path eg in streamPaths -
    *  /databus/system/distcp_mirror_<srcCluster>_<destCluster>/databus/streams
    *  /<streamName>/2012/1/13/15/7/
    *
    */

    for (FileStatus fileStatus : streamPaths) {
      String fileName = null;

      Path prefixDir = null;
      if (fileStatus.isDir()) {
        //empty directory
        prefixDir = fileStatus.getPath();
      } else {
        fileName = fileStatus.getPath().getName();
        prefixDir = fileStatus.getPath().getParent();
      }

      Path min = prefixDir;
      Path hr =  min.getParent() ;
      Path day = hr.getParent();
      Path month = day.getParent();
      Path year = month.getParent();
      Path streamName = year.getParent();

      String finalPath = getDestCluster().getFinalDestDirRoot() + File
      .separator + streamName.getName() + File.separator + year.getName() + File
      .separator + month.getName() + File.separator + day.getName() + File
      .separator + hr.getName() + File.separator + min.getName();

      if (fileName != null) {
        finalPath += File.separator + fileName;
      }

      commitPaths.put(fileStatus, new Path(finalPath));
      LOG.debug("Going to commit [" + fileStatus.getPath() + "] to [" +
      finalPath + "]");
    }

  }


  void createListing(FileSystem fs, FileStatus fileStatus,
                             List<FileStatus> results) throws IOException {
    if (fileStatus.isDir()) {
      FileStatus[] stats = fs.listStatus(fileStatus.getPath());
      if (stats.length == 0) {
        results.add(fileStatus);
        LOG.debug("createListing :: Adding [" + fileStatus.getPath() + "]");
      }
      for (FileStatus stat : stats) {
        createListing(fs, stat, results);
      }
    } else {
      LOG.debug("createListing :: Adding [" + fileStatus.getPath()+ "]");
      results.add(fileStatus);
    }
  }
}
