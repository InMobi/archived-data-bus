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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Handles MergedStreams for a Cluster
 */

public class MergedStreamService extends DistcpBaseService {

  private static final Log LOG = LogFactory.getLog(MergedStreamService.class);
  private Map<String, Set<Path>> missingDirsCommittedPaths = new HashMap<String, Set<Path>>();

  public MergedStreamService(DatabusConfig config, Cluster srcCluster,
                             Cluster destinationCluster) throws Exception {
    super(config, MergedStreamService.class.getName(), srcCluster,
            destinationCluster);
  }

  @Override
  public void execute() throws Exception {
    try {
      boolean skipCommit = false;
      Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
      

      Path tmpOut = new Path(getDestCluster().getTmpPath(),
              "distcp_mergedStream_" + getSrcCluster().getName() + "_"
                      + getDestCluster().getName()).makeQualified(getDestFs());
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

      synchronized (getDestCluster()) {
        addPublishMissingPaths(missingDirsCommittedPaths, -1, null);
      }

      Path inputFilePath = getDistCPInputFile(consumePaths, tmp);
      if (inputFilePath == null) {
        LOG.warn("No data to pull from " + "Cluster ["
                + getSrcCluster().getHdfsUrl() + "]" + " to Cluster ["
                + getDestCluster().getHdfsUrl() + "]");
        if (missingDirsCommittedPaths.size() > 0) {
          LOG.warn("Adding Missing Directories for Pull "
              + missingDirsCommittedPaths.size());
          commitMirroredConsumerPaths(missingDirsCommittedPaths, tmp);
        }
        return;
      }
      LOG.warn("Starting a distcp pull from [" + inputFilePath.toString()
              + "] " + "Cluster [" + getSrcCluster().getHdfsUrl() + "]"
              + " to Cluster [" + getDestCluster().getHdfsUrl() + "] " + " Path ["
              + tmpOut.toString() + "]");

      try {
        if (!executeDistCp(getDistCpOptions(inputFilePath, tmpOut)))
          skipCommit = true;
      } catch (Throwable e) {
        LOG.warn("Error in distcp", e);
        LOG.warn("Problem in MergedStream distcp PULL..skipping commit for this run");
        skipCommit = true;
      }
      Map<String, Set<Path>> committedPaths = null;

      // if success
      if (!skipCommit) {
        Map<String, List<Path>> categoriesToCommit = prepareForCommit(tmpOut);
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          addPublishMissingPaths(missingDirsCommittedPaths, commitTime,
              categoriesToCommit.keySet());

          // category, Set of Paths to commit
          committedPaths = doLocalCommit(commitTime, categoriesToCommit);

          for (Map.Entry<String, Set<Path>> entry : missingDirsCommittedPaths
              .entrySet()) {
            Set<Path> filesList = committedPaths.get(entry.getKey());
            if (filesList != null)
              filesList.addAll(entry.getValue());
            else
              committedPaths.put(entry.getKey(), entry.getValue());
          }
        }
        // Prepare paths for MirrorStreamConsumerService
        commitMirroredConsumerPaths(committedPaths, tmp);
        // Cleanup happens in parallel without sync
        // no race is there in consumePaths, tmpOut
        doFinalCommit(consumePaths);
      }
      // rmr tmpOut cleanup
      getDestFs().delete(tmpOut, true);
      LOG.debug("Deleting [" + tmpOut + "]");
    } catch (Exception e) {
			LOG.warn("Error in run ", e);
      throw new Exception(e);
    }
  }
  
  private void addPublishMissingPaths(
      Map<String, Set<Path>> missingDirsCommittedPaths, long commitTime,
      Set<String> categoriesToCommit)
      throws Exception {
      Map<String, Set<Path>> missingDirsforCategory = null;
      
      if(categoriesToCommit!=null) {
        missingDirsforCategory = new HashMap<String, Set<Path>>();
        for (String category : categoriesToCommit) {
          Set<Path> missingDirectories = publishMissingPaths(getDestFs(),
              getDestCluster().getFinalDestDirRoot(), commitTime, category);
          missingDirsforCategory.put(category, missingDirectories);
        }
      } else {
        missingDirsforCategory = publishMissingPaths(
          getDestFs(), getDestCluster().getFinalDestDirRoot());
      }

      if (missingDirsforCategory != null) {
        for (Map.Entry<String, Set<Path>> entry : missingDirsforCategory
            .entrySet()) {
          LOG.debug("Add Missing Directories to Commit Path: "
              + entry.getValue().size());
            if (missingDirsCommittedPaths.get(entry.getKey()) != null) {
              Set<Path> missingPaths = missingDirsCommittedPaths.get(entry
                  .getKey());
              missingPaths.addAll(entry.getValue());
            } else {
              missingDirsCommittedPaths.put(entry.getKey(), entry.getValue());
          }
        }
      }
  }

  /*
   * @param Map<String, Set<Path>> commitedPaths - Stream Name, It's committed
   * Path.
   */
  private void commitMirroredConsumerPaths(
          Map<String, Set<Path>> committedPaths, Path tmp) throws Exception {
    // Map of Stream and clusters where it's mirrored
    Map<String, Set<Cluster>> mirrorStreamConsumers = new HashMap<String, Set<Cluster>>();
    Map<Path, Path> consumerCommitPaths = new LinkedHashMap<Path, Path>();
    // for each stream in committedPaths
    for (String stream : committedPaths.keySet()) {
      // for each cluster
      for (Cluster cluster : getConfig().getClusters().values()) {
        // is this stream to be mirrored on this cluster
        if (cluster.getMirroredStreams().contains(stream)) {
          Set<Cluster> mirrorConsumers = mirrorStreamConsumers.get(stream);
          if (mirrorConsumers == null)
            mirrorConsumers = new HashSet<Cluster>();
          mirrorConsumers.add(cluster);
          mirrorStreamConsumers.put(stream, mirrorConsumers);
        }
      }
    } // for each stream

    // Commit paths for each consumer
    for (String stream : committedPaths.keySet()) {
      // consumers for this stream
      Set<Cluster> consumers = mirrorStreamConsumers.get(stream);
      Path tmpConsumerPath;
			if (consumers == null || consumers.size() == 0) {
				LOG.warn(" Consumers is empty for stream [" + stream + "]");
				continue;
			}
      for (Cluster consumer : consumers) {
        // commit paths for this consumer, this stream
        // adding srcCluster avoids two Remote Copiers creating same filename
        String tmpPath = "src_" + getSrcCluster().getName() + "_via_"
                + getDestCluster().getName() + "_mirrorto_" + consumer.getName()
                + "_" + stream;
        tmpConsumerPath = new Path(tmp, tmpPath);
        FSDataOutputStream out = getDestFs().create(tmpConsumerPath);
        try {
          for (Path path : committedPaths.get(stream)) {
            LOG.debug("Writing Mirror Commit Path [" + path.toString() + "]");
            out.writeBytes(path.toString());
            out.writeBytes("\n");
          }
        } finally {
          out.close();
        }
        // Two MergedStreamConsumers will write file for same consumer within
        // the same time
        // adding srcCLuster name avoids that conflict
        Path finalMirrorPath = new Path(getDestCluster().getMirrorConsumePath(
                consumer), tmpPath + "_"
                + new Long(System.currentTimeMillis()).toString());
        consumerCommitPaths.put(tmpConsumerPath, finalMirrorPath);
      } // for each consumer
    } // for each stream
    
		if (consumerCommitPaths == null || consumerCommitPaths.size() == 0) {
			LOG.info("consumerCommitPaths is empty for all stream, skipping mirrorCommit");
      missingDirsCommittedPaths.clear();
			return;
		}
    // Do the final mirrorCommit
    LOG.info("Committing [" + consumerCommitPaths.size() + "] paths for " +
            "mirrored Stream");
    FileSystem fs = FileSystem.get(getDestCluster().getHadoopConf());
    for (Map.Entry<Path, Path> entry : consumerCommitPaths.entrySet()) {
      LOG.info("Renaming [" + entry.getKey() + "] to [" + entry.getValue() +
              "]");
      fs.mkdirs(entry.getValue().getParent());
      if (fs.rename(entry.getKey(), entry.getValue()) == false) {
        LOG.warn("Failed to Commit for Mirrored Path. Aborting Transaction " +
                "to avoid DATA LOSS, " +
                "Partial data replay can happen for merged and mirror stream");
        throw new Exception("Rename failed from ["+ entry.getKey() +"] to ["
                + entry.getValue() +"]");
      }
    }
    missingDirsCommittedPaths.clear();
  }

  private Map<String, List<Path>> prepareForCommit(Path tmpOut)
          throws Exception {
    Map<String, List<Path>> categoriesToCommit = new HashMap<String, List<Path>>();
    FileStatus[] allFiles = getDestFs().listStatus(tmpOut);
    for (int i = 0; i < allFiles.length; i++) {
      String fileName = allFiles[i].getPath().getName();
      if (fileName != null) {
        String category = getCategoryFromFileName(fileName, getDestCluster()
            .getPrimaryDestinationStreams());
        if (category != null) {
          Path intermediatePath = new Path(tmpOut, category);
          if (!getDestFs().exists(intermediatePath))
            getDestFs().mkdirs(intermediatePath);
          Path source = allFiles[i].getPath().makeQualified(getDestFs());

          Path intermediateFilePath = new Path(intermediatePath.makeQualified(
                  getDestFs()).toString()
                  + File.separator + fileName);
          if (getDestFs().rename(source, intermediateFilePath) == false) {
            LOG.warn("Failed to Rename [" + source +"] to [" +
                    intermediateFilePath +"]");
            LOG.warn("Aborting Tranasction prepareForCommit to avoid data " +
                    "LOSS. Retry would happen in next run");
            throw new Exception("Rename [" + source + "] to [" +
                    intermediateFilePath + "]");
          }
          LOG.debug("Moving [" + source + "] to intermediateFilePath ["
                  + intermediateFilePath + "]");
          List<Path> fileList = categoriesToCommit.get(category);
          if (fileList == null) {
            fileList = new ArrayList<Path>();
            fileList.add(intermediateFilePath.makeQualified(getDestFs()));
            categoriesToCommit.put(category, fileList);
          } else {
            fileList.add(intermediateFilePath);
          }
        }
      }
    }
    return categoriesToCommit;
  }

  /*
   * @returns Map<String, Set<Path>> - Map of StreamName, Set of paths committed
   * for stream
   */
  private Map<String, Set<Path>> doLocalCommit(long commitTime,
                                               Map<String, List<Path>> categoriesToCommit) throws Exception {
    Map<String, Set<Path>> comittedPaths = new HashMap<String, Set<Path>>();
    Set<Map.Entry<String, List<Path>>> commitEntries = categoriesToCommit
            .entrySet();
    Iterator it = commitEntries.iterator();
    while (it.hasNext()) {
      Map.Entry<String, List<Path>> entry = (Map.Entry<String, List<Path>>) it
              .next();
      String category = entry.getKey();
      List<Path> filesInCategory = entry.getValue();
      for (Path filePath : filesInCategory) {
        Path destParentPath = new Path(getDestCluster().getFinalDestDir(
                category, commitTime));
        if (!getDestFs().exists(destParentPath)) {
          getDestFs().mkdirs(destParentPath);
        }
        LOG.debug("Moving from intermediatePath [" + filePath + "] to ["
                + destParentPath + "]");
        if (getDestFs().rename(filePath, destParentPath) == false) {
          LOG.warn("Rename failed, aborting transaction COMMIT to avoid " +
                  "dataloss. Partial data replay could happen in next run");
          throw new Exception("Abort transaction Commit. Rename failed from ["
                  + filePath + "] to [" + destParentPath + "]");
        }
        Path commitPath = new Path(destParentPath, filePath.getName());
        Set<Path> commitPaths = comittedPaths.get(category);
        if (commitPaths == null) {
          commitPaths = new HashSet<Path>();
        }
        commitPaths.add(commitPath);
        comittedPaths.put(category, commitPaths);
      }
    }
    return comittedPaths;
  }

  protected Path getInputPath() throws IOException {
    return getSrcCluster().getConsumePath(getDestCluster());

  }
}
