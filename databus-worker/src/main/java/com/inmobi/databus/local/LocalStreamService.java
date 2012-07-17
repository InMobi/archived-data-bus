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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/*
 * Handles Local Streams for a Cluster
 * Assumptions
 * (i) One LocalStreamService per Cluster
 */

public class LocalStreamService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(LocalStreamService.class);

  private final Cluster cluster;
  private Path tmpPath;
  private Path tmpJobInputPath;
  private Path tmpJobOutputPath;
  private final int FILES_TO_KEEP = 6;

  public LocalStreamService(DatabusConfig config, Cluster cluster,
                            CheckpointProvider provider) {
    super(LocalStreamService.class.getName(), config, DEFAULT_RUN_INTERVAL,
    provider);
    this.cluster = cluster;
    this.tmpPath = new Path(cluster.getTmpPath(), getName());
    this.tmpJobInputPath = new Path(tmpPath, "jobIn");
    this.tmpJobOutputPath = new Path(tmpPath, "jobOut");
  }

  private void cleanUpTmp(FileSystem fs) throws Exception {
    if (fs.exists(tmpPath)) {
      LOG.info("Deleting tmpPath recursively [" + tmpPath + "]");
      fs.delete(tmpPath, true);
    }
  }

  @Override
  public long getMSecondsTillNextRun(long currentTime) {
    return (long) (DEFAULT_RUN_INTERVAL - (long) (currentTime % DEFAULT_RUN_INTERVAL));
  }

  @Override
  protected void execute() throws Exception {
    try {

      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      // Cleanup tmpPath before everyRun to avoid
      // any old data being used in this run if the old run was aborted
      cleanUpTmp(fs);
      LOG.info("TmpPath is [" + tmpPath + "]");

      publishMissingPaths(fs, cluster.getLocalFinalDestDirRoot());

      Map<FileStatus, String> fileListing = new TreeMap<FileStatus, String>();
      Set<FileStatus> trashSet = new HashSet<FileStatus>();
      // checkpointKey, CheckPointPath
      Map<String, FileStatus> checkpointPaths = new TreeMap<String, FileStatus>();

      createMRInput(tmpJobInputPath, fileListing, trashSet, checkpointPaths);

      if (fileListing.size() == 0) {
        LOG.info("Nothing to do!");
        return;
      }
      Job job = createJob(tmpJobInputPath);
      job.waitForCompletion(true);
      if (job.isSuccessful()) {
        long commitTime = cluster.getCommitTime();
        LOG.info("Commiting mvPaths and ConsumerPaths");
        commit(prepareForCommit(commitTime, fileListing));
        checkPoint(checkpointPaths);
        LOG.info("Commiting trashPaths");
        commit(populateTrashCommitPaths(trashSet));
        LOG.info("Committed successfully at " + getLogDateString(commitTime));
      }
    } catch (Exception e) {
      LOG.warn("Error in running LocalStreamService " + e);
      throw e;
    }
  }

  private void checkPoint(Map<String, FileStatus> checkPointPaths) {
    Set<Entry<String, FileStatus>> entries = checkPointPaths.entrySet();
    for (Entry<String, FileStatus> entry : entries) {
      String value = entry.getValue().getPath().getName();
      LOG.debug("Check Pointing Key [" + entry.getKey() + "] with value ["
      + value + "]");
      checkpointProvider.checkpoint(entry.getKey(), value.getBytes());
    }
  }

  private Map<Path, Path> prepareForCommit(long commitTime,
                                           Map<FileStatus, String> fileListing) throws Exception {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());

    // find final destination paths
    Map<Path, Path> mvPaths = new LinkedHashMap<Path, Path>();
    FileStatus[] categories = fs.listStatus(tmpJobOutputPath);
    for (FileStatus categoryDir : categories) {
      String categoryName = categoryDir.getPath().getName();
      Path destDir = new Path(cluster.getLocalDestDir(categoryName, commitTime));
      FileStatus[] files = fs.listStatus(categoryDir.getPath());
      for (FileStatus file : files) {
        Path destPath = new Path(destDir, file.getPath().getName());
        LOG.debug("Moving [" + file.getPath() + "] to [" + destPath + "]");
        mvPaths.put(file.getPath(), destPath);
      }
      publishMissingPaths(fs, cluster.getLocalFinalDestDirRoot(), commitTime,
      categoryName);
    }

    // find input files for consumer
    Map<Path, Path> consumerCommitPaths = new HashMap<Path, Path>();
    for (Cluster clusterEntry : getConfig().getClusters().values()) {
      Set<String> destStreams = clusterEntry.getDestinationStreams().keySet();
      boolean consumeCluster = false;
      for (String destStream : destStreams) {
        if (clusterEntry.getPrimaryDestinationStreams().contains(destStream)
        && cluster.getSourceStreams().contains(destStream)) {
          consumeCluster = true;
        }
      }

      if (consumeCluster) {
        Path tmpConsumerPath = new Path(tmpPath, clusterEntry.getName());
        FSDataOutputStream out = fs.create(tmpConsumerPath);
        for (Path destPath : mvPaths.values()) {
          String category = getCategoryFromDestPath(destPath);
          if (clusterEntry.getDestinationStreams().containsKey(category)) {
            out.writeBytes(destPath.toString());
            LOG.debug("Adding [" + destPath + "]  for consumer ["
            + clusterEntry.getName() + "] to commit Paths in ["
            + tmpConsumerPath + "]");
            out.writeBytes("\n");
          }
        }
        out.close();
        Path finalConsumerPath = new Path(cluster.getConsumePath(clusterEntry),
        Long.toString(System.currentTimeMillis()));
        LOG.debug("Moving [" + tmpConsumerPath + "] to [ " + finalConsumerPath
        + "]");
        consumerCommitPaths.put(tmpConsumerPath, finalConsumerPath);
      }
    }

    Map<Path, Path> commitPaths = new LinkedHashMap<Path, Path>();
    commitPaths.putAll(mvPaths);
    commitPaths.putAll(consumerCommitPaths);

    return commitPaths;
  }

  private Map<Path, Path> populateTrashCommitPaths(Set<FileStatus> trashSet) {
    // find trash paths
    Map<Path, Path> trashPaths = new LinkedHashMap<Path, Path>();
    Path trash = cluster.getTrashPathWithDateHour();
    Iterator<FileStatus> it = trashSet.iterator();
    while (it.hasNext()) {
      FileStatus src = it.next();
      Path target = null;
      target = new Path(trash, src.getPath().getParent().getName() + "-"
      + src.getPath().getName());
      LOG.debug("Trashing [" + src.getPath() + "] to [" + target + "]");
      trashPaths.put(src.getPath(), target);
    }
    return trashPaths;
  }

  private void commit(Map<Path, Path> commitPaths) throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    for (Map.Entry<Path, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
      fs.mkdirs(entry.getValue().getParent());
      if (fs.rename(entry.getKey(), entry.getValue()) == false) {
        LOG.warn("Rename failed, aborting transaction COMMIT to avoid "
        + "dataloss. Partial data replay could happen in next run");
        throw new Exception("Abort transaction Commit. Rename failed from ["
        + entry.getKey() + "] to [" + entry.getValue() + "]");
      }
    }

  }

  private void createMRInput(Path inputPath,
                             Map<FileStatus, String> fileListing, Set<FileStatus> trashSet,
                             Map<String, FileStatus> checkpointPaths) throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());

    createListing(fs, fs.getFileStatus(cluster.getDataDir()), fileListing,
    trashSet, checkpointPaths);

    FSDataOutputStream out = fs.create(inputPath);
    Iterator<Entry<FileStatus, String>> it = fileListing.entrySet().iterator();

    while (it.hasNext()) {
      Entry<FileStatus, String> entry = it.next();
      out.writeBytes(entry.getKey().getPath().toString());
      out.writeBytes("\t");
      out.writeBytes(entry.getValue());
      out.writeBytes("\n");
    }
    out.close();
  }



  public void createListing(FileSystem fs, FileStatus fileStatus,
                            Map<FileStatus, String> results, Set<FileStatus> trashSet,
                            Map<String, FileStatus> checkpointPaths) throws
  IOException {
    createListing(fs, fileStatus, results, trashSet, checkpointPaths,
    300000);
  }

	static class CollectorPathFilter implements PathFilter {
		public boolean accept(Path path) {
			if (path.getName().endsWith("current")
			    || path.getName().equalsIgnoreCase("scribe_stats"))
				return false;
			return true;
		}
	}

  public void createListing(FileSystem fs, FileStatus fileStatus,
                            Map<FileStatus, String> results, Set<FileStatus> trashSet,
                            Map<String, FileStatus> checkpointPaths,
                            long lastFileTimeout) throws IOException {
    FileStatus[] streams = fs.listStatus(fileStatus.getPath());
    for (FileStatus stream : streams) {
      String streamName = stream.getPath().getName();
      LOG.debug("createListing working on Stream [" + streamName + "]");
      FileStatus[] collectors = fs.listStatus(stream.getPath());
      for (FileStatus collector : collectors) {
        TreeMap<String, FileStatus> collectorPaths = new TreeMap<String, FileStatus>();
        // check point for this collector
        String collectorName = collector.getPath().getName();
        String checkPointKey = streamName + collectorName;
        String checkPointValue = null;
        byte[] value = checkpointProvider.read(checkPointKey);
        if (value != null)
          checkPointValue = new String(value);
        LOG.debug("CheckPoint Key [" + checkPointKey + "] value [ "
        + checkPointValue + "]");

				FileStatus[] files = fs.listStatus(collector.getPath(),
				    new CollectorPathFilter());

				if(files == null) {
					LOG.warn("No Files Found in the Collector " + collector.getPath()
					    + " Skipping Directory");
					continue;
				}

				String currentFile = getCurrentFile(fs, files, lastFileTimeout);

        for (FileStatus file : files) {
          processFile(file, currentFile, checkPointValue, fs, results,
          collectorPaths);
        }
        populateTrash(collectorPaths, trashSet);
        populateCheckpointPathForCollector(checkpointPaths, collectorPaths,
        checkPointKey);
      } // all files in a collector
    }
  }

  private void processFile(FileStatus file, String currentFile,
                           String checkPointValue, FileSystem fs, Map<FileStatus, String> results,
                           Map<String, FileStatus> collectorPaths) throws IOException {

    String fileName = file.getPath().getName();
    if (fileName != null
    && !fileName.equalsIgnoreCase(currentFile)) {
      if (file.getLen() > 0) {
        Path src = file.getPath().makeQualified(fs);
        String destDir = getCategoryJobOutTmpPath(getCategoryFromSrcPath(src))
        .toString();
        if (aboveCheckpoint(checkPointValue, fileName))
          results.put(file, destDir);
        collectorPaths.put(fileName, file);
      } else {
        LOG.info("File [" + fileName + "] of size 0 bytes found. Deleting it");
        fs.delete(file.getPath(), false);
      }
    }
  }

  private void populateCheckpointPathForCollector(
  Map<String, FileStatus> checkpointPaths,
  TreeMap<String, FileStatus> collectorPaths, String checkpointKey) {
    // Last file in sorted ascending order to be checkpointed for this collector
    if (collectorPaths != null && collectorPaths.size() > 0) {
      Entry<String, FileStatus> entry = collectorPaths.lastEntry();
      checkpointPaths.put(checkpointKey, entry.getValue());
    }
  }

  private void populateTrash(Map<String, FileStatus> collectorPaths,
                             Set<FileStatus> trashSet) {
    if (collectorPaths.size() <= FILES_TO_KEEP)
      return;
    else {
      // put collectorPaths.size() - FILES_TO_KEEP in trash path
      // in ascending order of creation
      Iterator<String> it = collectorPaths.keySet().iterator();
      int trashCnt = (collectorPaths.size() - FILES_TO_KEEP);
      int i = 0;
      while (it.hasNext() && i++ < trashCnt) {
        String fileName = it.next();
        trashSet.add(collectorPaths.get(fileName));
      }
    }
  }

  private boolean aboveCheckpoint(String checkPoint, String file) {
    if (checkPoint == null)
      return true;
    else if (file != null && file.compareTo(checkPoint) > 0) {
      return true;
    } else
      return false;
  }

  /*
   * @returns null: if there are no files or the most significant timestamped
    * file is 5 min back.
   */
  protected String getCurrentFile(FileSystem fs, FileStatus[] files,
                                  long lastFileTimeout)
  {
    //Proposed Algo :-> Sort files based on timestamp
    //if ((currentTimeStamp - last file's timestamp) > 5min ||
    //     if there are no files)
    // then null (implying process this file as non-current file)
    // else
    // return last file as the current file
    class FileTimeStampComparator implements Comparator {
      public int compare(Object o, Object o1) {
        FileStatus file1 = (FileStatus)o;
        FileStatus file2 = (FileStatus)o1;
        long file1Time = file1.getModificationTime();
        long file2Time = file2.getModificationTime();
        if ((file1Time < file2Time))
          return -1;
        else
          return 1;
      }
    }

    if (files == null || files.length == 0)
      return null;
    TreeSet<FileStatus> sortedFiles = new TreeSet<FileStatus>(new
    FileTimeStampComparator());
    for (FileStatus file : files) {
      sortedFiles.add(file);
    }

    //get last file from set
    FileStatus lastFile = sortedFiles.last();

    long currentTime = System.currentTimeMillis();
    long lastFileTime = lastFile.getModificationTime();
    if (currentTime - lastFileTime >= lastFileTimeout) {
      return null;
    } else
      return lastFile.getPath().getName();
  }

  private String getCategoryFromSrcPath(Path src) {
    return src.getParent().getParent().getName();
  }

  private String getCategoryFromDestPath(Path dest) {
    return dest.getParent().getParent().getParent().getParent().getParent()
    .getParent().getName();
  }

  private Path getCategoryJobOutTmpPath(String category) {
    return new Path(tmpJobOutputPath, category);
  }

  private Job createJob(Path inputPath) throws IOException {
    String jobName = "localstream";
    Configuration conf = cluster.getHadoopConf();
    Job job = new Job(conf);
    job.setJobName(jobName);
    KeyValueTextInputFormat.setInputPaths(job, inputPath);
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setJarByClass(CopyMapper.class);
    job.setMapperClass(CopyMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(NullOutputFormat.class);
    job.getConfiguration().set("mapred.map.tasks.speculative.execution",
    "false");
    job.getConfiguration().set("localstream.tmp.path", tmpPath.toString());

    return job;
  }
}
