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


import com.inmobi.databus.AbstractService;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

  public LocalStreamService(DatabusConfig config, Cluster cluster) {
    super(LocalStreamService.class.getName(), config);
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
  protected void execute() throws Exception {
    try {

      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      //Cleanup tmpPath before everyRun to avoid
      //any old data being used in this run if the old run was aborted
      cleanUpTmp(fs);
      LOG.info("TmpPath is [" + tmpPath + "]");
      Map<FileStatus, String> fileListing = new HashMap<FileStatus, String>();
      createMRInput(tmpJobInputPath, fileListing);
      if (fileListing.size() == 0) {
        LOG.info("Nothing to do!");
        return;
      }
      Job job = createJob(tmpJobInputPath);
      job.waitForCompletion(true);
      if (job.isSuccessful()) {
        long commitTime = cluster.getCommitTime();
        Map<Path, Path> commitPaths = prepareForCommit(commitTime,
                fileListing);
        commit(commitPaths);
        LOG.info("Committed successfully for " + commitTime);
      }
    } catch (Exception e) {
      LOG.warn("Error in running LocalStreamService ["+ e.getMessage() + "]",
              e);
      throw new Exception(e);
    }
  }

  private Map<Path, Path> prepareForCommit(long commitTime,
                                           Map<FileStatus,
                                                   String> fileListing)
          throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());

    // find final destination paths
    Map<Path, Path> mvPaths = new LinkedHashMap<Path, Path>();
    FileStatus[] categories = fs.listStatus(tmpJobOutputPath);
    for (FileStatus categoryDir : categories) {
      Path destDir = new Path(cluster.getLocalDestDir(
              categoryDir.getPath().getName(), commitTime));
      FileStatus[] files = fs.listStatus(categoryDir.getPath());
      for (FileStatus file : files) {
        Path destPath = new Path(destDir, file.getPath().getName());
        mvPaths.put(file.getPath(), destPath);
      }
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
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter
                (out));
        for (Path destPath : mvPaths.values()) {
          String category = getCategoryFromDestPath(destPath);
          if (clusterEntry.getDestinationStreams().containsKey(category)) {
            out.writeBytes(destPath.toString());
            LOG.debug("Adding [" + destPath + "]  for consumer [" +
                    clusterEntry.getName() + "] to commit Paths in [" +
                    tmpConsumerPath + "]");
            out.writeBytes("\n");
          }
        }
        out.close();
        Path finalConsumerPath = new Path(cluster.getConsumePath(
                clusterEntry),
                Long.toString(System.currentTimeMillis()));
        LOG.debug("Moving [" + tmpConsumerPath + "] to [ " + finalConsumerPath
                +"]");
        consumerCommitPaths.put(tmpConsumerPath, finalConsumerPath);
      }
    }

    // find trash paths
    Map<Path, Path> trashPaths = new LinkedHashMap<Path, Path>();
    Path trash = cluster.getTrashPathWithDate();
    for (FileStatus src : fileListing.keySet()) {
      String category = getCategoryFromSrcPath(src.getPath());
      Path target = null;
      target = new Path(trash, src.getPath().getParent().getName() + "-" +
              src.getPath().getName());
      LOG.debug("Trashing [" + src.getPath() + "] to [" + target + "]");
      trashPaths.put(src.getPath(), target);
    }

    Map<Path, Path> commitPaths = new LinkedHashMap<Path, Path>();
    if (mvPaths.size() == trashPaths.size()) {// validate the no of files
      commitPaths.putAll(mvPaths);
      commitPaths.putAll(consumerCommitPaths);
      commitPaths.putAll(trashPaths);
    }
    return commitPaths;
  }

  private void commit(Map<Path, Path> commitPaths) throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    for (Map.Entry<Path, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
      fs.mkdirs(entry.getValue().getParent());
      if (fs.rename(entry.getKey(), entry.getValue()) == false)
      {
        LOG.warn("Rename failed, aborting transaction COMMIT to avoid " +
                "dataloss. Partial data replay could happen in next run");
        throw new Exception("Abort transaction Commit. Rename failed from ["
                + entry.getKey() + "] to [" + entry.getValue() + "]");
      }
    }

  }

  private void createMRInput(Path inputPath, Map<FileStatus,
          String> fileListing)
          throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());

    createListing(fs, fs.getFileStatus(cluster.getDataDir()), fileListing);

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

  private void createListing(FileSystem fs, FileStatus fileStatus,
                             Map<FileStatus, String> results
  ) throws IOException {
    FileStatus[] streams = fs.listStatus(fileStatus.getPath());
    for (FileStatus stream : streams) {
      FileStatus[] collectors = fs.listStatus(stream.getPath());
      for (FileStatus collector : collectors) {
        FileStatus[] files = fs.listStatus(collector.getPath());
        String currentFile = getCurrentFile(fs, files);
        for (FileStatus file : files) {
          String fileName = file.getPath().getName();
          if (fileName != null) {
            if (!fileName.endsWith("current") && !fileName.equalsIgnoreCase
                    (currentFile) && !fileName.equalsIgnoreCase("scribe_stats")) {
              if (file.getLen() > 0) {
                Path src = file.getPath().makeQualified(fs);
                String category = getCategoryFromSrcPath(src);
                String destDir = getCategoryJobOutTmpPath(category).toString();
                results.put(file, destDir);
              }
              else {
                LOG.info("File [" + file.getPath().getName() + "] of size 0 " +
                        "bytes found. Deleting it");
                fs.delete(file.getPath());
              }
            }
          }
        }
      }
    }
  }

  private String getCurrentFile(FileSystem fs, FileStatus[] files) throws IOException{
    for (FileStatus fileStatus : files) {
      if (fileStatus.getPath().getName().endsWith("current")) {
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open
                (fileStatus
                        .getPath())));
        String currentFileName = in.readLine().trim();
        in.close();
        return currentFileName;
      }
    }
    return null;
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
    job.getConfiguration().set("localstream.tmp.path", tmpPath.toString());
    job.setJobName(jobName);
    KeyValueTextInputFormat.setInputPaths(job, inputPath);
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setJarByClass(CopyMapper.class);
    job.setMapperClass(CopyMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(NullOutputFormat.class);
    job.getConfiguration().set("mapred.map.tasks.speculative.execution",
            "false");

    return job;
  }
}
