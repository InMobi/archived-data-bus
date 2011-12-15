package com.inmobi.databus.consume;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Stream;

public class DataConsumer extends AbstractCopier {

  private static final Log LOG = LogFactory.getLog(DataConsumer.class);

  public DataConsumer(DatabusConfig config) {
    super(config, config.getDestinationCluster());
  }

  protected void addStreamsToFetch() {
    for (Stream s : getConfig().getStreams().values()) {
      if (s.sourceClusters.contains(getSrcCluster())) {
        streamsToFetch.add(s);
      }
    }
  }

  @Override
  public void run() {
    Map<FileStatus, String> fileListing = new HashMap<FileStatus, String>();
    Path inputPath = new Path(getConfig().getTmpPath(), "input");
    try {
      createMRInput(inputPath, fileListing);

      Job job = createJob(inputPath);
      job.waitForCompletion(true);
      if (job.isSuccessful()) {
        // TODO:delete the source paths
        // This must be the last thing done in this execution
        //trashSrcs(fileListing.keySet());
      }
    } catch(Exception e) {
      LOG.warn("Failed to fetch from local", e);
    }
    
  }

  private void createMRInput(Path inputPath, Map<FileStatus, String> fileListing)
      throws IOException {
    FileSystem fs = FileSystem.get(getConfig().getHadoopConf());

    createListing(fs, fs.getFileStatus(getConfig().getDataDir()), fileListing,
        new HashSet<String>());

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
      Map<FileStatus, String> results, Set<String> excludes) throws IOException {

    if (fileStatus.isDir()) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
        createListing(fs, stat, results, excludes);
      }
    } else {
      String fileName = fileStatus.getPath().getName();
      if (fileName.endsWith("current")) {
        FSDataInputStream in = fs.open(fileStatus.getPath());
        String currentFileName = in.readLine();
        in.close();
        excludes.add(currentFileName);
      } else if ("scribe_stats".equalsIgnoreCase(fileName.trim())) {
        excludes.add(fileName);
      } else if (!excludes.contains(fileName)) {
        Path src = fileStatus.getPath().makeQualified(fs);
        String category = src.getParent().getParent().getName();
        String destDir = getConfig().getDestDir(category);
        results.put(fileStatus, destDir);
      }
    }
  }

  private Job createJob(Path inputPath) throws IOException {
    String jobName = "consumer";
    Configuration conf = new Configuration();
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

    return job;
  }

  /*private void trashSrcs(Collection<FileStatus> srcs)
      throws IOException {
    FileSystem fs = FileSystem.get(getConfig().getHadoopConf());
    Path trash = getConfig().getTrashPath();
    fs.mkdirs(trash);
    for (FileStatus src : srcs) {
      LOG.info("Deleting src " + src);
      Path target = new Path(trash, src.toString());
      fs.rename(src.getPath(), target);
    }
  }*/
}
