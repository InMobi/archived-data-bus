package com.inmobi.databus.consume;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inmobi.databus.DatabusConfig;

public class ConsumerJob implements Tool {

  private Configuration conf;
  public static final String TMP = "/databus/system/tmp";
  public static final String TRASH = "/databus/system/trash";
  public static final String DISTCP_INPUT = "/databus/system/distcp";
  public static final String PUBLISH_DIR = "/databus/data/";

  private static final Log LOG = LogFactory.getLog(ConsumerJob.class);
  private DatabusConfig databusConf = new DatabusConfig();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] arg) throws Exception {
    //LOG.info("JobId " + getConf().get(name))
    Path tmp = new Path(TMP);
    FileSystem fs = FileSystem.get(getConf());
    /*boolean stagingDirExists = FileSystem.get(getConf()).exists(stagingP);
    if (stagingDirExists) {
      System.out.println("Other wf in progress..exiting!");
      return 0;
    }*/
    
    Path inputPath = new Path(tmp, "input");
    
    String consumeDir = arg[0];
    Map<FileStatus, String> fileListing = new HashMap<FileStatus, String>();
    createListing(fs, fs.getFileStatus(new Path(consumeDir)),
        fileListing, new HashSet<String>());
    
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
    
    List<Path> donePaths = getPathsToCreateDone(fileListing.values());
    
    Job job = createJob(inputPath);
    job.waitForCompletion(true);
    if (job.isSuccessful()) {
      
      postProcess(fileListing.values(), donePaths, job.getJobID());
      
      //TODO:delete the source paths
      //This must be the last thing done in this execution
      //trashSrcs(fs, fileListing.keySet());
      
      
      //ParallelDistCp distCp = new ParallelDistCp(databusConf, getConf());
      //distCp.start();
    }

    //TODO: delete the job tmp dir recursively
    fs.delete(getJobTmpDir(job.getJobID()));
    return 0;
  }

  private void postProcess(Collection<String> destDirs, List<Path> donePaths, 
      JobID jobId) 
      throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    
    //create done files and done file input for distcp job
    Map<String, FSDataOutputStream> distcpInputMapping = 
        new HashMap<String, FSDataOutputStream>();
    
    for (Path doneFile : donePaths) {
      LOG.info("Creating done file " + doneFile);
      fs.create(doneFile).close();
      String category = getCategory(doneFile);
      LOG.info("Creating distcp done destCategory " + category);
      Set<String> dests = databusConf.getDestinationStreamMap().get(category);
      for (String dest : dests) {
        FSDataOutputStream out = distcpInputMapping.get(dest);
        if (out == null) {
          out = fs.create(new Path(DISTCP_INPUT + File.separator + 
              dest + File.separator + jobId.toString() + "_done"));
          distcpInputMapping.put(dest, out);
        }
        out.writeBytes(doneFile.toString());
        out.writeBytes("\n");
      }
    }
    //close all streams
    for (FSDataOutputStream out : distcpInputMapping.values()) {
      out.close();
    }
    

    //create the input for distcp jobs
    distcpInputMapping.clear();
    for (String destDir : destDirs) {
      Path destDirPath = new Path(destDir);
      String category = getCategory(destDirPath);
      LOG.info("Creating distcp destCategory " + category);
      Set<String> dests = databusConf.getDestinationStreamMap().get(category);
      for (String dest : dests) {
        FSDataOutputStream out = distcpInputMapping.get(dest);
        if (out == null) {
          out = fs.create(new Path(DISTCP_INPUT + File.separator + 
              dest + File.separator + jobId.toString()));
          distcpInputMapping.put(dest, out);
        }
        out.writeBytes(destDir);
        out.writeBytes("\n");
      }
    }
    //close all streams
    for (FSDataOutputStream out : distcpInputMapping.values()) {
      out.close();
    }

    //TODO:delete the staging dir
  }

  private void trashSrcs(FileSystem fs, Collection<FileStatus> srcs) 
    throws IOException {
    Path trash = new Path(TRASH);
    fs.mkdirs(trash);
    for (FileStatus src : srcs) {
      LOG.info("Deleting src " + src);
      Path target = new Path(TRASH + File.separator + src.toString());
      fs.rename(src.getPath(), target);
    }
  }

  private String getCategory(Path path) {
    String[]  pathSplit = path.toUri().getPath().split("/");
    return pathSplit[3];
  }

  private List<Path> getPathsToCreateDone(Collection<String> destDirs) 
     throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = FileSystem.get(getConf());
    for (String dir : destDirs) {
      Path p = new Path(dir);
      result.add(new Path(p, ".done"));
      doneFiles(fs, p, result, 1);
    }
    return result;
  }

  private void doneFiles(FileSystem fs, Path path, List<Path> result, 
      int level) throws IOException {
    Path p = path.makeQualified(fs);
    if (fs.exists(p)) {
      return;
    }
    LOG.info("listing " + p.getParent());
    FileStatus[] listing = fs.listStatus(p.getParent());
    if (listing != null && listing.length > 0) {
      List<FileStatus> list = Arrays.asList(listing);
      Collections.sort(list, new ModificationTimeComparator());
      FileStatus lastPath = list.get(list.size() - 1);
      Path donePath = new Path(lastPath.getPath(), ".done");
      result.add(donePath);
    } else if (level < 3){
      doneFiles(fs, p.getParent(), result, level++);
    }
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
      } else if ("scribe_stats".equalsIgnoreCase(fileName) ||
          !excludes.contains(fileName)){
        String destDir = getDestDir(fs, fileStatus.getPath());
        results.put(fileStatus, destDir);
      }
    }
  }

  private String getDestDir(FileSystem fs, Path src) 
      throws IOException {
    String category = src.getParent().getParent().getName();
    FileStatus status = fs.getFileStatus(src);
    long time = status.getModificationTime();
    Date date = new Date(time);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);
    String dest = PUBLISH_DIR + File.separator + 
        category + File.separator + 
        calendar.get(Calendar.YEAR) + File.separator +
        (calendar.get(Calendar.MONTH) + 1) + File.separator +
        calendar.get(Calendar.DAY_OF_MONTH) + File.separator +
        calendar.get(Calendar.HOUR_OF_DAY) + File.separator +
        calendar.get(Calendar.MINUTE);
    return dest;
  }

  protected Job createJob(Path inputPath) throws IOException {
    String jobName = "consumer";
    Job job = new Job(getConf());
    job.setJobName(jobName);
    KeyValueTextInputFormat.setInputPaths(job, inputPath);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    
    job.setJarByClass(CopyMapper.class);
    job.setMapperClass(CopyMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(NullOutputFormat.class);
    job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");

    return job;
  }

  public static Path getTaskAttemptTmpDir(TaskAttemptID attemptId) {
    return new Path(getJobTmpDir(attemptId.getJobID()), attemptId.toString());
  }

  public static Path getJobTmpDir(JobID jobId) {
    return new Path(TMP + File.separator + jobId.toString());
  }

  class ModificationTimeComparator implements Comparator<FileStatus> {

    @Override
    public int compare(FileStatus o1, FileStatus o2) {
      return (int) (o1.getModificationTime() - o2.getModificationTime());
    }
    
  }

  public static void main(String[] args) throws Exception {
    ConsumerJob job = new ConsumerJob();
    ToolRunner.run(job, args);
  }
}
