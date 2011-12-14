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

public class ConsumerJob {


  public static final String DATABUS_ROOT_DIR = "/databus/";
  public static final String DATABUS_SYSTEM_DIR = DATABUS_ROOT_DIR + "system/";
  public static final String TMP = DATABUS_SYSTEM_DIR + "tmp";
  public static final String TRASH = DATABUS_SYSTEM_DIR + "trash";
  public static final String DISTCP_INPUT = DATABUS_SYSTEM_DIR + "distcp";
  public static final String DATA_DIR = DATABUS_ROOT_DIR + "data/";
  public static final String PUBLISH_DIR = DATABUS_ROOT_DIR + "streams/";

  private static final Log LOG = LogFactory.getLog(ConsumerJob.class);
  
  private final Configuration conf;
  private final Path tmpPath;

  ConsumerJob(String wfId) {
    this.conf = new Configuration();
    this.tmpPath = new Path(TMP, wfId);
  }

  public void run() throws Exception {
    Map<FileStatus, String> fileListing = new HashMap<FileStatus, String>();
    List<Path> donePaths = getPathsToCreateDone(fileListing.values());
    Path inputPath = new Path(tmpPath, "input");
    createMRInput(inputPath, fileListing);
    
    Job job = createJob(inputPath);
    job.waitForCompletion(true);
    if (job.isSuccessful()) {
      
      createDoneFiles(donePaths);

      createDistcpInput(fileListing.values(), donePaths, job.getJobID());
      
      //TODO:delete the source paths
      //This must be the last thing done in this execution
      //trashSrcs(fs, fileListing.keySet());
    }

    //TODO: delete the tmp folders recursively
    FileSystem fs = FileSystem.get(conf);
    fs.delete(getJobTmpDir(job.getJobID()));
    fs.delete(tmpPath);
  }

  private void createMRInput(Path inputPath,
      Map<FileStatus, String> fileListing) throws IOException {
    FileSystem fs = FileSystem.get(conf);

    createListing(fs, fs.getFileStatus(new Path(DATA_DIR)),
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
  }

  private void createDoneFiles(List<Path> donePaths) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    for (Path doneFile : donePaths) {
      LOG.info("Creating done file " + doneFile);
      fs.create(doneFile).close();
    }
  }

  private void createDistcpInput(Collection<String> destDirs, 
      List<Path> donePaths, JobID jobId) 
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path inputTmpDir = getDistcpInputTmpDir(jobId);
    
    //create distcp input paths
    Path distpinput = new Path(inputTmpDir, "input");
    FSDataOutputStream out = fs.create(distpinput);
    for (String s : destDirs) {
      out.writeBytes(s);
      out.writeBytes("\n");
    }
    out.close();

    //create done input paths
    Path doneInput = new Path(inputTmpDir, "done");
    out = fs.create(doneInput);
    for (Path s : donePaths) {
      out.writeBytes(s.toString());
      out.writeBytes("\n");
    }
    out.close();
    
    //move the final distcp input
    Path finalDistcpInput = new Path(DISTCP_INPUT);
    fs.mkdirs(finalDistcpInput);
    fs.rename(inputTmpDir, finalDistcpInput);
  }

  private Path getDistcpInputTmpDir(JobID jobId) {
    Calendar calendar = new GregorianCalendar();
    //TODO: use wf start time instead of current time??
    calendar.setTime(new Date(System.currentTimeMillis()));
    String dir = "" + calendar.get(Calendar.YEAR) + "_"
        + (calendar.get(Calendar.MONTH) + 1) + "_"
        + calendar.get(Calendar.DAY_OF_MONTH) + "_"
        + calendar.get(Calendar.HOUR_OF_DAY) + "_"
        + calendar.get(Calendar.MINUTE);
    return new Path(tmpPath, dir);
  }

  /*private void createDistcpInput(Collection<String> destDirs, 
      List<Path> donePaths, JobID jobId) 
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    
    //create done files and done file input for distcp job
    Map<String, FSDataOutputStream> distcpInputMapping = 
        new HashMap<String, FSDataOutputStream>();
    
    for (Path doneFile : donePaths) {
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

  private String getCategory(Path path) {
    String[]  pathSplit = path.toUri().getPath().split("/");
    return pathSplit[3];
  }
  
  */

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



  private List<Path> getPathsToCreateDone(Collection<String> destDirs)
      throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = FileSystem.get(conf);
    for (String dir : destDirs) {
      Path p = new Path(dir);
      LOG.info("Going to create a done file in directory ["
          + p.makeQualified(fs).getName());
      result.add(new Path(p, ".done").makeQualified(fs));
      doneFiles(fs, p, result, 1);
    }
    return result;
  }

  private void doneFiles(FileSystem fs, Path path, List<Path> result, int level)
      throws IOException {
    Path p = path;
    while (level < 5) {
      p = p.makeQualified(fs);
      if (fs.exists(p)) {
        return;
      }
      LOG.info("Checking all level of parents for path [" + p.getName()
          + " to create DONE file");
      LOG.info("listing " + p.getParent());
      FileStatus[] listing = fs.listStatus(p.getParent());
      if (listing != null && listing.length > 0 && level < 3) {
        List<FileStatus> list = Arrays.asList(listing);
        Collections.sort(list, new ModificationTimeComparator());
        FileStatus lastPath = list.get(list.size() - 1);
        Path donePath = new Path(lastPath.getPath(), ".done");
        donePath = donePath.makeQualified(fs);
        LOG.info("adding path to create DONE file [" + donePath + "]");
        result.add(donePath);
      } else {
        // doneFiles(fs, p.getParent(), result, level++);
        p = p.getParent();
        level++;
      }
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
      } else if ("scribe_stats".equalsIgnoreCase(fileName.trim())) {
        excludes.add(fileName);
      } else if (!excludes.contains(fileName)) {
        String destDir = getDestDir(fs, fileStatus.getPath().makeQualified(fs));
        results.put(fileStatus, destDir);
      }
    }
  }

  private String getDestDir(FileSystem fs, Path src) throws IOException {
    String category = src.getParent().getParent().getName();
    FileStatus status = fs.getFileStatus(src);
    long time = status.getModificationTime();
    Date date = new Date(time);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);
    String dest = PUBLISH_DIR + File.separator + category + File.separator
        + calendar.get(Calendar.YEAR) + File.separator
        + (calendar.get(Calendar.MONTH) + 1) + File.separator
        + calendar.get(Calendar.DAY_OF_MONTH) + File.separator
        + calendar.get(Calendar.HOUR_OF_DAY) + File.separator
        + calendar.get(Calendar.MINUTE);
    return dest;
  }

  protected Job createJob(Path inputPath) throws IOException {
    String jobName = "consumer";
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
    ConsumerJob job = new ConsumerJob(args[0]);
    job.run();
  }
}
