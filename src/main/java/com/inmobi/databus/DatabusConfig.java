package com.inmobi.databus;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class DatabusConfig {

  public static String DATABUS_ROOT_DIR = "/databus/";
  public static String DATABUS_SYSTEM_DIR = DATABUS_ROOT_DIR + "system/";
  public static String TMP = DATABUS_SYSTEM_DIR + "tmp";
  public static String TRASH = DATABUS_SYSTEM_DIR + "trash";
  public static String CONSUMER = DATABUS_SYSTEM_DIR + "consumers";
  public static String DATA_DIR = DATABUS_ROOT_DIR + "data/";
  public static String PUBLISH_DIR = DATABUS_ROOT_DIR + "streams/";

  private Map<String, Cluster> clusters;
  private Map<String, Stream> streams;
  private Cluster destinationCluster;
  private Configuration hadoopConf = null;
  private final static Path tmpPath = new Path(TMP);

  public DatabusConfig(String rootDir, Map<String, Stream> streams,
      Map<String, Cluster> clusterMap, Cluster destinationCluster) {

    this.hadoopConf = new Configuration();
    hadoopConf.set("fs.default.name", destinationCluster.getHdfsUrl());
    setRootDir(rootDir);
    setStreams(streams);
    setClusters(clusterMap);
    setDestinationCluster(destinationCluster);
  }

  public void setDestinationCluster(Cluster destinationCluster) {
    this.destinationCluster = destinationCluster;
  }

  public void setStreams(Map<String, Stream> streams) {
    this.streams = streams;
  }

  public void setClusters(Map<String, Cluster> clusters) {
    this.clusters = clusters;
  }

  public void setRootDir(String rootDir) {
    DatabusConfig.DATABUS_ROOT_DIR = rootDir;
  }

  public Configuration getHadoopConf() {
    return this.hadoopConf;
  }

  public Cluster getDestinationCluster() {
    return destinationCluster;
  }

  public Map<String, Cluster> getClusters() {
    return clusters;
  }

  public Map<String, Stream> getStreams() {
    return streams;
  }

  public static Path getTmpPath() {
    return tmpPath;
  }

  public static Path getNewTmpPath() {
    return new Path(tmpPath, Long.toString(System.currentTimeMillis()));
  }

  public Path getTrashPath() {
    return new Path(TRASH);
  }

  public Path getDataDir() {
    return new Path(DATA_DIR);
  }

  public Path getConsumePath(Cluster srcCluster, Cluster consumeCluster) {
    return new Path(srcCluster.hdfsUrl + File.separator + CONSUMER
        + File.separator + consumeCluster.name);
  }

  public static Path getTaskAttemptTmpDir(TaskAttemptID attemptId) {
    return new Path(getJobTmpDir(attemptId.getJobID()), attemptId.toString());
  }

  public static Path getJobTmpDir(JobID jobId) {
    return new Path(tmpPath, jobId.toString());
  }

  public String getFinalDestDir(String category, long commitTime)
      throws IOException {
    Date date = new Date(commitTime);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);
    String dest = this.destinationCluster.hdfsUrl + File.separator
        + PUBLISH_DIR + File.separator + category + File.separator
        + calendar.get(Calendar.YEAR) + File.separator
        + (calendar.get(Calendar.MONTH) + 1) + File.separator
        + calendar.get(Calendar.DAY_OF_MONTH) + File.separator
        + calendar.get(Calendar.HOUR_OF_DAY) + File.separator
        + calendar.get(Calendar.MINUTE);
    return dest;
  }

  public static class Cluster {
    public final String name;
    public final String hdfsUrl;
    public final Map<String, ConsumeStream> consumeStreams;
    
    Cluster(String name, String hdfsUrl, Map<String, 
        ConsumeStream> consumeStreams) {
      this.name = name;
      this.hdfsUrl = hdfsUrl;
      this.consumeStreams = consumeStreams;
    }

    public String getHdfsUrl() {
      return hdfsUrl;
    }

    public String getName() {
      return name;
    }
  }

  public static class ConsumeStream {
    public final int retentionHours;
    public final String name;

    public ConsumeStream(String name, int retentionHours) {
      this.name = name;
      this.retentionHours = retentionHours;
    }
  }

  public static class Stream {
    public final String name;
    public final Set<String> sourceClusters;

    public Stream(String name, Set<String> sourceClusters) {
      super();
      this.name = name;
      this.sourceClusters = sourceClusters;
    }

    public Set<String> getSourceClusters() {
      return sourceClusters;
    }

    public String getName() {
      return name;
    }

   
  }
}
