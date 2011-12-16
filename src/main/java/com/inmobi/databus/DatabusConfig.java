package com.inmobi.databus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DatabusConfig {

  public static String DATABUS_ROOT_DIR = "/databus/";
  public static String DATABUS_SYSTEM_DIR = DATABUS_ROOT_DIR + "system/";
  public static String TMP = DATABUS_SYSTEM_DIR + "tmp";
  public static String TRASH = DATABUS_SYSTEM_DIR + "trash";
  public static String CONSUMER = DATABUS_SYSTEM_DIR + "consumers";
  public static String DATA_DIR = DATABUS_ROOT_DIR + "data/";
  public static String PUBLISH_DIR = DATABUS_ROOT_DIR + "streams/";

  private static final String CONFIG = "databus.xml";
  private Map<String, Cluster> clusters = new HashMap<String, Cluster>();
  private Map<String, Stream> streams = new HashMap<String, Stream>();
  private final Cluster destinationCluster;
  private final String wfId;
  private final Configuration hadoopConf;
  private final static Path tmpPath = new Path(TMP);

  public DatabusConfig() {
    this("uj1", "1");
  }

  public DatabusConfig(String destCluster, String wfId) {
    // load configuration

    Cluster uj1 = new Cluster("uj1", "hdfs://localhost:8020", new HashSet<ReplicatedStream>());
    clusters.put(uj1.name, uj1);
    
    Stream beacon = new Stream("beacon", new HashSet<String>());
    beacon.sourceClusters.add(uj1.name);
    streams.put(beacon.name, beacon);

    this.destinationCluster = clusters.get("uj1");
    this.hadoopConf = new Configuration();
    this.wfId = wfId;
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

  public Path getConsumePath(Cluster srcCluster) {
    return new Path(srcCluster.hdfsUrl + File.separator + CONSUMER);
  }

  public static Path getTaskAttemptTmpDir(TaskAttemptID attemptId) {
    return new Path(getJobTmpDir(attemptId.getJobID()), attemptId.toString());
  }

  public static Path getJobTmpDir(JobID jobId) {
    return new Path(tmpPath, jobId.toString());
  }

  public String getDestDir(String category) throws IOException {
    Date date = new Date(System.currentTimeMillis());
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
    public final Set<ReplicatedStream> replicatedStreams;

    Cluster(String name, String hdfsUrl, Set<ReplicatedStream> replicatedStreams) {
      this.name = name;
      this.hdfsUrl = hdfsUrl;
      this.replicatedStreams = replicatedStreams;
    }

      public String getName() {
          return name;
      }
  }

  public static class ReplicatedStream extends Stream {
    public final int retentionHours;
    //public final String offset;

    public ReplicatedStream(String name, Set<String> sourceClusters,
        int retentionHours) {
      super(name, sourceClusters);
      this.retentionHours = retentionHours;
    }
  }

  public static class Stream {
    public final String name;

    public Set<String> getSourceClusters() {
      return sourceClusters;
    }

    public String getName() {
      return name;
    }

    public final Set<String> sourceClusters;

    public Stream(String name, Set<String> sourceClusters) {
      super();
      this.name = name;
      this.sourceClusters = sourceClusters;
    }
  }
}
