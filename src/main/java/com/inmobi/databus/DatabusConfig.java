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

public class DatabusConfig {

  /*
    public static String DATABUS_ROOT_DIR = "/databus/";
    public static String DATABUS_SYSTEM_DIR = DATABUS_ROOT_DIR + "system/";
    public static String TMP = DATABUS_SYSTEM_DIR + "tmp";
    public static String TRASH = DATABUS_SYSTEM_DIR + "trash";
    public static String CONSUMER = DATABUS_SYSTEM_DIR + "consumers";
    public static String DATA_DIR = DATABUS_ROOT_DIR + "data/";
    public static String PUBLISH_DIR = DATABUS_ROOT_DIR + "streams/";
  */
  private final Map<String, Cluster> clusters;
  private final Map<String, Stream> streams;

  public DatabusConfig(String rootDir, Map<String, Stream> streams,
                       Map<String, Cluster> clusterMap) {
    //this.hadoopConf = new Configuration();
    //hadoopConf.set("fs.default.name", destinationCluster.getHdfsUrl());
    this.streams = streams;
    this.clusters = clusterMap;
  }

  public Map<String, Cluster> getClusters() {
    return clusters;
  }

  public Map<String, Stream> getStreams() {
    return streams;
  }

  public static class Cluster {
    private final String name;
    private final String rootDir;
    private final String hdfsUrl;
    private final Map<String, ConsumeStream> consumeStreams;
    private final Set<String> sourceStreams;
    private final Configuration hadoopConf;
    // first time starting time
    private long lastCommitTime = System.currentTimeMillis();

    Cluster(String name, String rootDir,
            String hdfsUrl, String jtUrl, Map<String,
            ConsumeStream> consumeStreams, Set<String> sourceStreams) {
      this.name = name;
      this.hdfsUrl = hdfsUrl;
      this.rootDir = rootDir;
      this.hadoopConf = new Configuration();
      this.hadoopConf.set("mapred.job.tracker", jtUrl);
      this.hadoopConf.set("databus.tmp.path", getTmpPath().toString());
      this.consumeStreams = consumeStreams;
      this.sourceStreams = sourceStreams;
      this.hadoopConf.set("fs.default.name", hdfsUrl);

    }

    public synchronized long getCommitTime() {
      long current = System.currentTimeMillis();
      if (current - lastCommitTime >= 60000) {
        lastCommitTime = current;
      }
      return lastCommitTime;
    }

    public String getHdfsUrl() {
      return hdfsUrl;
    }

    public Configuration getHadoopConf() {
      return hadoopConf;
    }

    public String getName() {
      return name;
    }

    public String getFinalDestDirRoot() {
      String dest = hdfsUrl + File.separator + rootDir + File.separator + "streams"
              + File.separator;
      return dest;
    }

    public String getDateTimeDestDir(String category, long commitTime) {
      Date date = new Date(commitTime);
      Calendar calendar = new GregorianCalendar();
      calendar.setTime(date);
      String dest = category + File.separator
              + calendar.get(Calendar.YEAR) + File.separator
              + (calendar.get(Calendar.MONTH) + 1) + File.separator
              + calendar.get(Calendar.DAY_OF_MONTH) + File.separator
              + calendar.get(Calendar.HOUR_OF_DAY) + File.separator
              + calendar.get(Calendar.MINUTE);
      return dest;
    }


    public String getFinalDestDir(String category, long commitTime)
            throws IOException {
      Date date = new Date(commitTime);
      Calendar calendar = new GregorianCalendar();
      calendar.setTime(date);
      String dest = hdfsUrl + File.separator +
              rootDir + File.separator + "streams"
              + File.separator + category + File.separator
              + calendar.get(Calendar.YEAR) + File.separator
              + (calendar.get(Calendar.MONTH) + 1) + File.separator
              + calendar.get(Calendar.DAY_OF_MONTH) + File.separator
              + calendar.get(Calendar.HOUR_OF_DAY) + File.separator
              + calendar.get(Calendar.MINUTE);
      return dest;
    }

    public String getFinalDestDirTillHour(String category, long commitTime)
            throws IOException {
      Date date = new Date(commitTime);
      Calendar calendar = new GregorianCalendar();
      calendar.setTime(date);
      String dest = hdfsUrl + File.separator +
              rootDir + File.separator + "streams"
              + File.separator + category + File.separator
              + calendar.get(Calendar.YEAR) + File.separator
              + (calendar.get(Calendar.MONTH) + 1) + File.separator
              + calendar.get(Calendar.DAY_OF_MONTH) + File.separator
              + calendar.get(Calendar.HOUR_OF_DAY) + File.separator;

      return dest;
    }


    public Map<String, ConsumeStream> getConsumeStreams() {
      return consumeStreams;
    }

    public Set<String> getSourceStreams() {
      return sourceStreams;
    }

    public Path getTrashPath() {
      return new Path(getSystemDir() + File.separator +
              "trash");
    }

    public Path getDataDir() {
      return new Path(hdfsUrl + File.separator +
              rootDir + File.separator +
              "data");
    }

    public Path getConsumePath(Cluster consumeCluster) {
      return new Path(getSystemDir()
              + File.separator + "consumers" + File.separator +
              consumeCluster.name);
    }

    public Path getTmpPath() {
      return new Path(getSystemDir() + File.separator +
              "tmp");
    }
    /*
        public Path getNewTmpPath() {
          return new Path(getTmpPath(),
              Long.toString(System.currentTimeMillis()));
        }
    */
    private String getSystemDir() {
      return hdfsUrl + File.separator +
              rootDir + File.separator +
              "system";
    }
  }

  public static class ConsumeStream {
    private final int retentionHours;
    private final String name;

    public ConsumeStream(String name, int retentionHours) {
      this.name = name;
      this.retentionHours = retentionHours;
    }

    public String getName() {
      return name;
    }
    public int getRentionHours() {
      return retentionHours;
    }
  }

  public static class Stream {
    private final String name;
    private final Set<String> sourceClusters;

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
