package com.inmobi.databus;

import com.inmobi.databus.utils.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;

public class DatabusConfig {


  private final Map<String, Cluster> clusters;
  private final Map<String, Stream> streams;
  private String zkConnectionString;

  public DatabusConfig(String rootDir, String zkConnectionString, Map<String, Stream> streams,
                       Map<String, Cluster> clusterMap) {
    this.zkConnectionString = zkConnectionString;
    this.streams = streams;
    this.clusters = clusterMap;
  }

  public String getZkConnectionString() {
    return zkConnectionString;
  }

  public Map<String, Cluster> getClusters() {
    return clusters;
  }

  public Map<String, Stream> getStreams() {
    return streams;
  }

  public static class Cluster {
    private final String zkConnectionString;
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
            ConsumeStream> consumeStreams, Set<String> sourceStreams, String zkConnectionString) {
      this.name = name;
      this.hdfsUrl = hdfsUrl;
      this.rootDir = rootDir;
      this.hadoopConf = new Configuration();
      this.hadoopConf.set("mapred.job.tracker", jtUrl);
      this.hadoopConf.set("databus.tmp.path", getTmpPath().toString());
      this.consumeStreams = consumeStreams;
      this.sourceStreams = sourceStreams;
      this.hadoopConf.set("fs.default.name", hdfsUrl);
      this.zkConnectionString = zkConnectionString;
    }

    public synchronized long getCommitTime() {
      long current = System.currentTimeMillis();
      if (current - lastCommitTime >= 60000) {
        lastCommitTime = current;
      }
      return lastCommitTime;
    }

    public String getZkConnectionString() {
      return zkConnectionString;
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

    public String getLocalFinalDestDirRoot() {
      String dest = hdfsUrl + File.separator + rootDir + File.separator + "streams-local"
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


    public String getLocalDestDir(String category, long commitTime)
        throws IOException {
      Date date = new Date(commitTime);
      Calendar calendar = new GregorianCalendar();
      calendar.setTime(date);
      String dest = hdfsUrl + File.separator + rootDir + File.separator
          + "streams-local" + File.separator + category + File.separator
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

    public Path getTrashPathWithDate() {
      return new Path(getTrashPath(), CalendarHelper.getCurrentDateAsString());
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

    private String getSystemDir() {
      return hdfsUrl + File.separator +
              rootDir + File.separator +
              "system";
    }
  }

  public static class ConsumeStream {
    private final int retentionInDays;
    private final String name;

    public ConsumeStream(String name, int retentionInDays) {
      this.name = name;
      this.retentionInDays = retentionInDays;
    }

    public String getName() {
      return name;
    }
    public int getRentionInDays() {
      return retentionInDays;
    }
  }

  public static class Stream {
    private final String name;
    //Map of SourceClusterName, Retention for stream on it.
    private Map<String, Integer> sourceClusters;
    private String primaryDestClusterName = null;
    private Integer retentionOnPrimaryDestCluster = null;
    //Map of DestClusterName, Retention for stream on it.
    private Map<String, Integer> mirrorClusters = null;


    public Stream(String name, Map<String, Integer> sourceClusters,
                  String primaryDestClusterName, Integer retentionOnPrimaryDestCluster,
                  Map<String, Integer> mirrorClusters) {
      super();
      this.name = name;
      this.sourceClusters = sourceClusters;
      this.primaryDestClusterName = primaryDestClusterName;
      this.retentionOnPrimaryDestCluster = retentionOnPrimaryDestCluster;
      this.mirrorClusters = mirrorClusters;
    }

    public boolean isPrimaryCluster(String clusterName) {
      return primaryDestClusterName.equalsIgnoreCase(clusterName);
    }

    public String getPrimaryDestClusterName() {
      return primaryDestClusterName;
    }

    public boolean isMirrorDestCluster(String clusterName) {
      return mirrorClusters.containsKey(clusterName);
    }

    public int getDefaultRetention(){
      return 2;
    }

    public int getretentionInDays(String clusterName) {
      if (isPrimaryCluster(clusterName))
      return sourceClusters.get(clusterName).intValue();
      else if (isMirrorDestCluster(clusterName))
        return mirrorClusters.get(clusterName).intValue();
      else
        return getDefaultRetention();
    }

    public Set<String> getMirrorClusters() {
      return mirrorClusters.keySet();
    }

    public Set<String> getSourceClusters() {
      return sourceClusters.keySet();
    }

    public String getName() {
      return name;
    }


  }

}
