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
package com.inmobi.databus;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.utils.CalendarHelper;

public class Cluster {
  private final String rootDir;
  private final String hdfsUrl;
  private final String clustername;
  private final String clusterjobqueuename;
  private final Configuration hadoopConf = new Configuration();

  private final Set<String> sourceStreams = new HashSet<String>();
  private final Set<String> destinationStreams = new HashSet<String>();

  public Cluster(Map<String, String> clusterConfiguration)
      throws Exception {
    this.rootDir = clusterConfiguration.get(DatabusConfigParser.ROOTDIR);
    this.clustername = clusterConfiguration.get(DatabusConfigParser.NAME);
    Validate(clustername, "Cluster Name");
    this.hdfsUrl = clusterConfiguration.get(DatabusConfigParser.HDFS_URL);
    Validate(hdfsUrl, "In Cluster " + clustername + " hdfsUrl");
    this.clusterjobqueuename = clusterConfiguration
        .get(DatabusConfigParser.JOB_QUEUE_NAME);
    Validate(clusterjobqueuename, "In Cluster " + clustername
        + " clusterjobqueuename");
    String jtUrl = clusterConfiguration.get(DatabusConfigParser.JT_URL);
    Validate(clusterjobqueuename, "In Cluster " + clustername + " jtUrl");

    this.hadoopConf.set("mapred.job.tracker",jtUrl);
    this.hadoopConf.set("databus.tmp.path", getTmpPath().toString());
    this.hadoopConf.set("fs.default.name", hdfsUrl);
    this.hadoopConf.set("mapred.job.queue.name", clusterjobqueuename);
  }

  private void Validate(String element, String objType) throws ParseException {
    if (element == null)
      throw new ParseException(objType
          + " element not found in cluster configuration", 0);
  }

  public String getRootDir() {
    return hdfsUrl + File.separator + rootDir + File.separator;
  }

  public String getLocalFinalDestDirRoot() {
    String dest = getRootDir() + "streams_local" + File.separator;
    return dest;
  }

  public String getLocalDestDir(String category, long commitTime)
      throws IOException {
    String dest = getLocalFinalDestDirRoot() + category + File.separator
        + CalendarHelper.getDateAsYYYYMMDDHHMNPath(commitTime);
    return dest;
  }

  public String getLocalDestDir(String category, Date date) throws IOException {
    String dest = getLocalFinalDestDirRoot() + category + File.separator
        + CalendarHelper.getDateAsYYYYMMDDHHMNPath(date);
    return dest;
  }

  public synchronized long getCommitTime() {
    return (long) System.currentTimeMillis();
  }

  public String getHdfsUrl() {
    return hdfsUrl;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public String getName() {
    return clustername;
  }

  public String getUnqaulifiedFinalDestDirRoot() {
    Path absolutePath = new Path(hdfsUrl);
    String dest = File.separator + absolutePath.toUri().getPath() + rootDir
        + File.separator + "streams"
        + File.separator;
    return dest;
  }

  public String getFinalDestDirRoot() {
    String dest = getRootDir() + "streams" + File.separator;
    return dest;
  }

  public String getDateTimeDestDir(String category, long commitTime) {
    String dest = category + File.separator
        + CalendarHelper.getDateAsYYYYMMDDHHMNPath(commitTime);
    return dest;
  }

  public String getFinalDestDir(String category, long commitTime)
      throws IOException {
    String dest = getFinalDestDirRoot() + category + File.separator
        + CalendarHelper.getDateAsYYYYMMDDHHMNPath(commitTime);
    return dest;
  }

  public String getFinalDestDirTillHour(String category, long commitTime)
      throws IOException {
    String dest = getFinalDestDirRoot() + category + File.separator
        + CalendarHelper.getDateAsYYYYMMDDHHPath(commitTime);
    return dest;
  }

  public Path getTrashPath() {
    return new Path(getSystemDir() + File.separator + "trash");
  }

  public Path getTrashPathWithDate() {
    return new Path(getTrashPath(), CalendarHelper.getCurrentDateAsString());
  }
  
  public Path getTrashPathWithDateHour() {
    return new Path(getTrashPath() + File.separator
        + CalendarHelper.getCurrentDateAsString() + File.separator
        + CalendarHelper.getCurrentHour());
  }

  public Path getDataDir() {
    return new Path(getRootDir() + "data");
  }

  public Path getConsumePath(Cluster cluster) {
    return new Path(getSystemDir() + File.separator + "consumers"
        + File.separator + cluster.getName());
  }

  public Path getMirrorConsumePath(Cluster cluster) {
    return new Path(getSystemDir() + File.separator + "mirrors"
        + File.separator + cluster.getName());
  }

  public Path getTmpPath() {
    return new Path(getSystemDir() + File.separator + "tmp");
  }

  public String getCheckpointDir() {
    return getSystemDir() + File.separator + "checkpoint";
  }

  private String getSystemDir() {
    return getRootDir() + "system";
  }

  public String getJobQueueName() {
    return clusterjobqueuename;
  }

  public void addSourceStream(String streamName) {
    sourceStreams.add(streamName);
  }

  public void addDestinationStream(String streamName) {
    destinationStreams.add(streamName);
  }

  public Set<String> getSourceStreams() {
    return sourceStreams;
  }

  public Set<String> getDestinationStreams() {
    return destinationStreams;
  }
}
