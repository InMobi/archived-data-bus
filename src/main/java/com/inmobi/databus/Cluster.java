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

import com.inmobi.databus.utils.CalendarHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Cluster {
  private final String name;

  private final String rootDir;
  private final String hdfsUrl;
  private final Map<String, DestinationStream> consumeStreams;
  private final Set<String> sourceStreams;
  private final Configuration hadoopConf;
  // first time starting time
  private long lastCommitTime = System.currentTimeMillis();

  Cluster(String name, String rootDir,
          String hdfsUrl, String jtUrl, Map<String,
          DestinationStream> consumeStreams, Set<String> sourceStreams) {
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


  public String getRootDir() {
    return hdfsUrl + File.separator + rootDir + File.separator;
  }

  public String getLocalFinalDestDirRoot() {
    String dest = hdfsUrl + File.separator + rootDir + File.separator + "streams_local"
            + File.separator;
    return dest;
  }

  public String getLocalDestDir(String category, long commitTime)
          throws IOException {
    Date date = new Date(commitTime);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);
    String dest = hdfsUrl + File.separator + rootDir + File.separator
            + "streams_local" + File.separator + category + File.separator
            + calendar.get(Calendar.YEAR) + File.separator
            + (calendar.get(Calendar.MONTH) + 1) + File.separator
            + calendar.get(Calendar.DAY_OF_MONTH) + File.separator
            + calendar.get(Calendar.HOUR_OF_DAY) + File.separator
            + calendar.get(Calendar.MINUTE);
    return dest;
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

  public String getUnqaulifiedFinalDestDirRoot() {
    String dest = File.separator + rootDir + File.separator + "streams" + File.separator;
    return dest;
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


  public Map<String, DestinationStream> getDestinationStreams() {
    return consumeStreams;
  }

  public Set<String> getMirroredStreams() {
    Set<String> mirroredStreams = new HashSet<String>();
    for(DestinationStream consumeStream : getDestinationStreams().values()) {
      if (!consumeStream.isPrimary())
        mirroredStreams.add(consumeStream.getName());
    }
    return mirroredStreams;
  }

  public Set<String> getPrimaryDestinationStreams() {
    Set<String> primaryStreams = new HashSet<String>();
    for(DestinationStream consumeStream : getDestinationStreams().values()) {
      if (consumeStream.isPrimary())
        primaryStreams.add(consumeStream.getName());
    }
    return primaryStreams;

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

  public Path getMirrorConsumePath(Cluster consumeCluster) {
    return new Path(getSystemDir()
            + File.separator + "mirrors" + File.separator +
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
