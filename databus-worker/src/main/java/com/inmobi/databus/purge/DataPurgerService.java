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
package com.inmobi.databus.purge;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.utils.CalendarHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/*
 * Assumptions
 * (i) One data Purger Service for a cluster
 */

public class DataPurgerService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(DataPurgerService.class);
  private static long MILISECONDS_PER_DAY = 24 * 60 * 60 * 1000;


  private final Cluster cluster;
  private final FileSystem fs;
  private Map<String, Integer> streamRetention;
  private Set<Path> streamsToPurge;
  DateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm");

  public DataPurgerService(DatabusConfig databusConfig, Cluster cluster)
  throws Exception {
    super(DataPurgerService.class.getName(), databusConfig, 60000 * 60);
    this.cluster = cluster;
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @Override
  public void stop() {
    stopped = true;
    /*
     * Pruger can sleep for an hour so it needs to be interuppted
     */
    thread.interrupt();
    LOG.info(Thread.currentThread().getName() + " stopped [" + stopped + "]");
  }


  @Override
  public long getMSecondsTillNextRun(long currentTime) {
    return runIntervalInMsec;
  }

  private void addMergedStreams() {
    Map<String, DestinationStream> destinationStreamMapStreamMap = cluster
    .getDestinationStreams();
    Set<Map.Entry<String, DestinationStream>> entrySet = destinationStreamMapStreamMap
    .entrySet();
    Iterator it = entrySet.iterator();
    while (it.hasNext()) {
      Map.Entry entry = (Map.Entry) it.next();
      String streamName = (String) entry.getKey();
      DestinationStream consumeStream = (DestinationStream) entry.getValue();
      Integer mergedStreamRetentionInDays = consumeStream.getRetentionInDays();
      LOG.debug("Merged Stream :: streamName [" + streamName
      + "] mergedStreamRetentionInDays [" + mergedStreamRetentionInDays
      + "]");
      if (streamRetention.get(streamName) == null) {
        streamRetention.put(streamName, mergedStreamRetentionInDays);
        LOG.debug("Adding Merged Stream [" + streamName + "] retentionInDays ["
        + mergedStreamRetentionInDays + "]");
      } else {
        // Partial & Merged stream are produced at this cluster
        // choose max retention period
        Integer partialStreamRetentionInDays = streamRetention.get(streamName);
        if (partialStreamRetentionInDays.compareTo(mergedStreamRetentionInDays) > 0) {
          streamRetention.put(streamName, partialStreamRetentionInDays);
          LOG.debug("Overriding Stream [" + streamName + "] retentionInDays ["
          + partialStreamRetentionInDays + "]");

        } else {
          streamRetention.put(streamName, mergedStreamRetentionInDays);
          LOG.debug("Overriding Stream [" + streamName + "] retentionInDays ["
          + mergedStreamRetentionInDays + "]");

        }

      }
    }
  }

  private void addLocalStreams() {
    for (SourceStream s : getConfig().getSourceStreams().values()) {
      if (s.getSourceClusters().contains(cluster)) {
        String streamName = s.getName();
        Integer retentionInDays = new Integer(s.getRetentionInDays(cluster
        .getName()));
        streamRetention.put(streamName, retentionInDays);
        LOG.debug("Adding Partial Stream [" + streamName
        + "] with retentionPeriod [" + retentionInDays + "]");
      }
    }
  }

  private Integer getDefaultStreamRetentionInDays() {
    return new Integer(1);
  }

  private Integer getTrashPathRetentionInDays() {
    return new Integer(1);
  }

  private Integer getRetentionPeriod(String streamName) {
    Integer retentionInDays = streamRetention.get(streamName);
    if (retentionInDays == null)
      return getDefaultStreamRetentionInDays();
    return retentionInDays;
  }

  private long getMsecInDay() {
    return 1000 * 60 * 60 * 24; // 1 day
  }

  @Override
  protected void execute() throws Exception {
    try {
      streamRetention = new HashMap<String, Integer>();
      streamsToPurge = new HashSet<Path>();

      // populates - streamRetention
      // Map of streams and their retention period at this cluster (Partial +
      // Merged)
      // Partial streams produced at this cluster - retention period config
      addLocalStreams();
      // Merged streams at this cluster - retention period config
      addMergedStreams();
      String mergedStreamRoot = cluster.getFinalDestDirRoot();
      Map<String, Path>  mergedStreamsInClusterPathMap = getStreamsInCluster(mergedStreamRoot);
      String localStreamRoot = cluster.getLocalFinalDestDirRoot();
      Map<String, Path> localStreamsInClusterPathMap = getStreamsInCluster
      (localStreamRoot);
      getPathsToPurge(mergedStreamsInClusterPathMap,
      localStreamsInClusterPathMap);
      purge();
    } catch (Exception e) {
      LOG.warn(e);
      e.printStackTrace();
      throw new Exception(e);
    }
  }

  private void getPathsToPurge(Map<String, Path> mergedStreamsInClusterPathMap,
                               Map<String, Path> localStreamsInClusterPathMap) throws Exception {
    getStreamsPathToPurge(mergedStreamsInClusterPathMap);
    getStreamsPathToPurge(localStreamsInClusterPathMap);
    getTrashPathsToPurge();

  }

  private void getTrashPathsToPurge() throws Exception {
    Path trashRoot = cluster.getTrashPath();
    LOG.debug("Looking for trashPaths in [" + trashRoot + "]");
    FileStatus[] trashPaths = fs.listStatus(trashRoot);
    // For each trashpath
    if (trashPaths != null && trashPaths.length > 1) {
      for (FileStatus trashPath : trashPaths) {
        Calendar trashPathDate = getDateFromTrashPath(trashPath.getPath()
        .getName());
        if (isPurge(trashPathDate, getTrashPathRetentionInDays()))
          streamsToPurge.add(trashPath.getPath().makeQualified(fs));
      }
    }

  }

  private Calendar getDateFromTrashPath(String trashPath) {
    // Eg: TrashPath :: 2012-1-9
    String[] date = trashPath.split("-");
    String year = date[0];
    String month = date[1];
    String day = date[2];
    return CalendarHelper.getDate(year, month, day);

  }

  private Map<String, Path> getStreamsInCluster(String root) throws Exception {
    Map<String, Path> streams = new HashMap<String, Path>();
    LOG.debug("Find streams in [" + root + "]");
    FileStatus[] paths = fs.listStatus(new Path(root));
    if (paths != null) {
      for (FileStatus fileStatus : paths) {
        streams.put(fileStatus.getPath().getName(), fileStatus.getPath()
        .makeQualified(fs));
        LOG.debug("Purger working for stream [" + fileStatus.getPath() + "]");
      }
    } else
      LOG.debug("No streams found in [" + root + "]");
    return streams;
  }

  private void getStreamsPathToPurge(Map<String, Path> streamPathMap)
  throws Exception {
    Set<Map.Entry<String, Path>> streamsToProcess = streamPathMap.entrySet();
    Iterator it = streamsToProcess.iterator();
    while (it.hasNext()) {
      Map.Entry<String, Path> entry = (Map.Entry<String, Path>) it.next();
      String streamName = entry.getKey();
      Path streamRootPath = entry.getValue();
      LOG.debug("Find Paths to purge for stream [" + streamName
      + "] streamRootPath [" + streamRootPath + "]");
      // For each Stream, all years
      FileStatus[] years = getAllFilesInDir(streamRootPath, fs);
      if (years != null) {
        for (FileStatus year : years) {
          // For each month
          FileStatus[] months = getAllFilesInDir(year.getPath(), fs);
          if (months != null && months.length >= 1) {
            for (FileStatus month : months) {
              // For each day
              FileStatus[] days = getAllFilesInDir(month.getPath(), fs);
              if (days != null && days.length >= 1) {
                for (FileStatus day : days) {
                  LOG.debug("Working for day [" + day.getPath() + "]");
                  Calendar streamDate = CalendarHelper.getDate(year.getPath()
                  .getName(), month.getPath().getName(), day.getPath()
                  .getName());
                  LOG.debug("Validate [" + streamDate.toString()
                  + "] against retentionDays ["
                  + getRetentionPeriod(streamName) + "]");
                  if (isPurge(streamDate, getRetentionPeriod(streamName))) {
                    LOG.debug("Adding stream to purge [" + day.getPath());
                    streamsToPurge.add(day.getPath().makeQualified(fs));
                  }
                } // each day
              }
              else {
                //No day found in month. Purge month
                streamsToPurge.add(month.getPath().makeQualified(fs));
              }
            }// each month
          }
          else {
            //no months found in year. Purge Year.
            streamsToPurge.add(year.getPath().makeQualified(fs));
          }
        }// each year
      }
    }// each stream
  }

  public boolean isPurge(Calendar streamDate, Integer retentionPeriodinDays) {
    //int streamDay = streamDate.get(Calendar.DAY_OF_MONTH);
    Calendar nowTime = CalendarHelper.getNowTime();
    String streamDateStr = dateFormat.format(new Date(streamDate
    .getTimeInMillis()));
    String nowTimeStr =  dateFormat.format(new Date
    (nowTime.getTimeInMillis()));

    LOG.debug("streamDate [" + streamDateStr +  "] currentDate : [" + nowTimeStr +
    "] against retention [" + retentionPeriodinDays + "] days");

    LOG.debug("Days between streamDate and nowTime is [" +
    getDaysBetweenDates(streamDate, nowTime));
    if (getDaysBetweenDates(streamDate, nowTime) < retentionPeriodinDays )
      return false;
    else
      return true;
  }

  private int getDaysBetweenDates(Calendar startDate, Calendar endDate) {
    long diff = endDate.getTimeInMillis() - startDate.getTimeInMillis();
    int days = (int) Math.floor(diff / MILISECONDS_PER_DAY);
    return Math.abs(days);
  }

  private void purge() throws Exception {
    Iterator it = streamsToPurge.iterator();
    while (it.hasNext()) {
      Path purgePath = (Path) it.next();
      fs.delete(purgePath, true);
      LOG.info("Purging [" + purgePath + "]");
    }
  }

  private FileStatus[] getAllFilesInDir(Path dir, FileSystem fs)
  throws Exception {
    return fs.listStatus(dir);
  }
}
