package com.inmobi.databus.purge;

import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.utils.CalendarHelper;

/*
 * Assumptions
 * (i) One data Purger Service for a cluster
 */

public class DataPurgerService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(DataPurgerService.class);

  private final Cluster cluster;
  private final FileSystem fs;
  private Map<String, Integer> streamRetention;
  private Set<Path> streamsToPurge;
  private Map<String, Path> mergedStreamsInClusterPathMap;
  private Map<String, Path> localStreamsInClusterPathMap;

  public DataPurgerService(DatabusConfig databusConfig, Cluster cluster)
      throws Exception {
    super(DataPurgerService.class.getName(), databusConfig, 60000 * 60 * 24);
    this.cluster = cluster;
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  private void addMergedStreams() {
    Map<String, DestinationStream> consumeStreamMap = cluster
        .getDestinationStreams();
    Set<Map.Entry<String, DestinationStream>> entrySet = consumeStreamMap
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
    return new Integer(2); // retentionperiod is 1 day, setting to 2 to avoid
                           // overlap
  }

  private Integer getTrashPathRetentionInDays() {
    return new Integer(2); // retentionperiod is 1 day, setting to 2 to avoid
                           // overlap
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
      mergedStreamsInClusterPathMap = getStreamsInCluster(mergedStreamRoot);
      String localStreamRoot = cluster.getLocalFinalDestDirRoot();
      localStreamsInClusterPathMap = getStreamsInCluster(localStreamRoot);
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
            }// each month
          }
        }// each year
      }
    }// each stream
  }

  private boolean isPurge(Calendar streamDate, Integer retentionPeriodinDays) {
    streamDate.add(Calendar.DAY_OF_MONTH, retentionPeriodinDays + 1); // 1 to
                                                                      // avoid
                                                                      // last
                                                                      // day
                                                                      // data
    Calendar nowTime = CalendarHelper.getNowTime();
    LOG.info("streamDate ::" + streamDate.getTimeInMillis() + " nowTime ::"
        + nowTime.getTimeInMillis());
    if (streamDate.before(nowTime))
      return true;
    else
      return false;
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
