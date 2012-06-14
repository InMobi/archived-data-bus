package com.inmobi.databus;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.databus.utils.CalendarHelper;

public class TestCluster {

  private static Logger LOG = Logger.getLogger(TestCluster.class);

  @Test
  public void testBasicCluster() throws Exception {
    DatabusConfigParser databusConfigParser = new DatabusConfigParser(
        "test-databus.xml");

    DatabusConfig config = databusConfigParser.getConfig();

    Map<String, Cluster> clusterMap = config.getAllClusters();

    for (Cluster cluster : clusterMap.values()) {

      String hdfsUrl = cluster.getHdfsUrl();
      String rootDir = config.getDefaults().get("rootdir");
      String LocalFinalDestDir = cluster.getRootDir() + "streams_local"
          + File.separator;

      LOG.info("Testing RootDir " + hdfsUrl + File.separator + rootDir
          + File.separator + " " + cluster.getRootDir());
      Assert.assertTrue(cluster.getRootDir().compareTo(
          hdfsUrl + File.separator + rootDir + File.separator) == 0);

      LOG.info("Testing LocalFinalDesDir " + LocalFinalDestDir + " "
          + cluster.getLocalFinalDestDirRoot());
      Assert.assertTrue(cluster.getLocalFinalDestDirRoot().compareTo(
          LocalFinalDestDir) == 0);

      Calendar startTime = new GregorianCalendar();
      startTime.add(Calendar.YEAR, -1);
      startTime.add(Calendar.MONTH, -4);
      startTime.add(Calendar.DAY_OF_MONTH, -2);
      startTime.add(Calendar.HOUR_OF_DAY, -6);
      startTime.add(Calendar.MINUTE, -19);

      DateFormat clusterdateHourMinuteFormat = new SimpleDateFormat("yyyy"
          + File.separator + "MM" + File.separator + "dd" + File.separator
          + "HH" + File.separator + "mm" + File.separator);
      String formattedDate = clusterdateHourMinuteFormat.format(startTime
          .getTime());

      LOG.info("Testing getDateAsYYYYMMDDHHMNPath with long " + formattedDate
          + " "
          + CalendarHelper.getDateAsYYYYMMDDHHMNPath(startTime
              .getTimeInMillis()));
      Assert.assertTrue(CalendarHelper.getDateAsYYYYMMDDHHMNPath(
          startTime.getTimeInMillis()).compareTo(formattedDate) == 0);

      LOG.info("Testing getDateAsYYYYMMDDHHMNPath with Date " + formattedDate
          + " " + CalendarHelper.getDateAsYYYYMMDDHHMNPath(startTime.getTime()));
      Assert.assertTrue(CalendarHelper.getDateAsYYYYMMDDHHMNPath(
          startTime.getTime())
          .compareTo(formattedDate) == 0);

      DateFormat clusterdateHourFormat = new SimpleDateFormat("yyyy"
          + File.separator + "MM" + File.separator + "dd" + File.separator
          + "HH" + File.separator);
      String HourformattedDate = clusterdateHourFormat.format(startTime
          .getTime());

      LOG.info("Testing getLocalDestDir with long " + LocalFinalDestDir
          + "dummy" + File.separator + formattedDate + " "
          + cluster.getLocalDestDir("dummy", startTime.getTimeInMillis()));
      Assert.assertTrue(cluster.getLocalDestDir("dummy",
          startTime.getTimeInMillis()).compareTo(
          LocalFinalDestDir + "dummy" + File.separator + formattedDate) == 0);

      LOG.info("Testing getLocalDestDir with date " + LocalFinalDestDir
          + "dummy" + File.separator + formattedDate + " "
          + cluster.getLocalDestDir("dummy", startTime.getTime()));
      Assert
          .assertTrue(cluster.getLocalDestDir("dummy", startTime.getTime())
              .compareTo(
                  LocalFinalDestDir + "dummy" + File.separator + formattedDate) == 0);

      LOG.info("Testing getUnqaulifiedFinalDestDirRoot " + File.separator
          + rootDir + File.separator + "streams" + File.separator
          + " "
          + cluster.getUnqaulifiedFinalDestDirRoot());

      Assert.assertTrue(cluster.getUnqaulifiedFinalDestDirRoot().compareTo(
          File.separator + rootDir + File.separator + "streams"
              + File.separator) == 0);

      String FinalDestDir = cluster.getRootDir() + "streams" + File.separator;

      LOG.info("Testing getFinalDestDirRoot " + FinalDestDir + " "
          + cluster.getFinalDestDirRoot());
      Assert
          .assertTrue(cluster.getFinalDestDirRoot().compareTo(FinalDestDir) == 0);

      LOG.info("Testing getDateTimeDestDir " + "dummy2" + File.separator
          + formattedDate + " "
          + cluster.getDateTimeDestDir("dummy2", startTime.getTimeInMillis()));
      Assert.assertTrue(cluster.getDateTimeDestDir("dummy2",
          startTime.getTimeInMillis()).compareTo(
          "dummy2" + File.separator + formattedDate) == 0);

      LOG.info("Testing getFinalDestDir " + FinalDestDir + "dummy2"
          + File.separator + formattedDate + " "
          + cluster.getFinalDestDir("dummy2", startTime.getTimeInMillis()));
      Assert.assertTrue(cluster.getFinalDestDir("dummy2",
          startTime.getTimeInMillis()).compareTo( FinalDestDir +
          "dummy2" + File.separator + formattedDate) == 0);

      LOG.info("Testing getFinalDestDirTillHour " + FinalDestDir + "dummy2"
          + File.separator
          + HourformattedDate
          + " "
          + cluster.getFinalDestDirTillHour("dummy2",
              startTime.getTimeInMillis()));
      Assert.assertTrue(cluster.getFinalDestDirTillHour("dummy2",
          startTime.getTimeInMillis()).compareTo( FinalDestDir +
          "dummy2" + File.separator + HourformattedDate) == 0);

      String SystemDir = cluster.getRootDir() + "system";
      String trashPath = SystemDir + File.separator + "trash";

      LOG.info("Testing getTrashPath " + trashPath + " "
          + cluster.getTrashPath());
      Assert
          .assertTrue(cluster.getTrashPath().compareTo(new Path(trashPath)) == 0);
      
      LOG.info("Testing getDataDir " + cluster.getRootDir() + "data" + " "
          + cluster.getDataDir());
      Assert.assertTrue(cluster.getDataDir().compareTo(
          new Path(cluster.getRootDir(), "data")) == 0);
      
      LOG.info("Testing getCheckpointDir " + SystemDir + File.separator
          + "checkpoint" + " " + cluster.getCheckpointDir());
      Assert.assertTrue(cluster.getCheckpointDir().compareTo(
          SystemDir + File.separator + "checkpoint") == 0);

      LOG.info("Testing getTmpPath " + SystemDir + "tmp" + " "
          + cluster.getTmpPath());
      Assert.assertTrue(cluster.getTmpPath()
.compareTo(
          new Path(SystemDir, "tmp")) == 0);
    }

  }

}
