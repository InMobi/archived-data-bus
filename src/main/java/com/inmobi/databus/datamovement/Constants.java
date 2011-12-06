package com.inmobi.databus.datamovement;


import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 04/12/11
 * Time: 3:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class Constants {
    Logger logger = Logger.getLogger(CalendarHelper.class);
    String logsParentDir   = "com.inmobi.databus.scribe.logs.parentdir";
    String hdfsNameNode   = "com.inmobi.databus.hdfs.namenode";
    String scribeDataParentDir = "com.inmobi.databus.scribe.data.parentdir";
    String scribeHouseKeepingThresholdInMinutes = "com.inmobi.databus.scribe.housekeeping.threshold.minutes";
    String scribeStatsFileName = "scribe_stats";
    String scribeCurrentFileSuffix = "_current";
    String doneFileName = "com.inmobi.databus.scribe.data.donefilename";

    private void setDoneFileName(String doneFileName) {
        this.doneFileName = doneFileName;
    }

    public String getDoneFileName() {
        return doneFileName.trim();
    }


    public String getScribeCurrentFileSuffix() {
        logger.debug("getScribeCurrentFileSuffix [" + scribeCurrentFileSuffix + "]");
        return scribeCurrentFileSuffix.trim();
    }

    public String getScribeStatsFileName() {
        logger.debug("getScribeStatsFileName [" + scribeStatsFileName + "]" );
        return scribeStatsFileName.trim();
    }

    PropertiesReader propertiesReader;

    public Constants(String propertyFile) {
        propertiesReader = new PropertiesReader();
        //Load fromt the classpath
        Properties properties = propertiesReader.loadScribeProperties(null == propertyFile ? null : propertyFile);
        setLogsParentDir(properties.getProperty(logsParentDir));
        setHdfsNameNode(properties.getProperty(hdfsNameNode));
        setScribeDataParentDir(properties.getProperty(scribeDataParentDir));
        setScribeHouseKeepingThresholdInMinutes(properties.getProperty(scribeHouseKeepingThresholdInMinutes));
        setDoneFileName(properties.getProperty(doneFileName));
    }

    public String getHdfsNameNode() {
        logger.debug("getHdfsNameNode [" + hdfsNameNode + "]");
        return hdfsNameNode.trim();
    }

    void setHdfsNameNode(String hdfsNameNode) {
        this.hdfsNameNode = hdfsNameNode;
    }

    public String getScribeDataParentDir() {
        logger.debug("getScribeDataParentDir [" + scribeDataParentDir + "]");
        return scribeDataParentDir.trim();
    }

    void setScribeDataParentDir(String scribeDataParentDir) {
        this.scribeDataParentDir = scribeDataParentDir;
    }

    public String getScribeHouseKeepingThresholdInMinutes() {
        logger.debug("getScribeHouseKeepingThresholdInMinutes [" + scribeHouseKeepingThresholdInMinutes + "]");
        return scribeHouseKeepingThresholdInMinutes.trim();
    }

    void setScribeHouseKeepingThresholdInMinutes(String scribeHouseKeepingThresholdInMinutes) {
        this.scribeHouseKeepingThresholdInMinutes = scribeHouseKeepingThresholdInMinutes;
    }

    public String getLogsParentDir() {
        logger.debug(" getLogsParentDir [" + logsParentDir + "]");
        return logsParentDir.trim();
    }

    void setLogsParentDir(String logsParentDir) {
        this.logsParentDir = logsParentDir;
    }




}
