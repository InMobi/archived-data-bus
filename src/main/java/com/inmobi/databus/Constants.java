package com.inmobi.databus;


import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 04/12/11
 * Time: 3:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class Constants {
    String logsParentDir   = "com.inmobi.databus.scribe.logs.parentdir";
    String hdfsNameNode   = "com.inmobi.databus.hdfs.namenode";
    String scribeDataParentDir = "com.inmobi.databus.scribe.data.parentdir";
    String scribeHouseKeepingThresholdInMinutes = "com.inmobi.databus.scribe.housekeeping.threshold.minutes";
    String scribeStatsFileName = "scribe_stats";
    String scribeCurrentFileSuffix = "_current";

    public String getScribeCurrentFileSuffix() {
        return scribeCurrentFileSuffix;
    }

    public String getScribeStatsFileName() {
        return scribeStatsFileName;
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
    }

    public String getHdfsNameNode() {
        return hdfsNameNode;
    }

    void setHdfsNameNode(String hdfsNameNode) {
        this.hdfsNameNode = hdfsNameNode;
    }

    public String getScribeDataParentDir() {
        return scribeDataParentDir;
    }

    void setScribeDataParentDir(String scribeDataParentDir) {
        this.scribeDataParentDir = scribeDataParentDir;
    }

    public String getScribeHouseKeepingThresholdInMinutes() {
        return scribeHouseKeepingThresholdInMinutes;
    }

    void setScribeHouseKeepingThresholdInMinutes(String scribeHouseKeepingThresholdInMinutes) {
        this.scribeHouseKeepingThresholdInMinutes = scribeHouseKeepingThresholdInMinutes;
    }

    public String getLogsParentDir() {
        return logsParentDir;
    }

    void setLogsParentDir(String logsParentDir) {
        this.logsParentDir = logsParentDir;
    }




}
