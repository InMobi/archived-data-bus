package com.inmobi.databus.datamovement;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 04/12/11
 * Time: 3:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class ScribeDataMover {
    Logger logger = Logger.getLogger(ScribeDataMover.class);
    Constants  constants;
    HdfsOperations hdfsOperations;

    List<String> getCategories() {
        String scribeLogsDir = constants.getLogsParentDir();
        List<String> categoryList = null;
        try {
            categoryList = hdfsOperations.getFilesInDirectory(scribeLogsDir);
            logger.debug("getCategories from [" + scribeLogsDir + "]");
        }
        catch (HDFSException hdfsException) {
            logger.warn("Failed to get categories List for scribeLogsDir" + scribeLogsDir);
        }
        return categoryList;
    }

    void loadHdfsConfiguration() {
        hdfsOperations = new HdfsOperations();
        Configuration configuration = hdfsOperations.getConfiguration();
        String dfsName = constants.getHdfsNameNode();
        configuration.set("fs.default.name", dfsName);
        logger.debug("loadHdfsConfiguration setting fs.default.name to [" + dfsName + "]");


    }
    void loadConstants(String propertyFile) {
        if(propertyFile == null) {
            //load from classpath
            constants = new Constants(null);
            logger.debug("Loading properties from classpath scribe.properties");
        }
        else  {
            constants = new Constants(propertyFile);
            logger.debug("loading properties from [" + propertyFile + "]");
        }
    }

    void moveScribeData(String propertyFile) {
        loadConstants(propertyFile);
        loadHdfsConfiguration();
        List<String> categoryList = getCategories();
        if (categoryList != null && !categoryList.isEmpty())   {
            //loadHdfsConfiguration();   hdfs configuration should be loaded by individual threads.
            //ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
            //1. Schedule a task for each category to execute every minute which moves files across all collectors
            //List<ScheduledFuture<CategoryDataMovementTask>>  scheduledFutureList = new ArrayList<ScheduledFuture<CategoryDataMovementTask>>();
            // for (String category : categoryList) {
            logger.warn("Scheduling a task for all categories   for data movement every minute from ScribeLogsParentDir [" +  constants.getLogsParentDir()  + "]"
            );
            CategoryDataMovementTask task =   new CategoryDataMovementTask(categoryList, constants);
            task.run();
        }
        else {
            logger.warn("No catgeories found in " +  constants.getLogsParentDir() + " Not doing anything..");
        }


    }



    public static void main(String[] args) {
        ScribeDataMover scribeDataMover = new ScribeDataMover();
        //1. Load all scribe related config
        if (args.length <=1 || ( args.length > 1  && args[1] == null)) {
            scribeDataMover.moveScribeData(null);
        }
        else
            scribeDataMover.moveScribeData(args[1]);

    }
}
