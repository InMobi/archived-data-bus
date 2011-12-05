package com.inmobi.databus;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

import java.util.*;

import java.text.StringCharacterIterator;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 05/12/11
 * Time: 11:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class CategoryDataMovementTask implements  Runnable{
    Logger logger = Logger.getLogger(CategoryDataMovementTask.class);

    String categoryName;
    Constants constants;
    HdfsOperations hdfsOperations;

    public CategoryDataMovementTask(String categoryName, Constants constants) {
        this.categoryName = categoryName;
        this.constants = constants;
    }

    HdfsOperations getHdfsOperationObject() {
        HdfsOperations hdfsOperations = new HdfsOperations();
        Configuration configuration = hdfsOperations.getConfiguration();
        String dfsName = constants.getHdfsNameNode();
        configuration.set("fs.default.name", dfsName);
        return  hdfsOperations;
    }

    private String getScribeLogsHdfsPathTillCategory(String categoryName) {
        String categoryPath =  constants.getLogsParentDir() + "/" + categoryName.trim();
        return categoryPath;

    }

    private String getScribeLogsHdfsPathTillCollector(String categoryName, String collectorName) {
        String collectorPath =  constants.getLogsParentDir() + "/" + categoryName.trim() + "/" + collectorName.trim();
        return collectorPath;
    }

    private String getScribeLogsHdfsPathTillDataFile(String categoryName, String collectorName, String dataFile) {
        String dataFilePath =  constants.getLogsParentDir() + "/" + categoryName.trim() + "/" + collectorName.trim() + "/" + dataFile.trim();
        return dataFilePath;
    }

    private String getScribeLogsHdfsPathTillDataFile(String collectorFullPath, String dataFile) {
        String dataFilePath =  collectorFullPath.trim() + "/" + dataFile.trim();
        return dataFilePath;
    }

    private boolean isScribeStatsFile(String relativeFilenameToCollector) {
        if (relativeFilenameToCollector == null)
            return false;
        return constants.getScribeStatsFileName().equalsIgnoreCase(relativeFilenameToCollector.trim());
    }

    private boolean isCurrentFile(String categoryName, String relativefileNameToCollector) {
        String scribeCurrentFileSymlinkName = categoryName.trim() + constants.getScribeCurrentFileSuffix();
        //TODO:: add code to check for symlink and current file not to be same

        return true;
    }

    private String getCurrentDateTimeAsPathWithCategory(String category) {
        String path = constants.getScribeDataParentDir() + "/" + category + "/" + CalendarHelper.getCurrentDayTimeAsPath();
        return path;
    }


    private String getDestinationFileNameForCategory(String destinationPathForCategory, String collectorName, String dataFileName)  {
        return        destinationPathForCategory + "/" + collectorName + "-" + dataFileName;
    }

    private String getDoneFilePathForCategory(String destinationPathForCategory) {
        return destinationPathForCategory + "/" + constants.getDoneFileName();
    }

    @Override
    public void run() {
        hdfsOperations = getHdfsOperationObject();
        //1. Find all collectors within this category
        String categoryPath = getScribeLogsHdfsPathTillCategory(categoryName);
        List<String> collectorsForCategory;
        try {
            collectorsForCategory = hdfsOperations.getFilesInDirectory(categoryPath);
        } catch (HDFSException e) {
            e.printStackTrace();
            logger.warn(e);
            logger.warn("Unable to/Error find any collectors for category [" + categoryPath +"]");
            //do nothing
            return;
        }
        logger.warn("Working on category [" + categoryPath + "]");

        String destinationPathForCategory  = getCurrentDateTimeAsPathWithCategory(categoryName);
        //Create a directory for  destinationPathForCategory
        try {
            hdfsOperations.createDirectory(destinationPathForCategory);
        } catch (HDFSException e) {
            e.printStackTrace();
            logger.warn(e);
            logger.warn("Error:: Cannot create destination Path [" + destinationPathForCategory + "] for category " + categoryName + "..skipping current iteration." );
            return;
        }

        //2. Find all files inside all collectors
        //2.1 build all collectors full path for HDFS
        //             collectorsFullPath is map of collectorName, collectorFullPath
        Map<String, String> collectorsFullPath = new HashMap<String, String>();
        for(String collectorName : collectorsForCategory) {
            collectorsFullPath.put(collectorName, getScribeLogsHdfsPathTillCollector(categoryName, collectorName));
        }
        Map<String, String> filesToBeMovedAcrossCollectors = new HashMap<String, String>();
        Iterator collectorsFullPathIterator = collectorsFullPath.entrySet().iterator();

        while (collectorsFullPathIterator.hasNext()) {
            Map.Entry pairs = (Map.Entry)collectorsFullPathIterator.next();
            String collectorFullPath = (String) pairs.getValue();
            String collectorName = (String) pairs.getKey();

            List<String> dataFilesInCollector = null;
            logger.warn("Working on collector [" + collectorFullPath + "]");
            try {
                dataFilesInCollector = hdfsOperations.getFilesInDirectory(collectorFullPath);
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("Unable to/Error find any files inside Collector [" + collectorFullPath + "]" + "..doing nothing for this collector");
            }
            // 3. Found files inside the collector
            // 3.1 check whehter it's the current file being written && not scribe_stats
            // 3.2 add it to filesToBeMovedAcrossCollectors with current and new Path

            if (dataFilesInCollector != null) {
                for (String dataFileInCollector : dataFilesInCollector) {
                    String dataFileFullHdfsPath = getScribeLogsHdfsPathTillDataFile(collectorFullPath, dataFileInCollector);
                    if (!isScribeStatsFile(dataFileInCollector) &&   !isCurrentFile(categoryName, dataFileInCollector))
                    {
                        //Add the file to filesToBeMovedAcrossCollectors
                        filesToBeMovedAcrossCollectors.put(dataFileFullHdfsPath, getDestinationFileNameForCategory(destinationPathForCategory, collectorName, dataFileInCollector));
                    }
                }

            }
            else {
                logger.warn("Can't find any files inside collector [" + collectorFullPath + "]");
            }
        }

        //4.    filesToBeMovedAcrossCollectors contains src/dest filename as key/value pairs across collectors for this category
        //4.1 Move the files
        //4.2 Create a Done file in destinationPathForCategory
        // Gottcha - This process can die before creating the DONE file thereby leaving the consumer confused
        // Since this daemon is going to be stateless if it goes down in between after renaming certain files
        // the remaining files will picked up in the next iteration over the collector directories.
        // We can have a house keeping thread which will check if a particular directory hasn't had any activity in the 'X' minutes
        // then create a DONE file within it. Now that house keeping thread is also stateless and can go down however it would catch
        // a scenario like this sometime.
        Iterator filesToBeMovedAcrossCollectorsIterator = filesToBeMovedAcrossCollectors.entrySet().iterator();
        while(filesToBeMovedAcrossCollectorsIterator.hasNext()) {
            Map.Entry pairs = (Map.Entry)filesToBeMovedAcrossCollectorsIterator.next();
            String sourceFileName =  (String) pairs.getKey();
            String destinationFileName = (String) pairs.getValue();
            try {
                hdfsOperations.rename(sourceFileName, destinationFileName);
                logger.warn("Moved [" + sourceFileName + "] to [" + destinationFileName +"]");
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("Error in renaming [" + sourceFileName + "] to [" + destinationFileName + "] will be picked up in next retry over this collector directory..");
            }
        }
        // Reached here means destinationPathForCategory is complete.
        // Create a DONE file to mark completion
        int retryCount = 0;
        while (retryCount < 3 )    {
            String doneFileFullPathForCategory = getDoneFilePathForCategory(destinationPathForCategory);
            try {
                hdfsOperations.createFile(doneFileFullPathForCategory);
                logger.warn("Successfully created DONE file at [" + doneFileFullPathForCategory + "]");
                break;
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("Error in creating Done File [" + doneFileFullPathForCategory + "] going to retry");

            }
            if (retryCount == 3) {
                logger.warn("Error :: exhausted retry count of 3, cannot create DONE file at ["+ doneFileFullPathForCategory + "]" );
                logger.warn("Error :: DONE file creation will be tried by HOUSE KEEPING THREAD");

            }
            retryCount++;
        }

    }
}
