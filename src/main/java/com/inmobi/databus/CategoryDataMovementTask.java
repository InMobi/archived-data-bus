package com.inmobi.databus;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

import java.util.*;


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
        logger.debug("getHdfsOperationObject setting fs.default.name to [" + dfsName + "]");
        return  hdfsOperations;
    }

    private String getScribeLogsHdfsPathTillCategory(String categoryName) {
        String categoryPath =  constants.getLogsParentDir() + "/" + categoryName.trim() + "/";
        logger.debug(" getScribeLogsHdfsPathTillCategory :: categoryPath [" + categoryPath + "]");
        return categoryPath;

    }

    private String getScribeLogsHdfsPathTillCollector(String categoryName, String collectorName) {
        String collectorPath =  constants.getLogsParentDir() + "/" + categoryName.trim() + "/" + collectorName.trim() + "/";
        logger.debug("getScribeLogsHdfsPathTillCollector :: collectorPath [" + collectorPath + "]");
        return collectorPath;
    }

    private String getScribeLogsHdfsPathTillDataFile(String categoryName, String collectorName, String dataFile) {
        String dataFilePath =  constants.getLogsParentDir() + "/" + categoryName.trim() + "/" + collectorName.trim() + "/" + dataFile.trim();
        logger.debug("getScribeLogsHdfsPathTillDataFile :: dataFilePath [" + dataFilePath + "]");
        return dataFilePath;
    }

    private String getScribeLogsHdfsPathTillDataFile(String collectorFullPath, String dataFile) {
        String dataFilePath =  collectorFullPath.trim() + "/" + dataFile.trim();
        logger.debug("getScribeLogsHdfsPathTillDataFile :: dataFilePath [" + dataFilePath + "]");
        return dataFilePath;
    }

    private boolean isScribeStatsFile(String relativeFilenameToCollector) {
        if (relativeFilenameToCollector == null)
            return false;
        logger.debug("isScribeStatsFile :: checking for [" + relativeFilenameToCollector + "] comparing with [" + constants.getScribeStatsFileName() + "]");
        return constants.getScribeStatsFileName().equalsIgnoreCase(relativeFilenameToCollector.trim());
    }

    private boolean isSymLinkFile(String relativefileNameToCollector) {
        logger.debug("isSymLinkFile :: Checking whether [" + relativefileNameToCollector + "] contains " + constants.getScribeCurrentFileSuffix());
        if (relativefileNameToCollector.contains(constants.getScribeCurrentFileSuffix())) {
            logger.debug("isSymLinkFile :: Check status [true]");
            return true;
        }
        logger.debug("isSymLinkFile :: Check status [false]");
        return false;
    }

    private boolean isCurrentFile(String categoryName, String relativefileNameToCollector, String collectorPath) {
        String scribeCurrentFileSymlinkName = categoryName.trim() + constants.getScribeCurrentFileSuffix();
        // Scribe is simulating symlink so we need to do the following
        String symLinkFile = collectorPath.trim() + "/" + categoryName.trim() + constants.getScribeCurrentFileSuffix();
        logger.debug("isCurrentFile :: symLinkFile [" + symLinkFile + "]");
        String actualCurrentFileName;
        try {
            actualCurrentFileName = hdfsOperations.readFirstLineOfFile(symLinkFile);
            logger.debug("isCurrentFile :: actualCurrentFileName [" + actualCurrentFileName + "]");
        } catch (HDFSException e) {
            logger.warn(e);
            logger.warn("Unable to read symlink File [" + symLinkFile + "]to find current File");
            return false;
        }
        if (actualCurrentFileName.trim().equalsIgnoreCase(relativefileNameToCollector)) {
            logger.debug("isCurrentFile :: [" + relativefileNameToCollector + "] is the current file being written, returning [true]");
            return true;

        }

        logger.debug("isCurrentFile :: [" + relativefileNameToCollector + "] is the current file being written, returning [false]");
        return false;
    }

    private String getCurrentDateTimeAsPathWithCategory(String category) {
        String path = constants.getScribeDataParentDir() + "/" + category + "/" + CalendarHelper.getCurrentDayTimeAsPath();
        logger.debug("getCurrentDateTimeAsPathWithCategory :: category [" + category + "] path [" + path + "]");
        return path;
    }


    private String getDestinationFileNameForCategory(String destinationPathForCategory, String collectorName, String dataFileName)  {
        String path = destinationPathForCategory + "/" + collectorName + "-" + dataFileName;
        logger.debug("getDestinationFileNameForCategory :: [" + path + "]");
        return       path;
    }

    private String getDoneFilePathForCategory(String destinationPathForCategory) {
        String path = destinationPathForCategory + "/" + constants.getDoneFileName();
        logger.debug("getDoneFilePathForCategory :: path [" + path + "]");
        return  path;
    }

    @Override
    public void run() {
        hdfsOperations = getHdfsOperationObject();
        //1. Find all collectors within this category

        String categoryPath = getScribeLogsHdfsPathTillCategory(categoryName);
        logger.warn("Working on category [" + categoryPath + "]");
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


        String destinationPathForCategory  = getCurrentDateTimeAsPathWithCategory(categoryName);
        logger.warn("Final Path for category [" + categoryName + "] is [" + destinationPathForCategory + "]");


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
            String collectorName = (String) pairs.getKey();
            String collectorFullPath = (String) pairs.getValue();

            List<String> dataFilesInCollector = null;
            logger.warn("Working on collector [" + collectorFullPath + "]");
            try {
                dataFilesInCollector = hdfsOperations.getFilesInDirectory(collectorFullPath);
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("Unable to/Error find any files inside Collector [" + collectorFullPath + "]" + "..doing nothing for this collector moving to next collector files");
                //Do nothing for this collector, move to next collector
                continue;
            }
            // 3. Found files inside the collector
            // 3.1 check whehter it's the current file being written && not scribe_stats
            // 3.2 add it to filesToBeMovedAcrossCollectors with current and new Path

            if (dataFilesInCollector != null) {
                for (String dataFileInCollector : dataFilesInCollector) {
                    String dataFileFullHdfsPath = getScribeLogsHdfsPathTillDataFile(collectorFullPath, dataFileInCollector);
                    long fileSize = 0 ;
                    try {
                        fileSize = hdfsOperations.getSize(dataFileFullHdfsPath) ;
                    }
                    catch (HDFSException e){
                        logger.warn(e);
                        logger.warn("Unable to get file Size for [" + dataFileFullHdfsPath + "] assuming default 0");
                    }
                    if (!isScribeStatsFile(dataFileInCollector) &&  !isSymLinkFile(dataFileInCollector) &&
                            !isCurrentFile(categoryName, dataFileInCollector, collectorFullPath) && fileSize > 0)
                    {
                        //Add the file to filesToBeMovedAcrossCollectors
                        filesToBeMovedAcrossCollectors.put(dataFileFullHdfsPath, getDestinationFileNameForCategory(destinationPathForCategory, collectorName, dataFileInCollector));
                    }
                    if (fileSize == 0 && !isCurrentFile(categoryName, dataFileInCollector, collectorFullPath)) {
                        //P.S. : isCurrentFile check is mandatory above as HDFS updates the file size to be > 0
                        //only on flush/sync and without it the current file of HDFS will get removed and scribe will cry :(
                        //remove the file from the source
                        logger.warn("Source filesize for [" + dataFileFullHdfsPath + "] is 0, deleteing it from source");
                        try {
                            hdfsOperations.delete(dataFileFullHdfsPath);
                        } catch (HDFSException e) {
                            e.printStackTrace();
                            logger.warn(e.getMessage());
                            logger.warn("Unable to delete file [" + dataFileFullHdfsPath + "]");
                        }
                    }
                }

            }
            else {
                logger.warn("Can't find any files inside collector [" + collectorFullPath + "]");
            }
        } // while loop to check for all collectors

        //4.    filesToBeMovedAcrossCollectors contains src/dest filename as key/value pairs across collectors for this category
        //4.1 Move the files
        //4.2 Create a Done file in destinationPathForCategory
        // Gottcha - This process can die before creating the DONE file thereby leaving the consumer confused
        // Since this daemon is going to be stateless if it goes down in between after renaming certain files
        // the remaining files will picked up in the next iteration over the collector directories.
        // We can have a house keeping thread which will check if a particular directory hasn't had any activity in the 'X' minutes
        // then create a DONE file within it. Now that house keeping thread is also stateless and can go down however it would catch
        // a scenario like this sometime.
        //Create a directory for  destinationPathForCategory if there are files to be moved
        if (!filesToBeMovedAcrossCollectors.isEmpty()) {
            try {
                hdfsOperations.createDirectory(destinationPathForCategory);
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("Error:: Cannot create destination Path [" + destinationPathForCategory + "] for category " + categoryName + "..skipping current iteration." );
                return;
            }
        }
        Iterator filesToBeMovedAcrossCollectorsIterator = filesToBeMovedAcrossCollectors.entrySet().iterator();
        while(filesToBeMovedAcrossCollectorsIterator.hasNext()) {
            Map.Entry pairs = (Map.Entry)filesToBeMovedAcrossCollectorsIterator.next();
            String sourceFileName =  (String) pairs.getKey();
            String destinationFileName = (String) pairs.getValue();
            try {
                hdfsOperations.rename(sourceFileName, destinationFileName);
                logger.debug("Moved [" + sourceFileName + "] to [" + destinationFileName + "]");
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("Error in renaming [" + sourceFileName + "] to [" + destinationFileName + "] will be picked up in next retry over this collector directory..");
            }
        }
        // Reached here means destinationPathForCategory is complete.
        // Create a DONE file to mark completion
        int retryCount = 0;
        if (!filesToBeMovedAcrossCollectors.isEmpty()) {
            //if no files were created don't create an empty DONE file
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
        else  {
            try {
                if (hdfsOperations.isExists(destinationPathForCategory))  {
                 // empty directory has not files were moved. This should not exist in the first place.
                    hdfsOperations.deleteRecursively(destinationPathForCategory);
                }
                logger.warn("No files were moved in this iteration for category [" + categoryPath + "], delete the destination directory [" + destinationPathForCategory + "]");
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("No files were moved in this iteration. Failed deleting directory [" + destinationPathForCategory + "]");
            }

        }


    }
}
