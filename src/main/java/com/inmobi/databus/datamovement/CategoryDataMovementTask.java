package com.inmobi.databus.datamovement;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

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

    List<String> categoryList;
    Constants constants;
    HdfsOperations hdfsOperations;

    public CategoryDataMovementTask(List<String> categoryList, Constants constants) {
        this.categoryList = categoryList;
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

    private String getCurrentDateTimeAsIntermediatePathWithCategory(String category) {
        String path = constants.getScribeIntermediateDataDir() + "/" + category + "/" + CalendarHelper.getCurrentDayTimeAsPath();
        logger.debug("getCurrentDateTimeAsIntermediatePathWithCategory :: category [" + category + "] path [" + path + "]");
        return path;
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

    private String getIntermediateDestinationFileNameForCategory(String intermediateDestinationPathForCategory, String collectorName, String dataFileName)  {
        String path = intermediateDestinationPathForCategory + "/" + collectorName + "-" + dataFileName;
        logger.debug("getIntermediateDestinationFileNameForCategory :: [" + path + "]");
        return       path;
    }


    private String getCheckPointFilePath() {
        String path = constants.getScribeTmpDir() + "/" +  CalendarHelper.getCurrentDayTimeAsString();
        return path;
    }

    private String getDoneFilePathForCategory(String intermediateDestinationPathForCategory) {
        String path = intermediateDestinationPathForCategory + "/" + constants.getDoneFileName();
        logger.debug("getDoneFilePathForCategory :: path [" + path + "]");
        return  path;
    }


    private  List<String>  getCollectorsForCategory(String categoryPath) {
        List<String> collectorsForCategory;
        try {
            collectorsForCategory = hdfsOperations.getFilesInDirectory(categoryPath);
        } catch (HDFSException e) {
            e.printStackTrace();
            logger.warn(e);
            logger.warn("Unable to/Error find any collectors for category [" + categoryPath +"]");
            //do nothing
            return null;
        }
        return collectorsForCategory;

    }

    private List<String> getDataFilesInCollector(String collectorFullPath) {
        logger.warn("Working on collector [" + collectorFullPath + "]");
        List<String> dataFilesInCollector = null;
        try {
            dataFilesInCollector = hdfsOperations.getFilesInDirectory(collectorFullPath);
        } catch (HDFSException e) {
            e.printStackTrace();
            logger.warn(e);
            logger.warn("Unable to/Error find any files inside Collector [" + collectorFullPath + "]" + "..doing nothing for this collector moving to next collector files");
            //Do nothing for this collector, move to next collector
            return null;
        }
        return dataFilesInCollector;
    }

    private boolean isFileSizeNonZero(String dataFileFullHdfsPath) {
        long fileSize = 0;
        try {
            fileSize = hdfsOperations.getSize(dataFileFullHdfsPath) ;
        }
        catch (HDFSException e){
            logger.warn(e);
            logger.warn("Unable to get file Size for [" + dataFileFullHdfsPath + "] assuming default 0");
        }
        if (fileSize > 0)
            return  true;
        else
            return false;
    }

    private void deleteFile(String dataFileFullHdfsPath) {
        try {
            hdfsOperations.delete(dataFileFullHdfsPath);
        } catch (HDFSException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
            logger.warn("Unable to delete file [" + dataFileFullHdfsPath + "]");
        }
    }

    private void createDoneFile(String intermediateDestinationPathForCategory) {
        int retryCount = 0;
        //if no files were created don't create an empty DONE file
        while (retryCount < 3 )    {
            String doneFileFullPathForCategory = getDoneFilePathForCategory(intermediateDestinationPathForCategory);
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

    private void deleteEmptyIntermediateDestinationPathForCategory(String intermediateDestinationPathForCategory) {
        try {
            if (intermediateDestinationPathForCategory == null)
                return;
            if (hdfsOperations.isExists(intermediateDestinationPathForCategory))  {
                // empty directory has not files were moved. This should not exist in the first place.
                hdfsOperations.deleteRecursively(intermediateDestinationPathForCategory);
            }
            logger.warn("No files were moved in this iteration for category  delete the destination directory [" + intermediateDestinationPathForCategory + "]");
        } catch (HDFSException e) {
            e.printStackTrace();
            logger.warn(e);
            logger.warn("No files were moved in this iteration. Failed deleting directory [" + intermediateDestinationPathForCategory + "]");
        }
    }

    private void initialize() {
        //Create the tmp directory if not existing
        boolean status = false;
        try {
            status  = hdfsOperations.isExists(constants.getScribeTmpDir());
        }
        catch (HDFSException hdfsException) {
            logger.warn(hdfsException);
            logger.warn("Error in checking existence of temp directory [" + constants.getScribeTmpDir() + "] will try creating");
        }
        if (!status) {
            logger.warn("Creating [" + constants.getScribeTmpDir() +"]" );
            try {
                hdfsOperations.createDirectory(constants.getScribeTmpDir());
            }
            catch (HDFSException hdfsException) {
                logger.warn(hdfsException);
                logger.warn("Error in creation of temp directory [" + constants.getScribeTmpDir() + "]...Fatal exiting");
                System.exit(-1);
            }
        }
    }

    @Override
    public void run() {
        hdfsOperations = getHdfsOperationObject();
        initialize();
        String checkPointFileForThisRun = getCheckPointFilePath();

        for (String categoryName : categoryList) {

            Map<String, String> filesToBeMovedAcrossCollectors = new HashMap<String, String>();

            String categoryPath = getScribeLogsHdfsPathTillCategory(categoryName);
            logger.warn("Working on category [" + categoryPath + "]");
            //1. Find all collectors within this category
            List<String> collectorsForCategory = getCollectorsForCategory(categoryPath);
            if (collectorsForCategory == null || (collectorsForCategory != null &&
                    collectorsForCategory.size() <= 0)) {
                logger.warn("No collectors found for category [" + categoryPath + "] skipping this catgeory");
                continue;
            }

            String intermediateDestinationPathForCategory =  getCurrentDateTimeAsIntermediatePathWithCategory(categoryName);
            logger.warn("Intermediate Path for category [" + categoryName + "] is [" + intermediateDestinationPathForCategory + "]");

            //2. Find all files inside all collectors
            //2.1 build all collectors full path for HDFS
            //             collectorsFullPath is map of collectorName, collectorFullPath
            Map<String, String> collectorsFullPath = new HashMap<String, String>();
            for(String collectorName : collectorsForCategory) {
                collectorsFullPath.put(collectorName, getScribeLogsHdfsPathTillCollector(categoryName, collectorName));
            }

            Iterator collectorsFullPathIterator = collectorsFullPath.entrySet().iterator();
            while (collectorsFullPathIterator.hasNext()) {
                Map.Entry pairs = (Map.Entry)collectorsFullPathIterator.next();
                String collectorName = (String) pairs.getKey();
                String collectorFullPath = (String) pairs.getValue();

                List<String> dataFilesInCollector = getDataFilesInCollector(collectorFullPath);

                // 3. Found files inside the collector
                // 3.1 check whehter it's the current file being written && not scribe_stats
                // 3.2 add it to filesToBeMovedAcrossCollectors with current and new Path

                if (dataFilesInCollector != null) {
                    for (String dataFileInCollector : dataFilesInCollector) {
                        String dataFileFullHdfsPath = getScribeLogsHdfsPathTillDataFile(collectorFullPath, dataFileInCollector);

                        if (!isScribeStatsFile(dataFileInCollector) &&  !isSymLinkFile(dataFileInCollector) &&
                            !isCurrentFile(categoryName, dataFileInCollector, collectorFullPath) && isFileSizeNonZero(dataFileFullHdfsPath))
                        {
                            //Add the file to filesToBeMovedAcrossCollectors
                            filesToBeMovedAcrossCollectors.put(dataFileFullHdfsPath, getDestinationFileNameForCategory(intermediateDestinationPathForCategory, collectorName, dataFileInCollector));
                        }
                        if (!isFileSizeNonZero(dataFileFullHdfsPath) && !isCurrentFile(categoryName, dataFileInCollector, collectorFullPath)) {
                            //P.S. : isCurrentFile check is mandatory above as HDFS updates the file size to be > 0
                            //only on flush/sync and without it the current file of HDFS will get removed and scribe will cry :(
                            //remove the file from the source
                            deleteFile(dataFileFullHdfsPath);
                            logger.warn("Source filesize for [" + dataFileFullHdfsPath + "] is 0, deleteing it from source");
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
            // Gottcha - This process can die before creating the DONE file thereby leaving these directories without DONE file
            // Since this daemon is going to be stateless if it goes down in between after renaming certain files
            // the remaining files will picked up in the next iteration over the collector directories.
            // When this program is restarted it will first run in cleanup mode wherein it will find such directories
            // for each CATEGORY and create a DONE file within it.
            //Create a directory for  intermediateDestinationPathForCategory if there are files to be moved
            if (!filesToBeMovedAcrossCollectors.isEmpty()) {
                try {
                    hdfsOperations.createDirectory(intermediateDestinationPathForCategory);
                } catch (HDFSException e) {
                    e.printStackTrace();
                    logger.warn(e);
                    logger.warn("Error:: Cannot create Intermediate destination Path [" + intermediateDestinationPathForCategory + "] for category " + categoryName + "..skipping current iteration." );
                    continue;
                }
            }
            Iterator filesToBeMovedAcrossCollectorsIterator = filesToBeMovedAcrossCollectors.entrySet().iterator();
            while(filesToBeMovedAcrossCollectorsIterator.hasNext()) {
                Map.Entry pairs = (Map.Entry)filesToBeMovedAcrossCollectorsIterator.next();
                String sourceFileName =  (String) pairs.getKey();
                String intermediateDestinationFileName = (String) pairs.getValue();
                try {
                    hdfsOperations.rename(sourceFileName, intermediateDestinationFileName);
                    logger.debug("Moved [" + sourceFileName + "] to [" + intermediateDestinationFileName + "]");
                } catch (HDFSException e) {
                    e.printStackTrace();
                    logger.warn(e);
                    logger.warn("Error in renaming [" + sourceFileName + "] to [" + intermediateDestinationFileName + "] will be picked up in next retry over this collector directory in next RUN..");
                }
            }
            // Reached here means intermediateDestinationFileName is complete.
            // Create a DONE file to mark completion
            // This program can die before creating a DONE file. This will be taken care during next cycle start
            // which will check all tmp directories and fix the DONE file and move them to destination

            if (!filesToBeMovedAcrossCollectors.isEmpty()) {
                createDoneFile(intermediateDestinationPathForCategory);
            }
            else  {
                deleteEmptyIntermediateDestinationPathForCategory(intermediateDestinationPathForCategory);
            }

            // files are available at the intermediate path
            // move to the final destination  :: by renaming the directories as that's an atomic operation
            // the files in intermediate storage are available in something like
            // Source Directory eg: -- /<com.inmobi.databus.intermediateDataDir>/<category>/<year>/<month>/<day>/<hour>/<minute>/
            // Destination Directory eg: - /<com.inmobi.databus.scribe.data.parentdir>/<category>/<year>/<month>/<day>/<hour>/<minute>/
            String finalDestinationPathForCategory  = getCurrentDateTimeAsPathWithCategory(categoryName);
            logger.warn("Final Path for category [" + categoryName + "] is [" + finalDestinationPathForCategory + "]");
            //Create   finalDestinationPathForCategory
            try {
                if (!hdfsOperations.isExists(finalDestinationPathForCategory))
                     hdfsOperations.createDirectory(finalDestinationPathForCategory);
            } catch (HDFSException e) {
                e.printStackTrace();
                logger.warn(e);
                logger.warn("Error:: Cannot create finalDestinationPathForCategory destination Path [" + finalDestinationPathForCategory + "] for category " + categoryName + " from [" + intermediateDestinationPathForCategory + "]");
                //If this failed in moving data to final destination
                //It will retry doing that in the next run of this program at start
                continue;
            }
            try {
                // before moving from    intermediateDestinationPathForCategory to      intermediateDestinationPathForCategory
                // copy the absolute file names to the checkpoint file to be used by distcp in the workflow.
                boolean doRename = true;
                try {
                    hdfsOperations.writeAppendLine(finalDestinationPathForCategory, checkPointFileForThisRun, true);
                }
                catch (HDFSException he) {
                    logger.warn(he);
                    logger.warn("Unable to check point [" + finalDestinationPathForCategory + "] skipping it's move from [" + intermediateDestinationPathForCategory + "] to ["+ finalDestinationPathForCategory + "] should get handled in next run cleanup mode");
                    doRename = false;
                }
                if(doRename)   {
                    // if this server dies after checkpointing the file
                    // it will be picked up in the next run CLEANUP MODE
                    // and distcp will just ignore since the file is not present.
                    hdfsOperations.rename(intermediateDestinationPathForCategory, finalDestinationPathForCategory);
                    logger.warn(" Moving [" + intermediateDestinationPathForCategory + "] to [" + finalDestinationPathForCategory + "] and check pointed it in [" + checkPointFileForThisRun + "]");
                }
            }
            catch(HDFSException hdfsException) {
                hdfsException.printStackTrace();
                logger.warn(hdfsException);
                logger.warn("Unable to move data to ["+ finalDestinationPathForCategory + "] from [" + intermediateDestinationPathForCategory + "] skipping in current interation");
            }

        }   // for each category
    }
}