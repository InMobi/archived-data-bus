package com.inmobi.databus;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 01/12/11
 * Time: 7:32 PM
 * To change this template use File | Settings | File Templates.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.inmobi.databus.Constants.*;

public class HdfsOperations {

    public Configuration getConfiguration() {
        return configuration;
    }

    Configuration configuration = new Configuration();

    Logger logger = Logger.getLogger(HdfsOperations.class);


    public String readFirstLineOfFile(String fileName) throws HDFSException {
           if (fileName == null)
            throw new HDFSException("fileName cannot be null");
           FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }
        catch (IOException e){
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path filePath = new Path(fileName);

        try {
            if (!hdfs.exists(filePath))
                throw new HDFSException("filename [" + fileName + "] not found");
            BufferedReader bfr;
            bfr = new BufferedReader(new InputStreamReader(hdfs.open(filePath)));
            String firstLine = bfr.readLine();
            bfr.close();
            return firstLine;

        }
        catch (IOException ioe) {
          ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }


    }

    public void createFile(String fileName) throws HDFSException {
        if (fileName == null)
            throw new HDFSException("fileName cannot be null");
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }
        catch (IOException e){
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path filePath = new Path(fileName);
        try {
            FSDataOutputStream out = null;
            if (hdfs.exists(filePath))
                throw new HDFSException("File [" + fileName + "] already exists");
            out = hdfs.create(filePath);
            out.close();
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }
    }

    public List<String> getFilesInDirectory(String dirName) throws HDFSException {
        List<String> fileList = new ArrayList<String>();

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }
        catch (IOException e){
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        if (!isDir(dirName))   {
            throw new HDFSException(dirName + " is not a directory in HDFS");
        }
        FileStatus[] fileStatuses;
        try {
            fileStatuses = hdfs.listStatus(new Path(dirName));
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }
        for(FileStatus fileStatus : fileStatuses) {
            if (fileStatus != null)
                fileList.add(fileStatus.getPath().getName());

        }
        return fileList;
    }

    public  void createDirectory(String dirName) throws HDFSException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }
        catch (IOException e){
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path path = new Path(dirName);

        try {
            hdfs.mkdirs(path);

        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }

    }


    public boolean isDir(String dirName) throws HDFSException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }
        catch (IOException e){
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path path = new Path(dirName);
        boolean isDir;
        try {
            isDir = hdfs.getFileStatus(path).isDir();
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }
        return isDir;
    }

    public boolean isFile(String fileName) throws HDFSException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }
        catch (IOException e){
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path path = new Path(fileName);
        boolean isFile;
        try {
            isFile = hdfs.isFile(path);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }
        return isFile;
    }
    /*
     * @returns true if file exists else false
     * fileName should be full HDFS path
     */
    public boolean isExists(String fileName)  throws  HDFSException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }
        catch (IOException e){
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path path = new Path(fileName);
        boolean isExists;
        try {
            isExists = hdfs.exists(path);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }
        return isExists;
    }

    /*
     * fileName should be full hdfs filePath
     */
    public long getLastModificationTimeStamp(String fileName) throws  HDFSException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        }   catch (IOException e) {
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }

        Path path = new Path(fileName);
        FileStatus fileStatus;
        try {
            fileStatus = hdfs.getFileStatus(path);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }
        return fileStatus.getModificationTime();

    }
    /*
    * This is a recursive delete operation.
    * path should be an HDFS path else it will
    * throw HDFS Exception
    */
    public void deleteRecursively(String path) throws HDFSException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path deletePath = new Path(path);
        try {
            // we are doing a  recursive delete here
            hdfs.delete(deletePath, true);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }

    }

    /*
    * This is a nonrecursive delete operation.
    * path should be an HDFS path else it will
    * throw HDFS Exception
    */
    public void delete(String path) throws HDFSException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path deletePath = new Path(path);
        try {
            // we are doing a non recursive delete here
            hdfs.delete(deletePath, false);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }

    }

    /*
     * rename srcName to destName. Path should be HDFS path
     */
    public void rename(String srcName, String destName) throws  HDFSException{

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            logger.debug(e);
            throw new HDFSException(e.getMessage());
        }
        Path srcPath = new Path(srcName);
        Path destPath = new Path(destName);
        try {
            hdfs.rename(srcPath, destPath);

        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            logger.debug(ioe);
            throw new HDFSException(ioe.getMessage());
        }
    }

    public static void main(String[] args) {
        // This is more like a test class for the above HDFS operations

        HdfsOperations hdfsOps = new HdfsOperations();

        //1. create a new directory
        try {
            Configuration configuration = hdfsOps.getConfiguration();
            String dfsName = "hdfs://gs1101.grid.corp.inmobi.com:54310/";
            configuration.set("fs.default.name", dfsName);
            //1. create a directory
            //String directory =    "user/inderbir/billing/2011/11/01/01";
            //hdfsOps.createDirectory(directory);
            //2. check whether it's a directory
            //System.out.println(" Checking " + directory + " for isDir :: " + hdfsOps.isDir(directory));
            //3. Delete the directory
            // System.out.println("Recursively deleting user/inderbir/billing/2011/11/01/01 starting from user/");
            //String deleteDir =   "/user/inderbir/user/  ";
            //hdfsOps.deleteRecursively(deleteDir);
            //4. Check for isFile
            //String fileName = "/user/inderbir/scribedata/metric/gs1102.grid.corp.inmobi.com/metric-2011-11-28_00000";
            //System.out.println("IsFile check on " + fileName + " :: " + hdfsOps.isFile(fileName));
            //5. Move fileName to directory
            //String destFile =   "user/inderbir/billing/2011/11/01/01/gs1102.grid.corp.inmobi.com-metric-2011-11-28_00000";
            //System.out.println("Move file :: " + fileName + " to :: " + destFile);
            //hdfsOps.rename(fileName, destFile);
            //6. List files in a directory
            String listDir = "/user/inderbir/scribedata/metric/gs1102.grid.corp.inmobi.com/";
            System.out.println("Listing files in a directory :: " + listDir);
            List<String> fileList = hdfsOps.getFilesInDirectory(listDir);
            for (String fileName : fileList) {
                System.out.println("File Name :: " + fileName);
            }



        } catch (HDFSException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

}
