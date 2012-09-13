package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.inmobi.databus.utils.CalendarHelper;

public class OrderlyCreationOfDirs {
  private static final Log LOG = LogFactory.getLog(
      OrderlyCreationOfDirs.class);

  public OrderlyCreationOfDirs() {
  }

  public void doRecursiveListing(Path dir, Set<Path> listing, 
      FileSystem fs) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dir);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + dir);
      listing.add(dir);
    } else {
      for (FileStatus file : fileStatuses) {  
        if (file.isDir()) {
          doRecursiveListing(file.getPath(), listing,  fs);	      
        } else {
          listing.add(file.getPath().getParent());
        }       
      } 
    }
  }

  public void validateOrderlyCreationOfPaths(
      TreeMap<Date , FileStatus> creationTimeOfFiles, 
      List<Path> outOfOrderDirs) {
    Date previousKeyEntry = null;
    for (Date presentKeyEntry : creationTimeOfFiles.keySet() ) {
      if (previousKeyEntry != null) {
        //LOG.info(previousKeyEntry);
        if (creationTimeOfFiles.get(previousKeyEntry).getModificationTime()
            > creationTimeOfFiles.get(presentKeyEntry).getModificationTime()) {
          System.out.println("Directory is created in out of order :    " + 
              creationTimeOfFiles.get(previousKeyEntry).getPath()); 
          outOfOrderDirs.add(creationTimeOfFiles.get(previousKeyEntry)
              .getPath());
          previousKeyEntry = presentKeyEntry;
        }
      }
      previousKeyEntry = presentKeyEntry;
    }
  }

  public void listingAndValidation(Path streamDir, FileSystem fs, 
      List<Path> outOfOrderDirs)
      throws IOException {
    Set<Path> listing = new HashSet<Path>();
    TreeMap<Date, FileStatus>creationTimeOfFiles = new TreeMap<Date, 
        FileStatus >();
    doRecursiveListing(streamDir, listing, fs);
    for (Path path :listing) {
      creationTimeOfFiles.put(CalendarHelper.getDateFromStreamDir(
          streamDir, path), fs.getFileStatus(path));
    }
    validateOrderlyCreationOfPaths(creationTimeOfFiles, outOfOrderDirs);
  }

  public List<Path> pathConstruction(String rootDirs[] , String baseDirs[] , 
      String streamNames[]) throws IOException{
    List<Path> outOfOrderDirs = new ArrayList<Path>();
    for (String rootDir : rootDirs) {
      FileSystem fs = new Path(rootDir).getFileSystem(new Configuration());
      for (String baseDir : baseDirs) {
        Path rootBaseDirPath = new Path(rootDir, baseDir);
        for (String streamName : streamNames) {
          Path streamDir = new Path(rootBaseDirPath , streamName); 
          FileStatus[] files = fs.listStatus(streamDir);
          if (files == null || files.length == 0) {
            break;
          }
          listingAndValidation(streamDir, fs , outOfOrderDirs);
        }    
      }
    }
    return outOfOrderDirs;
  }
  
  public static void main(String[] args) throws IOException {
    if (args.length == 3) {
      String[] rootDirs     =   args[0].split(",");
      String[] baseDir      =   args[1].split(",");
      String[] streamName   =   args[2].split(",");
      OrderlyCreationOfDirs obj = new OrderlyCreationOfDirs();
      obj.pathConstruction(rootDirs, baseDir, streamName);
    } else {
      System.out.println("Insufficient number of arguments: enter rootdirs," +
      		" basedirs, streamnames ");
    }
  }
}
