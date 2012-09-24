package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.inmobi.databus.utils.CalendarHelper;

/**
 * This class finds the out of order minute directories in various streams 
 * for different clusters. 
 * This class main function takes list of root dirs, base dirs, stream names
 *  as arguments. All are comma separated 
 */
public class OrderlyCreationOfDirs {
  private static final Log LOG = LogFactory.getLog(
      OrderlyCreationOfDirs.class);

  public OrderlyCreationOfDirs() {
  }
 
  /**
   * This method lists all the minute directories for a particular 
   * stream category.
   */
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

  /**
   *  This method finds the out of order minute directories for a 
   *  particular stream.
   *  @param creationTimeOfFiles : TreeMap for all the directories statuses
   *   for a particular stream
   *  @param outOfOrderDirs : store out of order directories : outOfOrderDirs
   */
  public void validateOrderlyCreationOfPaths(
      TreeMap<Date , FileStatus> creationTimeOfFiles, 
      List<Path> outOfOrderDirs) {
    Date previousKeyEntry = null;
    for (Date presentKeyEntry : creationTimeOfFiles.keySet() ) {
      if (previousKeyEntry != null) {
        if (creationTimeOfFiles.get(previousKeyEntry).getModificationTime()
            > creationTimeOfFiles.get(presentKeyEntry).getModificationTime()) {
          System.out.println("Directory is created in out of order :    " + 
              creationTimeOfFiles.get(presentKeyEntry).getPath()); 
          outOfOrderDirs.add(creationTimeOfFiles.get(previousKeyEntry)
              .getPath());
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
  
  public void getStreamNames(String baseDir, String rootDir, List<String>
  		streamNames) throws Exception {
  		FileSystem baseDirFs = new Path(rootDir, baseDir).getFileSystem
  				(new Configuration());
  		FileStatus[] streamFileStatuses = baseDirFs.listStatus(new Path
  				(rootDir, baseDir));
  		for (FileStatus file : streamFileStatuses) {
  			if (!streamNames.contains(file.getPath().getName())) {
  				streamNames.add(file.getPath().getName());
  			}
  		}
  	
  }
  
  public void getBaseDirs(String baseDirArg, List<String> baseDirs) {
  	for (String baseDir : baseDirArg.split(",")) {
			baseDirs.add(baseDir);
		}
  }
  
  public List<Path> run(String [] args) throws Exception {
  	List<Path> outoforderdirs = new ArrayList<Path>();
  	if (args.length >= 1) {
  		String[]	rootDirs = args[0].split(",");
  		List<String> baseDirs = new ArrayList<String>();
  		List<String> streamNames;
  		if (args.length == 1) {
  			baseDirs.add("streams");
  			baseDirs.add("streams_local");
  			for (String rootDir : rootDirs) {
  				for (String baseDir : baseDirs) {
  					streamNames = new ArrayList<String>();
  					getStreamNames(baseDir, rootDir, streamNames);
  					outoforderdirs.addAll(pathConstruction(rootDir, baseDir, 
  							streamNames));
  				}
  			}
  			if (outoforderdirs.isEmpty()) {
  				System.out.println("There are no out of order dirs");
  			} 
  		} else if (args.length == 2) {
  			getBaseDirs(args[1], baseDirs);
  			for (String rootDir : rootDirs) {
  				for (String baseDir : baseDirs) {
  					streamNames = new ArrayList<String>();
  					getStreamNames(baseDir, rootDir, streamNames);
  					outoforderdirs.addAll(pathConstruction(rootDir, baseDir, 
  							streamNames));
  				}
  			}
  			if (outoforderdirs.isEmpty()) {
  				System.out.println("There are no out of order dirs");
  			} 
  		} else if (args.length == 3) {
  			getBaseDirs(args[1], baseDirs);
  			streamNames = new ArrayList<String>();
  			for (String streamname : args[2].split(",")) {
  				streamNames.add(streamname);
  			}
  			for (String rootDir : rootDirs) {
  				for (String baseDir : baseDirs) {
  					outoforderdirs.addAll(pathConstruction(rootDir, baseDir, 
  							streamNames));
  				}
  			}
  			if (outoforderdirs.isEmpty()) {
  				System.out.println("There are no out of order dirs");
  			} 
  		} 
  	} else {
  		System.out.println("Insufficient number of arguments: 1st argument:" +
  				" rootdirs," + " 2nd arument :basedirs, 3rd arguments: streamnames"
  				+ " 2nd arg, 3rd args are optionl here");
  		System.exit(1);
  	}
  	return outoforderdirs;
  }

  /**
   * @param  rootDirs : array of root directories
   * @param  baseDirs : array of baseDirs
   * @param  streamNames : array of stream names
   * @return outOfOrderDirs: list of out of directories for all the streams.
   */
  public List<Path> pathConstruction(String rootDir, String baseDir,
  		List<String> streamNames) throws IOException {
  	List<Path> outOfOrderDirs = new ArrayList<Path>();
  	FileSystem fs = new Path(rootDir).getFileSystem(new Configuration());
  	Path rootBaseDirPath = new Path(rootDir, baseDir);
  	for (String streamName : streamNames) {
  		Path streamDir = new Path(rootBaseDirPath , streamName);
  		FileStatus[] files = fs.listStatus(streamDir);
  		if (files == null || files.length == 0) {
  			LOG.info("No direcotries in that stream: " + streamName);
  			continue;
  		}
  		listingAndValidation(streamDir, fs , outOfOrderDirs);
  	}
  	return outOfOrderDirs;
  }

  public static void main(String[] args) throws Exception {
  	OrderlyCreationOfDirs obj = new OrderlyCreationOfDirs();
  	obj.run(args);
  }
}