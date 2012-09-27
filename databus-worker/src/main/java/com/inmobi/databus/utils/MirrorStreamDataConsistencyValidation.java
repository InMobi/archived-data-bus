package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class checks the data consistency between merged stream and mirrored 
 * stream for different clusters. It returns the both missed paths and data 
 * replay paths.
 * This class main method takes merged stream, comma separated list of mirror 
 * stream urls and comma separated list of stream names
 *
 */
public class MirrorStreamDataConsistencyValidation {
  private static final Log LOG = LogFactory.getLog(
  		MirrorStreamDataConsistencyValidation.class);
  List<Path> mirrorStreamDirs;
  Path mergedStreamDirPath;
  
  public MirrorStreamDataConsistencyValidation(String mirrorStreamUrls, 
  		String mergedStreamUrl) throws IOException {
  	String[] rootDirSplits;
  	mergedStreamDirPath = new Path(mergedStreamUrl, "streams");
  	if (mirrorStreamUrls != null) {
  		rootDirSplits = mirrorStreamUrls.split(",");
  	} else {
  		throw new IllegalArgumentException("Databus root directory not specified");
  	}
  	mirrorStreamDirs = new ArrayList<Path>(rootDirSplits.length);
  	for (String mirrorRootDir : rootDirSplits) {
  		mirrorStreamDirs.add(new Path(mirrorRootDir, "streams"));
  	}
	}
  
  public List<Path> processListingStreams(String streamName) throws 
  		IOException {
  	List<Path> inconsistentData = new ArrayList<Path>();
  	List<Path> filesInMergedStream = new ArrayList<Path>();
 	  mergedStreamListing(streamName, filesInMergedStream);
 	  mirrorStreamListing(streamName, inconsistentData, filesInMergedStream);
 	  return inconsistentData;
  }
  
  public void mergedStreamListing(String streamName, List<Path> 
  		filesInMergedStream) throws IOException {
  	Path completeMergedStreamDirPath = new Path(mergedStreamDirPath,
  			streamName);
  	LOG.info("merged Stream path : " + completeMergedStreamDirPath);    
  	FileSystem mergedFS = completeMergedStreamDirPath.getFileSystem(
  			new Configuration());
  	doRecursiveListing(completeMergedStreamDirPath, filesInMergedStream,
  			mergedFS);             
  	Iterator it = filesInMergedStream.iterator();
  	while (it.hasNext()) {
  		LOG.debug(" files in merged stream : " + (it.next()));
  	}
  }
  
  public void mirrorStreamListing(String streamName, List<Path> 
  		inconsistentData, List<Path> filesInMergedStream) throws IOException {
  	Path mirrorStreamDirPath;
  	FileSystem mirroredFs;
  	for (int i = 0; i < mirrorStreamDirs.size(); i++) {
  		List<Path> filesInMirroredStream = new ArrayList<Path>();
  		mirrorStreamDirPath = new Path(mirrorStreamDirs.get(i), streamName);
  		mirroredFs = mirrorStreamDirPath.getFileSystem(new Configuration());
  		LOG.info("mirroredStream Path : " + mirrorStreamDirPath);
  		doRecursiveListing(mirrorStreamDirPath, filesInMirroredStream, mirroredFs);
  		Iterator it = filesInMirroredStream.iterator();
  		while (it.hasNext() ) {
  			LOG.debug(" files in mirrored stream: " + (it.next()));
  		}
  		System.out.println("stream name:" + streamName);
  		compareMergedAndMirror(filesInMergedStream, filesInMirroredStream, 
  				mirrorStreamDirs.get(i).toString(), mergedStreamDirPath.
  				toString(), inconsistentData);
  	}
  }
  
  /**
   * It compares the merged stream and mirror streams 
   * stores the missed paths and data replay paths in the inconsistent data List
   * @param mergedStreamFiles : list of files in the merged stream
   * @param mirrorStreamFiles : list of files in the mirrored stream
   * @param mirrorStreamDirPath: mirror stream dir path for finding   
   * 				minute dirs paths only
   * @param mergedStreamDirPath : merged stream dir path for finding 
   * 			  minute dirs paths only
   * @param inconsistentData : stores all the missed paths and data replay paths
   */
  void compareMergedAndMirror(List<Path> mergedStreamFiles, 
  		List<Path> mirrorStreamFiles, String mirrorStreamDirPath, 
  		String mergedStreamDirPath, List<Path> inconsistentData) {
  	int i;
  	int j;
  	int mergedStreamLen = mergedStreamDirPath.length();
  	int mirrorStreamLen = mirrorStreamDirPath.length();
  	for( i=0, j=0 ; i < mergedStreamFiles.size() && 
  			j < mirrorStreamFiles.size(); i++, j++) {
  		String mergedStreamfilePath = mergedStreamFiles.get(i).toString().
  				substring(mergedStreamLen);
  		String mirrorStreamfilePath = mirrorStreamFiles.get(j).toString().
  				substring(mirrorStreamLen);
  		if(!mergedStreamfilePath.equals(mirrorStreamfilePath)) {
  			if(mergedStreamfilePath.compareTo(mirrorStreamfilePath) < 0) {
  				System.out.println("Missing file path : " + mergedStreamFiles.get(i));
  				inconsistentData.add(mergedStreamFiles.get(i));
  				--j;
  			} else {
  				System.out.println("Data Replica : " + mirrorStreamFiles.get(j));
  				inconsistentData.add(mirrorStreamFiles.get(j));
  				--i;
  			}
  		} else {
  			// System.out.println("match between   " + i + " and  " + j);
  		}	   
  	}	
  	if((i == j) && i== mergedStreamFiles.size() && j == mirrorStreamFiles.
  			size()) {
  		System.out.println("There are no missing paths");
  	} else {
  		/* check whether there are any missing file paths or extra dummy files  
  		 * or not
  		 */
  		if(i == mergedStreamFiles.size() ) {
  			for(;j < mirrorStreamFiles.size(); j++) {
  				System.out.println("Extra files are in the Mirrored Stream: " + 
  						mirrorStreamFiles.get(j));	
  				inconsistentData.add(mirrorStreamFiles.get(j));
  			}
  		} else {
  			for( ; i < mergedStreamFiles.size(); i++) {
  				System.out.println("To be Mirrored files: " + mergedStreamFiles.get(i));	
  				inconsistentData.add(mergedStreamFiles.get(i));
  			}
  		}
  	} 
  }

  /**
   * It lists all the dirs and file paths which are presented in 
   * the stream directory
   * @param dir : stream directory path 
   * @param listoffiles : stores all the files and directories 
   * @param fs : FileSystem object
   */
  public void doRecursiveListing(Path dir, List<Path> listOfFiles, 
  		FileSystem fs) throws IOException {
  	FileStatus[] fileStatuses = fs.listStatus(dir);
  	if (fileStatuses == null || fileStatuses.length == 0) {
  		LOG.debug("No files in directory:" + dir);
  		listOfFiles.add(dir);
  	} else {
  		for (FileStatus file : fileStatuses) { 
  			if (file.isDir()) {
  				doRecursiveListing(file.getPath(), listOfFiles, fs);
  			} else {
  				listOfFiles.add(file.getPath());
  			}
  		} 
  	}
  	Collections.sort(listOfFiles);
  }
  
  public List<Path> run(String [] args) throws Exception {
  	List<String> streamNames = new ArrayList<String>();
  	List<Path> inconsistentData = new ArrayList<Path>();
  	if (args.length == 2) {
  		FileSystem fs = mirrorStreamDirs.get(0).getFileSystem(new Configuration());
  		FileStatus[] fileStatuses = fs.listStatus(mirrorStreamDirs.get(0));
  		if (fileStatuses.length != 0) {
  			for (FileStatus file : fileStatuses) {  
  				streamNames.add(file.getPath().getName());
  			} 
  		} else {
  			System.out.println("There are no stream names in the mirrored stream");
  		}
  	} else if (args.length == 3) {
  		for (String streamname : args[2].split(",")) {
  			streamNames.add(streamname);
  		}
  	} 
  	for (String streamName : streamNames) {
  		inconsistentData.addAll(this.processListingStreams(streamName));
  	}
  	if (inconsistentData.isEmpty()) {
  		System.out.println("there is no inconsistency data");
  	}
  	return inconsistentData;
  }

  public static void main(String args[]) throws Exception {
  	if (args.length >= 2) {
  		String mergedStreamUrl = args[0];
  		String mirrorStreamUrls = args[1];	
  		MirrorStreamDataConsistencyValidation obj = new 
  				MirrorStreamDataConsistencyValidation(mirrorStreamUrls, 
  						mergedStreamUrl);
  		obj.run(args);
  	} else {
  		System.out.println("Enter the arguments" + " 1st arg :MergedStream Path" + 
  				"2nd arg: " + "Set of Mirrored stream paths" + "3rd arg: Set of " +
  				"stream names");
  		System.exit(1);
  	}
  }
}
