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
 * @author rajubairishetti
 *
 */
public class MirrorStreamDataConsistencyValidation {
  private static final Log LOG = LogFactory.getLog(
  		MirrorStreamDataConsistencyValidation.class);
  List<Path> mirrorStreamDirs;
  List<Path> filesInMergedStream;
  Path mergedStreamDirectoryPath;
  
  public MirrorStreamDataConsistencyValidation(String mirrorStreamUrls, 
  		String mergedStreamUrl) throws IOException{
    String[] rootDirSplits;
		if (mirrorStreamUrls != null) {
		  rootDirSplits = mirrorStreamUrls.split(",");
		} else {
		  throw new IllegalArgumentException("Databus root directory not specified");
		}
		mirrorStreamDirs = new ArrayList<Path>(rootDirSplits.length);
		for (String mirrorRootDir : rootDirSplits) {
			mirrorStreamDirs.add(new Path(mirrorRootDir, "streams"));
		}
		mergedStreamDirectoryPath = new Path(mergedStreamUrl, "streams");
	}
  
  public List<Path> processListingStreams(String streamName) throws 
  		IOException {
  	List<Path> inconsistentData = new ArrayList<Path>();
 	  mergedStreamListing(streamName);
 	  mirrorStreamListing(streamName, inconsistentData);
 	  return inconsistentData;
  }
  
  public void mergedStreamListing(String streamName) throws IOException {
  	Path completeMergedStreamDirPath =new Path(mergedStreamDirectoryPath,
	    		streamName);
		LOG.info("paths:   " + completeMergedStreamDirPath);    
		FileSystem mergedFS = completeMergedStreamDirPath.getFileSystem(
				new Configuration());
		filesInMergedStream = new ArrayList<Path>();
		doRecursiveListing(completeMergedStreamDirPath, filesInMergedStream,
				mergedFS);             
		Iterator it = filesInMergedStream.iterator();
		while (it.hasNext() ) {
			//it.next();
		  System.out.println(" files in merged stream    "+(it.next()));
		}
  }
  
  public void mirrorStreamListing(String streamName, List<Path> 
  		inconsistentData) throws IOException {
		Path mirrorStreamDirPath;
		FileSystem mirroredFs;
		for (int i = 0; i < mirrorStreamDirs.size(); i++) {
			List<Path> filesInMirroredStream = new ArrayList<Path>();
		  mirrorStreamDirPath = new Path(mirrorStreamDirs.get(i), streamName);
		  mirroredFs = mirrorStreamDirPath.getFileSystem(new Configuration());
		  LOG.info(" mirroredStream Path"+ mirrorStreamDirPath);
		  doRecursiveListing(mirrorStreamDirPath, filesInMirroredStream, mirroredFs);
		  Iterator it = filesInMirroredStream.iterator();
		  while (it.hasNext() ) {
		  	//it.next();
		    System.out.println(" files in mirrored stream    "+(it.next()));
		  }
		  compareMergedAndMirror(filesInMergedStream, filesInMirroredStream, 
		  		mirrorStreamDirs.get(i).toString(), mergedStreamDirectoryPath.
		  				toString(), inconsistentData);
	  }
  }
  
  /**
   * It compares the merged stream and mirror streams 
   * stores the missed paths and data replay paths in the inconsistent data List.
   * @param mergedStreamFiles : list of files in the merged stream
   * @param mirrorStreamFiles : list of files in the mirrored stream
   * @param mirrorStreamDirPath: mirror stream dir path for finding   
   * 				minute dirs paths only
   * @param mergedStreamDirectoryPath : merged stream dir path for finding 
   * 			  minute dirs paths only
   * @param inconsistentData : stores all the missed paths and data replay paths
   */
  void compareMergedAndMirror(List<Path> mergedStreamFiles, 
  		List<Path> mirrorStreamFiles, String mirrorStreamDirPath, 
  				String mergedStreamDirectoryPath, List<Path> inconsistentData) {
		int i;
		int j;
		for( i=0, j=0 ; i < mergedStreamFiles.size() && 
					j < mirrorStreamFiles.size(); i++ , j++) {
			int mergedStreamLen = mergedStreamDirectoryPath.length();
		  int mirrorStreamLength = mirrorStreamDirPath.length();
		  String mergedStreamfilePath = mergedStreamFiles.get(i).toString().
		  		substring(mergedStreamLen);
		  String mirrorStreamfilePath = mirrorStreamFiles.get(j).toString().
		  		substring(mirrorStreamLength);
		  if(!mergedStreamfilePath.equals(mirrorStreamfilePath)) {
		  	if(mergedStreamfilePath.compareTo(mirrorStreamfilePath) < 0) {
		  		LOG.info("Missing file path : " + mergedStreamFiles.get(i));
			    inconsistentData.add(mergedStreamFiles.get(i));
			    --j;
			  } else {
			    LOG.info("Data Replica : " + mirrorStreamFiles.get(j));
			    inconsistentData.add(mirrorStreamFiles.get(j));
				  --i;
			  }
			} else {
			       // System.out.println("match between   " + i + " and  " + j);
			}	   
		}	
		if((i == j) && i== mergedStreamFiles.size()) {
			 LOG.info("There are no missing paths");
		 } else {
			 /* check whether there are any missing file paths or extra dummy files  
			  * or not
			  */
			 if(i == mergedStreamFiles.size() ) {
	        for(;j < mirrorStreamFiles.size(); j++) {
	        	LOG.info("Extra files are in the Mirrored Stream" + 
	        	mirrorStreamFiles.get(j));	
	        	inconsistentData.add(mirrorStreamFiles.get(j));
	        }
	   	 } else {
	   	   for( ; i <  mergedStreamFiles.size()  ; i++) {
	   	  	 LOG.info("Missing File path " + mergedStreamFiles.get(i));	
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
    } else {
      for (FileStatus file : fileStatuses) { 
		   	if (file.isDir()) {
		   	  doRecursiveListing(file.getPath(), listOfFiles, fs);
		  	} 
 	      listOfFiles.add(file.getPath());
      } 
    }
    Collections.sort(listOfFiles);
  }
  
  public static void main(String args[]) throws Exception {
   
		String mergedStreamUrl;
		String mirrorStreamUrls;										
		if(args.length!=3) {
			LOG.info("Enter the arguments" + " 1st arg :MergedStream Path" + 
		"2nd arg: " + "Stream Name" + "3rd arg: Set of Mirrored stream paths ");
		} else {
		  mergedStreamUrl  = args[0];
	    mirrorStreamUrls = args[2];
		  MirrorStreamDataConsistencyValidation obj=new 
		  		MirrorStreamDataConsistencyValidation(mirrorStreamUrls, 
		  				mergedStreamUrl);
		  String streamNames[]=args[1].split(",") ;
		  for(String streamName : streamNames) {
			  obj.processListingStreams(streamName);
		  }
		}
	}
}