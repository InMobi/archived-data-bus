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

public class MirrorStreamDataConsistencyValidation {
  
  private static final Log LOG = LogFactory.getLog(
  		MirrorStreamDataConsistencyValidation.class);
  List<Path> mirrorStreamDirs;
  List<Path> filesInMergedStream;
  Path mergedStreamDirectoryPath;
  List<Path> inconsistentData = new ArrayList<Path>();
  
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
	//filesInMergedStream = new ArrayList<Path>();
    mergedStreamDirectoryPath = new Path(mergedStreamUrl, "streams");
   // LOG.info("mergedStreamDirecotry path is"+ mergedStreamDirectoryPath);
  }
  
  public List<Path> processListingStreams( String streamName) throws 
  		IOException {
	 mergedStreamListing(streamName);
	 mirrorStreamListing(streamName);
	 Iterator it = inconsistentData.iterator();
		while (it.hasNext() ) {
			it.next();
		  //System.out.println(" files in merged stream    "+(it.next()));
		}
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
				mergedFS);             //listing all the files and stores 
																								// it in filesInMergedStream
		//LOG.info("Listing all files in the mergedStream directory properly");
		Iterator it = filesInMergedStream.iterator();
		while (it.hasNext() ) {
			it.next();
		  //System.out.println(" files in merged stream    "+(it.next()));
		}
    //LOG.info("merged Stream file at 1st index " + filesInMergedStream.get(filesInMergedStream.size()-1).getParent());
	}

  public void mirrorStreamListing(String streamName) throws IOException {
		Path mirrorStreamDirPath;
		FileSystem mirroredFs;
		List<Path> filesInMirroredStream = new ArrayList<Path>();

		for (int i = 0; i < mirrorStreamDirs.size(); i++) {
		  mirrorStreamDirPath = new Path(mirrorStreamDirs.get(i), streamName);
		  mirroredFs = mirrorStreamDirPath.getFileSystem(new Configuration());
		  LOG.info(" mirroredStream Path"+ mirrorStreamDirPath);
		  doRecursiveListing(mirrorStreamDirPath, filesInMirroredStream, mirroredFs);
		  //LOG.info("mirror stream last file   : "  filesInMirroredStream.get(filesInMirroredStream.size()-1));
		  Iterator it = filesInMirroredStream.iterator();
		  while (it.hasNext() ) {
		  	it.next();
		    //LOG.info(" files in mirrored stream    "+(it.next()));
		  }
		  compareMergedAndMirror(filesInMergedStream, filesInMirroredStream);
	  }
  }
  
  /* Compare The merged Stream and Mirrored Streams and display if any missing paths */
  void compareMergedAndMirror(List<Path> mergedStreamFiles, 
  		List<Path> mirrorStreamFiles) {
		int i;
		int j;
		for( i=0, j=0 ; i < mergedStreamFiles.size() && 
					j < mirrorStreamFiles.size(); i++ , j++) {
			  LOG.info("Merged " + i + "   " + mergedStreamFiles.get(i).toUri()
			  		.getPath());
			  LOG.info("Mirrored " + j + "   "+ mirrorStreamFiles.get(j).toUri()
			  		.getPath() );
			  if(!mergedStreamFiles.get(i).toUri().getPath().equals(mirrorStreamFiles
			  		.get(j).toUri().getPath())) {
			  	if(mergedStreamFiles.get(i).toUri().getPath().compareTo((
							 mirrorStreamFiles.get(j).toUri().getPath())) < 0) {
				     //  LOG.info("Missing file path is"+ mergedStreamFiles.get(i));
				     inconsistentData.add(mergedStreamFiles.get(i));
				     --j;
				  } else {
				    LOG.info("Data Replica (duplicate)"+j +"     " + 
				    	mirrorStreamFiles.get(j));
					  --i;
				  }
				} else {
				 //      System.out.println("match   " + i + "    " + j);
				}	  
		 }	
		 if((i == j) && i== mergedStreamFiles.size()) {
			 LOG.info("There are no missing paths");
		 }
			
			/* check whether any missing file paths or extra dummy files are there or not*/
		 if(mirrorStreamFiles.size()!= mergedStreamFiles.size()) {
	   	  if(i == mergedStreamFiles.size() ) {
	        for(;j < mirrorStreamFiles.size(); j++) {
	        	LOG.info("Extra files are in the Mirrored Stream" + 
	        	mirrorStreamFiles.get(j));	 
	        }
	   	  } else {
	   	  	for( ; i <  mergedStreamFiles.size()  ; i++) {
			  //LOG.info("Missing Files " + mergedStreamFiles.get(i));	
	   	  	}
	   	  }
		 }
		
	}
  
  
   public void doRecursiveListing(Path dir, List<Path> listOfFiles , 
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
    // LOG.info("listof files" + listOfFiles.size());
   }
  
   
  public static void main(String args[]) throws Exception {
   
	String mergedStreamUrl;
	String mirrorStreamUrls;										//List<String> streamName = new ArrayList<String>();
	if(args.length!=3) {
		LOG.info("Enter the arguments" + " 1st arg :MergedStream Path" + "2nd arg: " +
				"Stream Name" + "3rd arg: Set of Mirrored stream paths ");
	} else {
	  mergedStreamUrl  = args[0];
      mirrorStreamUrls = args[2];
	  MirrorStreamDataConsistencyValidation obj=new 
	  		MirrorStreamDataConsistencyValidation(mirrorStreamUrls, mergedStreamUrl);
	  /*for(String streamNamesplit : args[1].split(",")) {
		obj.processListingStreams(streamNamesplit);
	  }*/
	  String streamNamesplits[]=args[1].split(",") ;
	  for(String streamName : streamNamesplits) {
		  obj.processListingStreams(streamName);
	  }
	}
  }
}
