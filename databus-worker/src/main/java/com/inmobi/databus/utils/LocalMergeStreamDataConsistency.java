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

public class LocalMergeStreamDataConsistency {
	private static final Log LOG = LogFactory.getLog(
			LocalMergeStreamDataConsistency.class);
	
	public void compareLocalMergeStreams(List<Path> localStreamFiles, List<Path> 
			mergedStreamFiles) {
		 int i;
		 int j;
		 Collections.sort(localStreamFiles);
		 Collections.sort(mergedStreamFiles);
		 LOG.info("localstream file is    : " +localStreamFiles.get(0).getName());
		 for ( i = 0, j = 0; (i < localStreamFiles.size()-1) && 
				 (j < mergedStreamFiles.size()-1 ); i++, j++) {
			 
			 if (!localStreamFiles.get(i).getName().equals(mergedStreamFiles.
					 get(j).getName())) {
				 if (localStreamFiles.get(i).getName().compareTo(
						 mergedStreamFiles.get(j).getName()) < 0) {
					 LOG.info("missing path:  " + localStreamFiles.get(i));
					 --j;
				 } else {
					 LOG.info("data replay:  " + mergedStreamFiles.get(j));
					 --i;
				 }
			 } else {
				 LOG.info("match between " + "local" + i + " merged" + j);
			 }
		 }
		 if (i == j && i == localStreamFiles.size()) {
			 LOG.info("there are no missing files");
		 }
		 if (localStreamFiles.size() != mergedStreamFiles.size()) {
			 if (i == localStreamFiles.size()) {
				 for ( ; j < mergedStreamFiles.size(); j++ ) {
					 LOG.info(" extra files in merged stream " + mergedStreamFiles.get(j));
				 }
			 } else {
				 LOG.info("missing files are: " + localStreamFiles.get(i));
			 }
		 }
	}
	
	public void processing(String mergedStreamRoorDir, String[] 
			localStreamrootDirs, String[] streamNames) throws Exception {
		Path streamDir;
		FileSystem fs;
		for (String streamName : streamNames) {
			List<Path> localStreamFiles = new ArrayList<Path>();
			List<Path> mergedStreamFiles = new ArrayList<Path>();
			for (String localStreamRootDir : localStreamrootDirs) {
				streamDir = new Path(new Path(localStreamRootDir, "streams_local"),
						streamName);
				fs = streamDir.getFileSystem(new Configuration());
				doRecursiveListing(streamDir, localStreamFiles, fs);
			}
			streamDir = new Path(new Path(mergedStreamRoorDir, "streams"), streamName);
			fs = streamDir.getFileSystem(new Configuration());
			doRecursiveListing(streamDir, mergedStreamFiles, fs);
			
			compareLocalMergeStreams(localStreamFiles, mergedStreamFiles);
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
		   	 } else { 
		 	     listOfFiles.add(file.getPath());
		   	 }
      } 
    }
   
   // LOG.info("listof files" + listOfFiles.size());
  }
 
	public static void main(String [] args) throws Exception {
		if (args.length ==3) {
			String [] localStreamrootDirs = args[0].split(",");
			String mergedStreamRoorDir = args[1];
			String [] streamNames = args[2].split(",");
			LocalMergeStreamDataConsistency obj = new LocalMergeStreamDataConsistency();
			obj.processing(mergedStreamRoorDir, localStreamrootDirs, streamNames);
		} else {
			System.out.println("incorrect number of arguments");
		}
	}
}
