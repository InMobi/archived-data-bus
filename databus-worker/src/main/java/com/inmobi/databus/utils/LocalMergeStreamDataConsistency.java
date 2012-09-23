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
		LOG.info("localstream file is : " +localStreamFiles.get(0).getName());
		for ( i = 0, j = 0; i < localStreamFiles.size() && 
				j < mergedStreamFiles.size(); i++, j++) {

			if (!localStreamFiles.get(i).getName().equals(mergedStreamFiles.
					get(j).getName())) {
				if (localStreamFiles.get(i).getName().compareTo(
						mergedStreamFiles.get(j).getName()) < 0) {
					System.out.println("missing path:  " + localStreamFiles.get(i));
					--j;
				} else {
					System.out.println("data replay:  " + mergedStreamFiles.get(j));
					--i;
				}
			} else {
				//LOG.info("match between " + "local" + i + " " + " merged" + j);
			}
		}
		if (i == j && i == localStreamFiles.size() && 
				j == mergedStreamFiles.size()) {
			System.out.println("there are no missing files");
		} else {
			if (i == localStreamFiles.size()) {
				for ( ; j < mergedStreamFiles.size(); j++ ) {
					System.out.println("extra files in merged stream " + 
							mergedStreamFiles.get(j));
				}
			} else {
				System.out.println("missing files are: " + localStreamFiles.get(i));
			}
		}
	}

	public void processing(String mergedStreamRoorDir, String[] 
			localStreamrootDirs, List<String> streamNames) throws Exception {
		Path streamDir;
		FileSystem fs;
		List<Path> localStreamFiles;
		List<Path> mergedStreamFiles;
		Iterator<String> it = streamNames.iterator();
		String streamName;
		while (it.hasNext()) {
			streamName = it.next();
			localStreamFiles = new ArrayList<Path>();
			mergedStreamFiles = new ArrayList<Path>();
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
	}
 
	public static void main(String [] args) throws Exception {
		
		
		 if (args.length >= 2) {
  		String [] localStreamrootDirs = args[0].split(",");
			String mergedStreamRoorDir = args[1];	
  		List<String> streamNames = new ArrayList<String>();
  		LocalMergeStreamDataConsistency obj = new LocalMergeStreamDataConsistency();
  		if (args.length == 3) {
  			for (String streamname : args[2].split(",")) {
  				streamNames.add(streamname);
  			}
  		} else if (args.length == 2) {
  			FileSystem fs = new Path(mergedStreamRoorDir, "streams").
  					getFileSystem(new Configuration());
  			FileStatus[] fileStatuses = fs.listStatus(new Path(mergedStreamRoorDir,
  					"streams"));
  			if (fileStatuses.length != 0) {
  				for (FileStatus file : fileStatuses) {  
  					streamNames.add(file.getPath().getName());
  				} 
  			} else {
  				System.out.println("There are no streams in the merged stream ");
  				System.exit(0);
  			}
  		}
  		obj.processing(mergedStreamRoorDir, localStreamrootDirs, streamNames);
  	} else {
  		System.out.println("Enter the arguments" + " 1st arg :MergedStream Path" + 
  				"2nd arg: " + "Set of Mirrored stream paths" + "3rd arg: Set of " +
  				"stream names");
  		System.exit(1);
  	}
		 
	}
}