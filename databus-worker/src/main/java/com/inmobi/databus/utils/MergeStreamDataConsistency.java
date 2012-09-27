package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * this class checks the data consistency between local vs merge streams.
 * The main method takes 3 arguments. Local stream urls as first argument, merge
 *  stream url as second argument and third argument is set of stream names. 
 *  Third argument is optional here  
 *
 */
public class MergeStreamDataConsistency {
	private static final Log LOG = LogFactory.getLog(
			MergeStreamDataConsistency.class);
	
	public List<Path> compareLocalMergeStreams(TreeMap<String, Path> 
			localStreamFiles, TreeMap<String, Path> mergedStreamFiles, List<Path> 
					inconsistency) {
		Set<Entry<String, Path>> localStreamFileEntries = localStreamFiles.
				entrySet();
		Set<Entry<String, Path>> mergedStreamFileEntries = mergedStreamFiles.
				entrySet();
		Iterator<Entry<String, Path>> localIt = localStreamFileEntries.iterator();
		Iterator<Entry<String, Path>> mergedIt = mergedStreamFileEntries.iterator();
		String localKey = null;
		String mergedKey = null;
		if (localIt.hasNext()) {
			localKey = localIt.next().getKey(); 
		}
		if (mergedIt.hasNext()) {
			mergedKey = mergedIt.next().getKey();
		}
		while (localIt.hasNext() && mergedIt.hasNext()) {
			if (!localKey.equals(mergedKey)) {
				if(localKey.compareTo(mergedKey) < 0) {
					System.out.println("missing path: " + localStreamFiles.get(localKey));
					inconsistency.add(localStreamFiles.get(localKey));
					localKey = localIt.next().getKey(); 
				} else {
					System.out.println("data replay: " + mergedStreamFiles.get(mergedKey));
					inconsistency.add(mergedStreamFiles.get(mergedKey));
					mergedKey = mergedIt.next().getKey(); 
				}
			} else {
				localKey = localIt.next().getKey(); 
				mergedKey = mergedIt.next().getKey();
			}
		}
		if ((!localIt.hasNext()) && (!mergedIt.hasNext()) && (localStreamFiles.
				size() == mergedStreamFiles.size())) {
			System.out.println("there are no missing files");
		} else {
			if (!mergedIt.hasNext()) {
				while (localIt.hasNext()) {
					localKey = localIt.next().getKey();
					inconsistency.add(localStreamFiles.get(localKey));
					System.out.println("To be merged files: " + 
							localStreamFiles.get(localKey));
				}
			} else {
				while (mergedIt.hasNext()) {
					mergedKey = mergedIt.next().getKey();
					inconsistency.add(mergedStreamFiles.get(mergedKey));
					System.out.println("extra files in merged stream: " + 
							mergedStreamFiles.get(mergedKey));
				}
			}
		}
		return inconsistency;
	}

	public List<Path> listingValidation(String mergedStreamRoorDir, String[] 
			localStreamrootDirs, List<String> streamNames) throws Exception {
		Path streamDir;
		FileSystem fs;
		TreeMap<String, Path> localStreamFiles;
		TreeMap<String, Path> mergedStreamFiles;
		List<Path> inconsistency = new ArrayList<Path>();
		for (String streamName : streamNames) {
			localStreamFiles = new TreeMap<String, Path>();
			mergedStreamFiles = new TreeMap<String, Path>();
			for (String localStreamRootDir : localStreamrootDirs) {
				streamDir = new Path(new Path(localStreamRootDir, "streams_local"),
						streamName);
				fs = streamDir.getFileSystem(new Configuration());
				doRecursiveListing(streamDir, localStreamFiles, fs);
			}
			streamDir = new Path(new Path(mergedStreamRoorDir, "streams"), streamName);
			fs = streamDir.getFileSystem(new Configuration());
			doRecursiveListing(streamDir, mergedStreamFiles, fs);
			System.out.println("stream name: " + streamName);
			compareLocalMergeStreams(localStreamFiles, mergedStreamFiles, 
					inconsistency);
		}
		return inconsistency;
	}
	
	public void doRecursiveListing(Path streamDir, TreeMap<String, Path> 
			listOfFiles, FileSystem fs) throws IOException {
		FileStatus[] fileStatuses = fs.listStatus(streamDir);
		if (fileStatuses == null || fileStatuses.length == 0) {
			LOG.debug("No files in directory:" + streamDir);
		} else {
			for (FileStatus file : fileStatuses) { 
				if (file.isDir()) {
					doRecursiveListing(file.getPath(), listOfFiles, fs);
				} else { 
					listOfFiles.put(file.getPath().getName(), file.getPath());
				}
			} 
		}
	}
	
	public List<Path> run(String [] args) throws Exception {
		List<Path> inconsistencydata = new ArrayList<Path>();
		String [] localStreamrootDirs = args[0].split(",");
		String mergedStreamRoorDir = args[1];	
		List<String> streamNames = new ArrayList<String>();
		if (args.length == 2) {
			FileSystem fs = new Path(mergedStreamRoorDir, "streams").getFileSystem(
					new Configuration());
			FileStatus[] fileStatuses = fs.listStatus(new Path(mergedStreamRoorDir, 
					"streams"));
			if (fileStatuses.length != 0) {
				for (FileStatus file : fileStatuses) {  
					streamNames.add(file.getPath().getName());
				} 
			} else {
				System.out.println("There are no stream names in the stream");
			}
		} else if (args.length == 3) {
			for (String streamname : args[2].split(",")) {
				streamNames.add(streamname);
			}
		} 
		inconsistencydata = this.listingValidation(mergedStreamRoorDir, 
				localStreamrootDirs, streamNames);
		if (inconsistencydata.isEmpty()) {
			System.out.println("there is no inconsistency data");
		}
		return inconsistencydata;
	}

	public static void main(String [] args) throws Exception {
		MergeStreamDataConsistency obj = new MergeStreamDataConsistency();
		if (args.length >= 2) {
			obj.run(args);
		} else {
			System.out.println("Enter the arguments" + "1st arg: Set of local stream" 
					+	"urls" + "2nd arg: MergedStream url" + "3rd arg: Set of " +
					"stream names" + "stream names are optional");
			System.exit(1);
		}
	}
}