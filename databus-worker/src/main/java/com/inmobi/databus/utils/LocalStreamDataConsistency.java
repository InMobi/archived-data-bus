package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.sound.midi.SysexMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class is used to check the data consistency between data(data + trash) 
 * and streams_local. It takes 3 arguments. 1st one is set of root directories,
 * second argument is set of stream names and 3rd argument is set of collector 
 * names. Second and third arguments are optional here.
 *
 */
public class LocalStreamDataConsistency {

	private static final Log LOG = LogFactory.getLog(
			LocalStreamDataConsistency.class);
	
	public LocalStreamDataConsistency() {
		
	}
	
	/**
	 * This method is used to compare the data between the data and streams_local.
	 * @param listOfDataTrashFiles : list of all files (data + trash)
	 * @param listOfLocalFiles     : list of streams_local files
	 */
	public List<Path> compareDataLocalStreams(TreeMap<String, Path> 
			listOfDataTrashFiles, TreeMap<String, Path> listOfLocalFiles, List<Path> 
					inconsistency) {
		Set<Entry<String, Path>> dataTrashFileEntries = listOfDataTrashFiles.
				entrySet();
		Set<Entry<String, Path>> localFileEntries = listOfLocalFiles.
				entrySet();
		Iterator<Entry<String, Path>> dataTrashIt = dataTrashFileEntries.iterator();
		Iterator<Entry<String, Path>> localIt = localFileEntries.iterator();
		String dataTrashKey = null;
		String localKey = null;
		if (dataTrashIt.hasNext()) {
			dataTrashKey = dataTrashIt.next().getKey(); 
		}
		if (localIt.hasNext()) {
			localKey = localIt.next().getKey();
		}
		while (dataTrashKey!= null && localKey!= null) {
			LOG.debug("comparision : " + dataTrashKey + " " + localKey);
			if (!dataTrashKey.equals(localKey)) {
				if(dataTrashKey.compareTo(localKey) < 0) {
					System.out.println("missing path: " + listOfDataTrashFiles.get(dataTrashKey));
					inconsistency.add(listOfDataTrashFiles.get(dataTrashKey));
					if (dataTrashIt.hasNext()) {
						dataTrashKey = dataTrashIt.next().getKey(); 
					} else {
						dataTrashKey = null;
					}
				} else {
					System.out.println("data replay: " + listOfLocalFiles.get(localKey));
					inconsistency.add(listOfLocalFiles.get(localKey));
					if (localIt.hasNext()) {
						localKey = localIt.next().getKey();
					} else {
						localKey = null;
					}
				}
			} else { 
				if (dataTrashIt.hasNext() && localIt.hasNext()) {
					dataTrashKey = dataTrashIt.next().getKey(); 
					localKey = localIt.next().getKey();
				} else {
					localKey = null;
					dataTrashKey = null;
				} 
			}
		}
		if ((!dataTrashIt.hasNext()) && (!localIt.hasNext()) && (listOfDataTrashFiles.
				size() == listOfLocalFiles.size())) {
			System.out.println("there is no inconsitent data");
		} else {
			if (!localIt.hasNext()) {
				while (dataTrashIt.hasNext()) {
					dataTrashKey = dataTrashIt.next().getKey();
					inconsistency.add(listOfDataTrashFiles.get(dataTrashKey));
					System.out.println("Files to be sent: " + 
							listOfDataTrashFiles.get(dataTrashKey));
				}
			} else {
				while (localIt.hasNext()) {
					localKey = localIt.next().getKey();
					inconsistency.add(listOfLocalFiles.get(localKey));
					System.out.println("extra files in streams_local: " + 
							listOfLocalFiles.get(localKey));
				}
			}
		}
		return inconsistency;
	}
	
	public void doRecursiveListing(Path pathName, TreeMap<String, Path> listOfFiles, 
			FileSystem fs, String baseDir, String streamName) throws Exception {
		FileStatus [] fileStatuses = fs.listStatus(pathName);
		if (fileStatuses == null || fileStatuses.length == 0) {
			LOG.debug("No files in directory:" + pathName);
		} else {
			for (FileStatus file : fileStatuses) {
				if (file.isDir()) {
					doRecursiveListing(file.getPath(), listOfFiles, fs, baseDir, streamName);
				} else {
					if (baseDir.equals("data")) {
						// check for meta files (scribe_stats.gz file and *current.gz file) 
						 // and ignore them 
						if (!((file.getPath().getName() + ".gz").equals(streamName + 
								"_current.gz") || (file.getPath().getName() + ".gz").equals
										("scribe_stats.gz"))) {
							String filename = file.getPath().getParent().getName() + "_" + 
									file.getPath().getName() + ".gz";
							listOfFiles.put(filename, file.getPath());
							LOG.debug("data files: " + filename);
						}
					} else if (baseDir.equals("trash")){
						listOfFiles.put(file.getPath().getName(), file.getPath());
						LOG.debug("trash files" + file.getPath().getName());
					} else {
						listOfFiles.put(file.getPath().getName(), file.getPath());
						LOG.debug("local files are:" + file.getPath().getName());
					}
				}
			}
		}
}
	
	public void getStreamNames(List<String> streamNames, String pathName) throws 
			Exception{
		for (String streamName : pathName.split(",")) {
			streamNames.add(streamName);
		}
	}
	
	public void getStreamCollectorNames( Path streamDir, List<String>  
			streamCollectorNames) throws Exception {
		FileSystem fs = streamDir.getFileSystem(new Configuration());
		FileStatus [] filestatuses = fs.listStatus(streamDir);
		for (FileStatus file : filestatuses) {
			streamCollectorNames.add(file.getPath().getName());
		}
	}
	
	public FileSystem getFs(Path pathName) throws Exception {
		FileSystem fs = pathName.getFileSystem(new Configuration());
		return fs;
	}

	public void listingAllPaths(String rootDir, String streamName, String 
			collectorName, TreeMap<String, Path> listOfDataTrashFiles, TreeMap<String, Path> 
					listOfLocalFiles) throws Exception {
		Path pathName;
		FileSystem fs;
		pathName = new Path(new Path(new Path (rootDir, "data"), streamName), 
				collectorName); 
		fs = getFs(pathName);
		doRecursiveListing(pathName, listOfDataTrashFiles, fs, "data", streamName);
		pathName = new Path(new Path(rootDir, "streams_local"), streamName);
		fs = getFs(pathName);
		doRecursiveListing(pathName, listOfLocalFiles, fs, "streams_local", streamName);
	}

	public void processing(String rootDir, String streamName, List<String> 
			collectorNames, TreeMap<String, Path> listOfDataTrashFiles, TreeMap<String, Path>
					listOfLocalFiles) throws Exception {
		for (String collectorName : collectorNames) {
			listingAllPaths(rootDir, streamName, collectorName, listOfDataTrashFiles,
					listOfLocalFiles);
		}
	}
	
	public void processAllStreams(List<String> streamNames, List<String> 
			collectorNames, Path baseDir, TreeMap<String, Path> listOfDataTrashFiles, 
					TreeMap<String, Path> listOfLocalFiles, List<Path> inconsistentData, 
							String rootDir) throws Exception {
		for (String streamName : streamNames) {
			collectorNames = new ArrayList<String>();
			getStreamCollectorNames(new Path(baseDir, streamName), collectorNames);
			processing(rootDir, streamName, collectorNames, listOfDataTrashFiles,
					listOfLocalFiles);
		}
		compareDataLocalStreams(listOfDataTrashFiles, listOfLocalFiles, 
				inconsistentData);
	}
	
	public void findTrashFiles(String rootDir, TreeMap<String, Path> 
			listOfDataTrashFiles) throws Exception {
		Path	pathName = new Path(new Path(rootDir, "system"), "trash");
		FileSystem	fs = getFs(pathName);
		doRecursiveListing(pathName, listOfDataTrashFiles, fs, "trash", null);
	}
	
	public void allocateMemory(TreeMap<String, Path> listOfDataTrashFiles, 
			TreeMap<String, Path> listOfLocalFiles) {
		listOfDataTrashFiles = new TreeMap<String, Path>();
		listOfLocalFiles = new TreeMap<String, Path>();
	}
	
	public List<Path> run(String [] args) throws Exception {
		String [] rootDirs = args[0].split(",");
		List<String> streamNames = new ArrayList<String>();
		List<String> collectorNames = new ArrayList<String>();
		TreeMap<String, Path> listOfDataTrashFiles = new TreeMap<String, Path>(); 
		TreeMap<String, Path> listOfLocalFiles = new TreeMap<String, Path>(); 
		List<Path> inconsistentData = new ArrayList<Path>();
		if (args.length == 1) {
			for (String rootDir : rootDirs) {
				Path baseDir = new Path(rootDir, "streams_local");
				streamNames = new ArrayList<String>();
				allocateMemory(listOfDataTrashFiles, listOfLocalFiles);
				findTrashFiles(rootDir, listOfDataTrashFiles);
				getStreamCollectorNames(baseDir, streamNames);
				baseDir = new Path(rootDir, "data");
				processAllStreams(streamNames, collectorNames, baseDir, 
						listOfDataTrashFiles, listOfLocalFiles, inconsistentData, rootDir);
			}
		} else if (args.length == 2) {
			getStreamNames(streamNames, args[1]);
			for (String rootDir : rootDirs) {
				allocateMemory(listOfDataTrashFiles, listOfLocalFiles);
				Path baseDir = new Path(rootDir, "data");
				findTrashFiles(rootDir, listOfDataTrashFiles);
				processAllStreams(streamNames, collectorNames, baseDir, 
						listOfDataTrashFiles, listOfLocalFiles, inconsistentData, rootDir);
			}
		} else if (args.length == 3) {
			getStreamNames(streamNames, args[1]);
			for (String collectorName : args[2].split(",")) {
				collectorNames.add(collectorName);
			}
			for (String rootDir : rootDirs) {
				findTrashFiles(rootDir, listOfDataTrashFiles);
				for (String streamName : streamNames ) {
					processing(rootDir, streamName, collectorNames, listOfDataTrashFiles,
							listOfLocalFiles);
				}
				compareDataLocalStreams(listOfDataTrashFiles, listOfLocalFiles, inconsistentData);
			}
		}
		return inconsistentData;
	}
 
	
	public static void main(String [] args) throws Exception {
		if (args.length >= 1) {
			LocalStreamDataConsistency obj = new LocalStreamDataConsistency();
			obj.run(args);
		} else {
			System.out.println("incorrect number of arguments:" + "Enter 1st arg: " +
					"rootdir url" + "2nd arg: set of stream names" + "3rd arg: set of " +
							"collector names" + "2nd and 3rd arguments are optional here");
			System.exit(1);
		}
	}
}