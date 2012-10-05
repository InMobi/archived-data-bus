package com.inmobi.databus.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestLocalStreamDataConsistency {
	private static Logger LOG = Logger.getLogger(TestLocalStreamDataConsistency.
			class);
	private static String className = TestLocalStreamDataConsistency.class.
			getSimpleName();

	FileSystem fs;
	String rootDirUrl = ("file:///tmp/test/" + className + "/1/,file:///" +
			"tmp/test/" + className + "/2/");
	String streamNameUrl = "empty,consistnetData,missingData,dataReplayData," +
			"extraFilesData";
	String collectorNameUrl = "collector1";
	String [] rootDirs = rootDirUrl.split(",");
	List<String> emptyStreamName = new ArrayList<String>();
	List<String> missedFilesStreamName = new ArrayList<String>();
	List<String> consistentDataStreamName = new ArrayList<String>();
	List<String> dataReplayFilesStreamName = new ArrayList<String>();
	List<String> extrafilesStreamName = new ArrayList<String>();
	List<String> allStreamNames = new ArrayList<String>();
	List<String> trashFiles = new ArrayList<String>();
	List<String> collectorNames = new ArrayList<String>();
	List<Path> emptyPaths = new ArrayList<Path>();
	List<Path> missedFilePaths = new ArrayList<Path>();
	List<Path> dataReplayFilePaths = new ArrayList<Path>();
	List<Path> extraFilePaths = new ArrayList<Path>();

	long temptime = System.currentTimeMillis();

	@BeforeTest
	public void setUp() throws Exception {
		fs = FileSystem.getLocal(new Configuration());
		defineStreamNames(emptyStreamName, "empty");
		defineStreamNames(consistentDataStreamName, "consistentData");
		defineStreamNames(missedFilesStreamName, "missingFiles");
		defineStreamNames(dataReplayFilesStreamName, "dataReplayFiles");
		defineStreamNames(extrafilesStreamName, "extraFiles");
		defineStreamNames(allStreamNames, "consistentData");
		defineStreamNames(allStreamNames, "missingFiles");
		defineStreamNames(allStreamNames, "dataReplayFiles");
		defineStreamNames(allStreamNames, "extraFiles");
		for (String collectorName : collectorNameUrl.split(",")) {
			collectorNames.add(collectorName);
		}
		createtestdata(allStreamNames);
	}
	public void createtestdata(List<String> streamNames) throws Exception {
		createTestData("data", streamNames );
		createTestData("trash", streamNames);
		createTestData("streams_local", streamNames);
	}

	@AfterTest
	public void cleanup() throws Exception {
		fs.delete(new Path(rootDirs[0]).getParent(), true);
	}

	public void defineStreamNames(List<String> streamNames, String streamName) {
		streamNames.add(streamName);
	}
	
	public void createTestData(String streamType, List<String> streamNames) throws
			Exception {
		for (String rootDir : rootDirs) {
			for (String streamName : streamNames) {
				for (String collectorName : collectorNames) {
					createCompletePath(streamName, streamType, collectorName, rootDir);
				}
			}
		}
	}
	
	public void createCompletePath(String streamName, String dataType, String 
			collectorName, String rootDir) throws Exception {
		Path dataPath;
		Path dataCompletePath;
		if (dataType.equals("data")) {
			dataPath = new Path(rootDir, "data" );
			dataCompletePath = new Path(new Path(dataPath, streamName), collectorName);
			createFiles(dataCompletePath, "data", streamName, collectorName);
		} else if (dataType.equals("trash")) {
			dataPath = new Path(new Path(rootDir, "system"), "trash");
			dataCompletePath = new Path(dataPath, streamName);
			createFiles(dataCompletePath, "trash", streamName, collectorName);
		} else if (dataType.equals("streams_local")) {
			dataPath = new Path(rootDir, "streams_local");
			dataCompletePath = new Path(dataPath, streamName);
			createFiles(dataCompletePath, "streams_local", streamName, collectorName);
		}
	}
	
	public void createFiles(Path pathName, String streamType, String streamName, 
			String collectorName) throws Exception {
		String filePrefix = null;
		if (streamType.equals("data")) {
			filePrefix = streamName + "-";
			if (streamName.equals("empty")) {
			} else if (streamName.equals("consistentData")) { 
				createFilesData(fs, pathName, 3, 0, filePrefix, streamType);
			} else if (streamName.equals("missingFiles")) {
				createFilesData(fs, pathName, 5, 0, filePrefix, streamType);
				missedFilePaths.add(new Path(pathName, new Path(filePrefix + "file" + 4)));
			} else if (streamName.equals("dataReplayFiles")) {
				createFilesData(fs, pathName, 2, 0, filePrefix, streamType);
			} else if (streamName.equals("extraFiles")) {
				createFilesData(fs, pathName, 3, 0, filePrefix, streamType);
			}
		} else if (streamType.equals("trash")) {
			filePrefix = collectorName + "_" + streamName + "-";
			if (streamName.equals("empty")) {
			} else if (streamName.equals("consistentData")) {
				createFilesData(fs, pathName, 2, 3, filePrefix, streamType);
			} else if (streamName.equals("missingFiles")) {
				createFilesData(fs, pathName, 1, 5, filePrefix, streamType);
			} else if (streamName.equals("dataReplayFiles")) {
				createFilesData(fs, pathName, 2, 3, filePrefix, streamType);
			} else if (streamName.equals("extraFiles")) {
				createFilesData(fs, pathName, 2, 3, filePrefix, streamType);
			}
		} else if (streamType.equals("streams_local")) {
			filePrefix = collectorName + "_" + streamName + "-";
			if (streamName.equals("empty")) {
			} else if (streamName.equals("consistentData")) {
				createFilesData(fs, pathName, 5, 0, filePrefix, streamType);
			} else if (streamName.equals("missingFiles")) {
				createFilesData(fs, pathName, 4, 0, filePrefix, streamType);
				createFilesData(fs, pathName, 1, 5, filePrefix, streamType);				
			} else if (streamName.equals("dataReplayFiles")) {
				createFilesData(fs, pathName, 5, 0, filePrefix, streamType);
				dataReplayFilePaths.add(new Path(pathName, new Path(filePrefix + "file"
						+ 2 + ".gz")));
			} else if (streamName.equals("extraFiles")) {
				createFilesData(fs, pathName, 6, 0, filePrefix, streamType);
				extraFilePaths.add(new Path(pathName, new Path(filePrefix + "file" + 5 
						+ ".gz")));
			}
		}
	}
	
	public static List<String> createFilesData(FileSystem fs, Path pathName,
			int filesCount, int start, String filePrefix, String streamType) throws 
					Exception {
		fs.mkdirs(pathName);
		List<String> filesList = new ArrayList<String>();
		Path path;
		for (int j = start; j < filesCount + start; j++) {
			if (streamType.equals("trash") || streamType.equals("streams_local")) {
				filesList.add(filePrefix + "file" + j + ".gz");
			} else {
				filesList.add(filePrefix + "file" + j);
			}
			path= new Path(pathName, filesList.get(j-start));
			LOG.debug("Creating Test Data with filename [" + filesList.get(j-start) + "]");
			FSDataOutputStream streamout = fs.create(path);
			streamout.writeBytes("Creating Test data for teststream"
					+ filesList.get(j-start));
			streamout.close();
			Assert.assertTrue(fs.exists(path));
		}
		return filesList;
	}

  public void testLocalStreamData(List<String> streamNames, List<Path> 
  		expectedPaths, LocalStreamDataConsistency obj) throws Exception {
  	List<Path> inconsistentdata = new ArrayList<Path>();
  	TreeMap<String, Path> listOfDataTrashFiles;
  	TreeMap<String, Path> listOfLocalFiles;
  	for (String rootDir : rootDirs) {
  		listOfDataTrashFiles = new TreeMap<String, Path>();
  		listOfLocalFiles = new TreeMap<String, Path>();
  		inconsistentdata = new ArrayList<Path>();
  		Path pathName = new Path(new Path(rootDir, "system"), "trash");
  		obj.doRecursiveListing(pathName, listOfDataTrashFiles, fs, "trash", null);
  		for (String streamName : streamNames) {
  			obj.processing(rootDir, streamName, collectorNames, listOfDataTrashFiles,
  					listOfLocalFiles);
  		}
  		obj.compareDataLocalStreams(listOfDataTrashFiles, listOfLocalFiles, 
  				inconsistentdata);
  		Assert.assertEquals( 2 * inconsistentdata.size(), expectedPaths.size());
  	}
  }

  private void checkDataConsistency(List<Path> expectedStreamPaths, List<Path> 
  		inconsistentdata, String [] args, LocalStreamDataConsistency obj) 
  				throws Exception{
  	inconsistentdata = obj.run(args);
		Assert.assertEquals(inconsistentdata.size(), expectedStreamPaths.size());
		Assert.assertTrue(inconsistentdata.containsAll(expectedStreamPaths)); 
  }
  
	@Test
	public void testLocalStream() throws Exception {
		LocalStreamDataConsistency obj = new LocalStreamDataConsistency();
		LOG.info("all streams together");
		List<Path> allStreamPaths = new ArrayList<Path>();
		allStreamPaths.addAll(emptyPaths);
		allStreamPaths.addAll(missedFilePaths);
		allStreamPaths.addAll(dataReplayFilePaths);
		allStreamPaths.addAll(extraFilePaths);
		testLocalStreamData(allStreamNames, allStreamPaths, obj);

		// testing run method
		List<Path> inconsistentdata = new ArrayList<Path>();
		String[] args = {("file:///tmp/test/" + className + "/1/,file:///" + 
				"tmp/test/" + className + "/2/")};
		LOG.info("testing run method");
		checkDataConsistency(allStreamPaths, inconsistentdata, args, obj);

		LOG.info("collector names are optional here");
		String[] arguments = {("file:///tmp/test/" + className + "/1/,file:///" + 
				"tmp/test/" + className + "/2/"), ("consistentData,missingFiles," +
						"dataReplayFiles,extraFiles")};
		checkDataConsistency(allStreamPaths, inconsistentdata, arguments, obj);

		LOG.info("no optional arguments");
		String[] arg = {("file:///tmp/test/" + className + "/1/,file:///" + 
				"tmp/test/" + className + "/2/"), ("consistentData,missingFiles," +
						"dataReplayFiles,extraFiles"), "collector1"};
		checkDataConsistency(allStreamPaths, inconsistentdata, arg, obj);
	}
}

