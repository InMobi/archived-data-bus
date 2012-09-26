package com.inmobi.databus.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.inmobi.databus.Cluster;

public class TestLocalMergeStreamConsistency {
	private static Logger LOG = Logger.getLogger(
			TestLocalMergeStreamConsistency.class);
	private static String className = TestLocalMergeStreamConsistency.class
			.getSimpleName();
	FileSystem fs;
	String mergedStreamUrl = "file:///tmp/test/" + className + "/1/" ;
	String localStreamUrl = ("file:///tmp/test/" + className + "/2/," +
			"file:///tmp/test/" + className +"/3/");
	String [] localStreamUrls = localStreamUrl.split(",");

	List<String> emptyStreamName = new ArrayList<String>();
	List<String> missedFilesStreamName = new ArrayList<String>();
	List<String> emptyDirStreamName = new ArrayList<String>();
	List<String> consistentDataStreamName = new ArrayList<String>();
	List<String> dataReplayFilesStreamName = new ArrayList<String>();
	List<String> extrafilesStreamName = new ArrayList<String>();
	List<String> allStreamNames = new ArrayList<String>();

	List<Path> emptyPaths = new ArrayList<Path>();
	List<Path> emptyDirsPaths = new ArrayList<Path>();
	List<Path> missedFilePaths = new ArrayList<Path>();
	List<Path> dataReplayFilePaths = new ArrayList<Path>();
	List<Path> extraFilePaths = new ArrayList<Path>();
	boolean missing = false;
	long temptime = System.currentTimeMillis();

	@BeforeTest
	public void setup() throws Exception {
		fs = FileSystem.getLocal(new Configuration());
		defineStreamNames(emptyStreamName, "empty");
		defineStreamNames(emptyDirStreamName, "emptyDirs");
		defineStreamNames(consistentDataStreamName, "consistentData");
		defineStreamNames(missedFilesStreamName, "missingFiles");
		defineStreamNames(dataReplayFilesStreamName, "dataReplayFiles");
		defineStreamNames(extrafilesStreamName, "extraFiles");
		defineStreamNames(allStreamNames, "empty");
		defineStreamNames(allStreamNames, "emptyDirs");
		defineStreamNames(allStreamNames, "consistentData");
		defineStreamNames(allStreamNames, "missingFiles");
		defineStreamNames(allStreamNames, "dataReplayFiles");
		defineStreamNames(allStreamNames, "extraFiles");

		createTestData(localStreamUrl, "local"); 
		createTestData(mergedStreamUrl, "merge");
	}

	@AfterTest
	public void cleanup() throws Exception {
		fs.delete(new Path(mergedStreamUrl).getParent(), true);
	}
	
	public void defineStreamNames(List<String> streamNames, String streamName) {
		streamNames.add(streamName);
	}
	public void createMinDirs(Path listPath, boolean streamFlag, int dirCount, 
			List<String> streamNames, int start) throws Exception {
		int milliseconds = 60000;
		String date;
		Path streamDir;

		for (String streamName : streamNames) {
			for (int i = 1; i <= dirCount; i++) {
				streamDir = new Path(listPath, streamName);
				if (!streamFlag) {
					int numOfFiles = 5 * localStreamUrls.length;
					if (streamName.equals("emptyDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 0, 0);
					} else if (streamName.equals("consistentData")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), numOfFiles, 0);
					} else if (streamName.equals("missingFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 4, 0);
						createFilesData(fs, new Path(streamDir, date), 5, 5);
					} else if (streamName.equals("dataReplayFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), numOfFiles + 2, 0);
						dataReplayFilePaths.add(new Path(new Path(streamDir, date), 
								"file10"));
						dataReplayFilePaths.add(new Path(new Path(streamDir, date), 
								"file11"));
					} else if (streamName.equals("extraFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), numOfFiles, 0);
						extraFilePaths.add(new Path(new Path(streamDir, date), "file4"));
						extraFilePaths.add(new Path(new Path(streamDir, date), "file9"));
					} 
				} else {
					if (streamName.equals("emptyDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 0, 0);
					} else if (streamName.equals("consistentData")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 5, start);
					} else if (streamName.equals("missingFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 5, start);
						if (!missing) {
							missedFilePaths.add(new Path(new Path(streamDir, date), "file4"));
						}
						missing = true;
					}  else if (streamName.equals("dataReplayFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 5, start);
					} else if (streamName.equals("extraFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 4, start);
					}
				}	
			}	
		}
	}

	public void createTestData(String rootDir, String streamType) throws  
			Exception {
		Path baseDir;
		if (streamType.equals("merge")) {
			int start = 0; 
			baseDir = new Path(rootDir, "streams");
			createMinDirs(baseDir, false, 0, emptyStreamName, start);
			createMinDirs(baseDir, false, 1, emptyDirStreamName, start);
			createMinDirs(baseDir, false, 1, consistentDataStreamName, start);
			createMinDirs(baseDir, false, 1, missedFilesStreamName, start);
			createMinDirs(baseDir, false, 1, dataReplayFilesStreamName, start);
			createMinDirs(baseDir, false, 1, extrafilesStreamName, start); 
		} else {
			int start = 0;
			for (String localStreamUrl : localStreamUrls) {
				baseDir = new Path(localStreamUrl, "streams_local");
				createMinDirs(baseDir, true, 0, emptyStreamName, start);
				createMinDirs(baseDir, true, 1, emptyDirStreamName, start);
				createMinDirs(baseDir, true, 1, consistentDataStreamName, start);
				createMinDirs(baseDir, true, 1, missedFilesStreamName, start);
				createMinDirs(baseDir, true, 1, dataReplayFilesStreamName, start);
				createMinDirs(baseDir, true, 1, extrafilesStreamName, start); 
				start += 5;
			}
		}
	}

	public static List<Path> createFilesData(FileSystem fs, Path minDir, 
			int filesCount, int start) throws Exception {
		fs.mkdirs(minDir);
		List<Path> filesList = new ArrayList<Path>();
		Path path;
		for (int j = start; j < filesCount + start; j++) {
			filesList.add(new Path("file" + j));
			path= new Path(minDir, filesList.get(j-start));
			LOG.debug("Creating Test Data with filename [" + filesList.get(j-start) 
					+ "]");
			FSDataOutputStream streamout = fs.create(path);
			streamout.writeBytes("Creating Test data for teststream"
					+ filesList.get(j-start));
			streamout.close();
			Assert.assertTrue(fs.exists(path));
		}
		return filesList; 
	}
	
	private void testLocalMergeStreams(List<String> streamNames, List<Path> 
			expectedPaths, LocalMergeStreamDataConsistency obj) throws Exception {
		List<Path> inconsistentdata = new ArrayList<Path>();
		inconsistentdata = obj.listingValidation(mergedStreamUrl, localStreamUrls, 
				streamNames);
		Assert.assertEquals(inconsistentdata.size(), expectedPaths.size());
		Assert.assertTrue(inconsistentdata.containsAll(expectedPaths));
	}

	@Test
	public void testMergeStream() throws Exception {
		LocalMergeStreamDataConsistency obj = new LocalMergeStreamDataConsistency();
		testLocalMergeStreams(emptyStreamName, emptyPaths, obj);
		testLocalMergeStreams(emptyDirStreamName, emptyDirsPaths, obj);
		testLocalMergeStreams(missedFilesStreamName, missedFilePaths, obj);
		testLocalMergeStreams(consistentDataStreamName, emptyPaths, obj);
		testLocalMergeStreams(dataReplayFilesStreamName, dataReplayFilePaths, obj);
		testLocalMergeStreams(extrafilesStreamName, extraFilePaths, obj);

		LOG.info("all streams together");
		List<Path> allStreamPaths = new ArrayList<Path>();
		allStreamPaths.addAll(emptyPaths);
		allStreamPaths.addAll(emptyDirsPaths);
		allStreamPaths.addAll(missedFilePaths);
		allStreamPaths.addAll(dataReplayFilePaths);
		allStreamPaths.addAll(extraFilePaths);

		testLocalMergeStreams(allStreamNames, allStreamPaths, obj);
		// testing run method
		List<Path> inconsistentdata = new ArrayList<Path>();
		String[] args = { ("file:///tmp/test/" + className + "/2/,file:///tmp/test/"
				+ className +"/3/"), "file:///tmp/test/" + className + "/1/"};
		LOG.info("testing run method");
		inconsistentdata = obj.run(args);
		Assert.assertEquals(inconsistentdata.size(), allStreamPaths.size());
		Assert.assertTrue(inconsistentdata.containsAll(allStreamPaths));
	}
}
