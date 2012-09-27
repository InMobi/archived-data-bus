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

public class TestMirrorStreamDataConsistency {
	private static Logger LOG = Logger.getLogger(
			TestMirrorStreamDataConsistency.class);
	private static String className = TestMirrorStreamDataConsistency.class
			.getSimpleName();
	FileSystem fs;
	String mergedStreamUrl = "file:///tmp/test/" + className + "/1/";
	String mirrorStreamUrl = ("file:///tmp/test/" + className
			+ "/2/,file:///tmp/test/" + className +"/3/");
	String [] mirrorStreamUrls = mirrorStreamUrl.split(",");
	String [] allStreamNames = ("empty,emptyDirs,consistentData,missingFiles," +
			"missingDirs,dataReplayDirs,dataReplayFiles,extraFiles,extraDirs").
			split(",");
	String [] emptyStreamName = "empty".split(",");
	String [] emptyDirStreamName = "emptyDirs".split(",");
	String [] consistentDataStreamName = "consistentData".split(",");
	String [] missedFilesStreamName = "missingFiles".split(",");
	String [] missedDirsStreamName = "missingDirs".split(",");
	String [] dataReplayFilesStreamName = "dataReplayFiles".split(",");
	String [] dataReplayDirsStreamName = "dataReplayDirs".split(",");
	String [] extrafilesStreamName = "extraFiles".split(",");
	String [] extraDirsStreamName = "extraDirs".split(",");

	List<Path> emptyPaths = new ArrayList<Path>();
	List<Path> emptyDirsPaths = new ArrayList<Path>();
	List<Path> missedFilePaths = new ArrayList<Path>();
	List<Path> missedDirPaths = new ArrayList<Path>();
	List<Path> dataReplayFilePaths = new ArrayList<Path>();
	List<Path> dataReplayDirPaths = new ArrayList<Path>();
	List<Path> extraFilePaths = new ArrayList<Path>();
	List<Path> extraDirPaths = new ArrayList<Path>();

	long temptime = System.currentTimeMillis();

	@BeforeTest
	public void setup() throws Exception {
		fs = FileSystem.getLocal(new Configuration());
		createTestData(mergedStreamUrl, "merge");
		createTestData(mirrorStreamUrl, "mirror"); 
	}

	@AfterTest
	public void cleanup() throws Exception {
		fs.delete(new Path(mergedStreamUrl).getParent(), true);
	}

	public void createMinDirs(Path listPath, boolean streamFlag, int dirCount, 
			String [] streamNames) throws Exception {
		int milliseconds = 60000;
		String date;
		Path streamDir;
		for(String streamName : streamNames ) {
			boolean added = false;
			for (int i = 1; i <= dirCount; i++) {
				streamDir = new Path(listPath, streamName);
				if (!streamFlag) {
					if (streamName.equals("emptyDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 0, 0);
					} else if (streamName.equals("consistentData")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
					} else if (streamName.equals("missingFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
						for (int j = 0; j < mirrorStreamUrls.length; j++ ) {
							missedFilePaths.add(new Path(new Path(streamDir, date), "file1"));
						} 
					} else if (streamName.equals("missingDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
						if (i == dirCount) {
							for (int j = 0; j < mirrorStreamUrls.length; j++ ) {
								missedDirPaths.add(new Path(new Path(streamDir, date), "file0"));
								missedDirPaths.add(new Path(new Path(streamDir, date), "file1"));
							}
						}
					} else if (streamName.equals("dataReplayFiles") ) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 1);

					} else if (streamName.equals("dataReplayDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
					} else if (streamName.equals("extraFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
					} else if (streamName.equals("extraDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
					} 
				} else {
					if (streamName.equals("emptyDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 0, 0);
					} else if (streamName.equals("consistentData")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
					} else if (streamName.equals("missingFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 1, 0);
					} else if (streamName.equals("missingDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
					} else if (streamName.equals("dataReplayFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
						dataReplayFilePaths.add(new Path(new Path(streamDir, date), 
								"file0"));
					} else if (streamName.equals("dataReplayDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + (i-1) * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
						if (!added) {
							dataReplayDirPaths.add(new Path(new Path(streamDir, date), 
									"file0"));
							dataReplayDirPaths.add(new Path(new Path(streamDir, date), 
									"file1"));
							added = true;
						}
					} else if (streamName.equals("extraFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 3, 0);
						extraFilePaths.add(new Path(new Path(streamDir, date), "file2"));
					} else if(streamName.equals("extraDirs")){
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i * 
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 2, 0);
						if ( i == dirCount) {
							extraDirPaths.add(new Path(new Path(streamDir, date), "file0"));
							extraDirPaths.add(new Path(new Path(streamDir, date), "file1"));
						}
					} 
				}
			}	
		}
	}

	public void createTestData(String rootDir, String streamType) throws  
			Exception {
		Path baseDir;
		if (streamType.equals("merge")) {
			baseDir = new Path(rootDir, "streams");
			createMinDirs(baseDir, false, 0, emptyStreamName);
			createMinDirs(baseDir, false, 2, emptyDirStreamName);
			createMinDirs(baseDir, false, 2, consistentDataStreamName);
			createMinDirs(baseDir, false, 2, missedFilesStreamName);
			createMinDirs(baseDir, false, 2, missedDirsStreamName);
			createMinDirs(baseDir, false, 2, dataReplayFilesStreamName);
			createMinDirs(baseDir, false, 2, dataReplayDirsStreamName);
			createMinDirs(baseDir, false, 2, extrafilesStreamName);
			createMinDirs(baseDir, false, 2, extraDirsStreamName);
		} else {
			for (String mirrorstreamName : mirrorStreamUrls) {
				baseDir = new Path(mirrorstreamName, "streams");
				createMinDirs(baseDir, true, 0, emptyStreamName);
				createMinDirs(baseDir, true, 2, emptyDirStreamName);
				createMinDirs(baseDir, true, 2, consistentDataStreamName);
				createMinDirs(baseDir, true, 2, missedFilesStreamName);
				createMinDirs(baseDir, true, 1, missedDirsStreamName);
				createMinDirs(baseDir, true, 2, dataReplayFilesStreamName);
				createMinDirs(baseDir, true, 3, dataReplayDirsStreamName);
				createMinDirs(baseDir, true, 2, extrafilesStreamName);
				createMinDirs(baseDir, true, 3, extraDirsStreamName);
			}
		}
	}

	public static List<Path> createFilesData(FileSystem fs,
			Path minDir, int filesCount, int start) throws Exception {
		fs.mkdirs(minDir);
		List<Path> filesList = new ArrayList<Path>();
		Path path;
		for (int j = start; j < filesCount; j++) {
			filesList.add(new Path("file"+j));
			if (start == 1) {
				path= new Path(minDir, filesList.get(j-1));
			} else {
				path= new Path(minDir, filesList.get(j));
			}
			FSDataOutputStream streamout = fs.create(path);
			if (start == 1) {
				streamout.writeBytes("Creating Test data for teststream "
						+ filesList.get(j-1));
			} else {
				streamout.writeBytes("Creating Test data for teststream "
						+ filesList.get(j));
			}
			streamout.close();
			Assert.assertTrue(fs.exists(path));
		}
		return filesList; 
	}

	private void mirrorStreamConsistency(String [] streamNames, List<Path> 
			expectedPaths, MirrorStreamDataConsistencyValidation obj) throws 
					Exception {
		List<Path> inconsistentdata = new ArrayList<Path>();
		for (String streamName : streamNames) {
			inconsistentdata.addAll(obj.processListingStreams(streamName));
		}
		Assert.assertEquals(inconsistentdata.size(), expectedPaths.size());
		Assert.assertTrue(inconsistentdata.containsAll(expectedPaths));
	}

	@Test
	public void testMirrorStream() throws Exception {
		MirrorStreamDataConsistencyValidation obj = new 
				MirrorStreamDataConsistencyValidation(mirrorStreamUrl, mergedStreamUrl);
		mirrorStreamConsistency(emptyStreamName, emptyPaths, obj);
		// empty dirs
		mirrorStreamConsistency(emptyDirStreamName, emptyDirsPaths, obj);
		//missing file paths
		mirrorStreamConsistency(missedFilesStreamName, missedFilePaths, obj);
		//missing dir paths 
		mirrorStreamConsistency(missedDirsStreamName, missedDirPaths, obj);
		//data replay : dir paths
		mirrorStreamConsistency(dataReplayDirsStreamName, dataReplayDirPaths, obj);
		//data replay : file paths
		mirrorStreamConsistency(dataReplayFilesStreamName, dataReplayFilePaths, obj);
		//extra file paths
		mirrorStreamConsistency(extrafilesStreamName, extraFilePaths, obj);
		//extra dir paths
		mirrorStreamConsistency(extraDirsStreamName, extraDirPaths, obj);
		//all streams together
		List<Path> allStreamPaths = new ArrayList<Path>();
		allStreamPaths.addAll(emptyPaths);
		allStreamPaths.addAll(emptyDirsPaths);
		allStreamPaths.addAll(missedFilePaths);
		allStreamPaths.addAll(missedDirPaths);
		allStreamPaths.addAll(dataReplayDirPaths);
		allStreamPaths.addAll(dataReplayFilePaths);
		allStreamPaths.addAll(extraFilePaths);
		allStreamPaths.addAll(extraDirPaths);
		
		mirrorStreamConsistency(allStreamNames, allStreamPaths, obj);
		String [] args = {("file:///tmp/test/" + className + "/1/"), ("file:///tmp/" +
				"test/" + className	+ "/2/,file:///tmp/test/" + className +"/3/")}; 
		List<Path> inconsistentdata = obj.run(args);
		Assert.assertEquals(inconsistentdata.size(), allStreamPaths.size());
		Assert.assertTrue(inconsistentdata.containsAll(allStreamPaths));
		
	}
}
