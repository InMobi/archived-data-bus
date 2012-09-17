package com.inmobi.databus.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.local.FSDataOutputStream;
import com.inmobi.databus.local.Path;

public class TestMirrorStreamDataConsistency {
	private static Logger LOG = Logger.getLogger(
      TestMirrorStreamDataConsistency.class);
  private static String className = TestMirrorStreamDataConsistency.class
      .getSimpleName();

  FileSystem fs;
  String mergedStreamUrl = "file:///tmp/test/" + className + "/1/" ;
  String mirrorStreamUrl = ("file:///tmp/test/" + className
      + "/2/,file:///tmp/test/" + className +"/3/");
  String [] mirrorStreamUrls = mirrorStreamUrl.split(",");
  String [] streamNames = "nothing,missing,dataReplay,extraFiles".split(",");
  List<Path> expectedResults = new ArrayList<Path>();
  long temptime = System.currentTimeMillis();
  
  @BeforeTest
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    createTestData(mergedStreamUrl, "merge");
    createTestData(mirrorStreamUrl, "mirror"); 
  }
  
  @AfterTest
  public void cleanup() throws Exception {
    fs.delete(new Path(rootDirs[0]).getParent(), true);
  }
  
  public void createMinDirs(String listPath, boolean streamFlag, 
  		boolean missingFlag, int dirCount) 
      throws Exception {
    int milliseconds = 60000;
    String date;
    Path streamDir;
    for(String streamName : streamNames ) {
	    for (int i = 0; i < dirCount; i++) {
		    date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + 
		        i * milliseconds);
		    streamDir = new Path(listPath, streamName );
		    if (streamFlag) {
		    	createFilesData( fs, new Path(streamDir, date), 2, 0);
		    } else {
		    	if (missingFlag) {
		    		cretaeFilesData(fs, new path(streamDir, date), 2, 0);
		    	} else {
		    		expectedResults.addAll(createFilesData(fs, new Path(streamDir,
		    				date), 2, 1));
		    	}
		    }
	    }
    }
   }
  
  public void createTestData(String rootDir, String streamType) throws Exception {
    String  listPath ;
    if (streamType.equals("merge")) {
    	String baseDir = new Path(rootDir, "streams");
    	createMinDrirs(baseDir, false, true, 2);
    } else {
    	for (String mirrorstreamName : mirrorStreamUrls) {
    		String baseDir = new Path(mirrorstreamName,"streams");
    		createMinDirs(baseDir,true, true, 2);
    		createMinDirs(baseDir, true, false, 2);
    	}
    }
  }

  public static List<Path> createFilesData(FileSystem fs,
      Path minDir, int fileNum, int start) throws Exception {
  	Path createPath = new Path(minDir);
    fs.mkdirs(createPath);
    List<Path> filesList = new ArrayList<Path>();
    Path path;
    for (int j = start; j < filesCount; ++j) {
       // String filenameStr = new String(streamName + "-"
       //   + getDateAsYYYYMMDDHHmm(new Date()) + "_" + idFormat.format(j));
      filesList.add("file"+j);
      if (start == 1) {
      	path= new Path(createPath, filesList.get(j-1));
      } else {
      	path= new Path(createPath, filesList.get(j));
      }
      LOG.debug("Creating Test Data with filename [" + filesList.get(j) + "]");
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
	
	@Test
	public void TestMirrorStreamConsistency() {
		mirrorStreamDataConsistencyValidation obj = new 
				mirrorStreamDataConsistencyValidation(mergedStreamUrl, mirrorStreamUrl);
		for (String streamName : streamNames) {
			obj.processingStreams(streamName);
		}
	}
}
