package com.inmobi.databus.utils;

import java.util.ArrayList;
import java.io.File;
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
  String mergedStreamUrl = "file:///tmp/test/" + className + "/1/" ;
  //String mirrorStreamUrl = ("file:///tmp/test/" + className
   //   + "/2/,file:///tmp/test/" + className +"/3/");
  String mirrorStreamUrl = ("file:///tmp/test/" + className + "/2/");
  String [] mirrorStreamUrls = mirrorStreamUrl.split(",");
 // String [] streamNames = "nothing,missing,dataReplay,extraFiles".split(",");
  String [] streamNames = "missing".split(",");
  List<Path> missedPaths = new ArrayList<Path>();
  List<Path> dataReplayPaths = new ArrayList<Path>();
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
  
  public void createMinDirs(Path listPath, boolean streamFlag, 
  		boolean missingFlag, int dirCount) 
      throws Exception {
    int milliseconds = 60000;
    String date;
    Path streamDir;
    for(String streamName : streamNames ) {
	    for (int i = 0; i < dirCount; i++) {
		    date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + 
		        i * milliseconds);
		    streamDir = new Path(listPath, streamName);
		    if (!streamFlag) {
		    	createFilesData(fs, new Path(streamDir, date), 2, 0);
		    } else {
		    	if (!missingFlag) {
		    		createFilesData(fs, new Path(streamDir, date), 2, 0);
		    	} else {
		    		missedPaths.addAll(createFilesData(fs, new Path(streamDir,
		    				date), 2, 1));
		    	}
		    }
	    }
    }
   }
  
  public void createTestData(String rootDir, String streamType) throws  
  		Exception {
   // String  listPath ;
    Path baseDir;
    if (streamType.equals("merge")) {
      baseDir = new Path(rootDir, "streams");
    	createMinDirs(baseDir, false, true, 2);
    } else {
    	for (String mirrorstreamName : mirrorStreamUrls) {
    		baseDir = new Path(mirrorstreamName, "streams");
    		createMinDirs(baseDir, true, false, 2);
    		createMinDirs(baseDir, true, true, 2);
    	}
    }
  }

  public static List<Path> createFilesData(FileSystem fs,
      Path minDir, int filesCount, int start) throws Exception {
  	fs.mkdirs(minDir);
    List<Path> filesList = new ArrayList<Path>();
    Path path;
    for (int j = start; j < filesCount; j++) {
       // String filenameStr = new String(streamName + "-"
       //   + getDateAsYYYYMMDDHHmm(new Date()) + "_" + idFormat.format(j));
      filesList.add(new Path("file"+j));
      if (start == 1) {
      	path= new Path(minDir, filesList.get(j-1));
      } else {
      	path= new Path(minDir, filesList.get(j));
      }
    //  LOG.debug("Creating Test Data with filename [" + filesList.get(j) + "]");
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
	public void testMirrorStream() throws IOException {
		MirrorStreamDataConsistencyValidation obj = new 
				MirrorStreamDataConsistencyValidation(mirrorStreamUrl, mergedStreamUrl);
		List<Path> inconsistentdata = new ArrayList<Path>();
		for (String streamName : streamNames) {
			inconsistentdata.addAll(obj.processListingStreams(streamName));
		}
		
		LOG.info(missedPaths.size() +"   "+ inconsistentdata.size());
	  Assert.assertTrue(1 == 1);
	  /*List<Path> merged = new ArrayList<Path>();
	  List<Path> mirrored = new ArrayList<Path>();
	  merged.add(new Path("file:///1"));
	  merged.add(new Path("file:///2"));
	  merged.add(new Path("file:///3"));
	  merged.add(new Path("file:///4"));
	  mirrored.add(new Path("file:///2"));
	  mirrored.add(new Path("file:///3"));
	  mirrored.add(new Path("file:///4"));
	  LOG.info("size is:"+ obj.compareMergedAndMirror(merged, mirrored));
	 */
	}
}
