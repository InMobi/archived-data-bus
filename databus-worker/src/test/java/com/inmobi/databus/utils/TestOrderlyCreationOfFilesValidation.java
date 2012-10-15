package com.inmobi.databus.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

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
import com.inmobi.databus.Cluster;

public class TestOrderlyCreationOfFilesValidation {
  private static Logger LOG = Logger.getLogger(
      TestOrderlyCreationOfFilesValidation.class);
  private static String className = TestOrderlyCreationOfFilesValidation.class
      .getSimpleName();

  FileSystem fs;
  Set<Path> outoforderExpectedResults = new HashSet<Path>();
  Set<Path> noneMissingDirsExpectedResults = new HashSet<Path>();
  Set<Path> prevHourMissingDirsExpectedResults = new HashSet<Path>();
  Set<Path> lastHourMissingDirsExpectedResults = new HashSet<Path>();
  Set<Path> inorderExpectedResults = new HashSet<Path>();
  String rootDirs [] = ("file:///tmp/test/" + className
  		+ "/1/,file:///tmp/test/" + className +"/2/").split(",");
  List<String> baseDirs = new ArrayList<String>();
  List<String> emptyStream = new ArrayList<String>();
  List<String> inorderStream = new ArrayList<String>();
  List<String> outoforderStream = new ArrayList<String>();
  List<String> noneMissingStream = new ArrayList<String>();
  List<String> missingPrevHoursStream = new ArrayList<String>();
  List<String> missingLastHourStream = new ArrayList<String>();
  long temptime = System.currentTimeMillis();

  @BeforeTest
  public void setup() throws Exception {
  	fs = FileSystem.getLocal(new Configuration());
  	emptyStream.add("empty");
  	inorderStream.add("inorder");
  	outoforderStream.add("outoforder");
  	baseDirs.add("streams");
  	baseDirs.add("streams_local");
  	noneMissingStream.add("nonemissing");
  	missingPrevHoursStream.add("missingprevhours");
  	missingLastHourStream.add("missinglasthour");
  	createTestData();

  }
  
  @AfterTest
  public void cleanup() throws Exception {
    fs.delete(new Path(rootDirs[0]).getParent(), true);
  }
  
  public void createMinDirs(String listPath, boolean outofOrder, int dirNumber) 
      throws Exception {
    int milliseconds;
    if (outofOrder) {
      milliseconds = -60000;
    } else {
      milliseconds = 60000;
    }
 
    String date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + 
        dirNumber * milliseconds);
    int filesCount = 2;
    Path minDir = new Path(listPath, date);
    fs.mkdirs(minDir);
    LOG.info("minDir " + minDir);
    for (int i =0; i < filesCount; i++) {
      createFilesData(fs, minDir, i );
    }
  if (outofOrder) {
    outoforderExpectedResults.add(minDir);
  }
}
  
  public void createMinDirsForHoleValidation(String listPath,
      boolean noneMissing, boolean prevHourMissing, int dirNumber)
      throws Exception {
    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date(temptime));
    cal.add(Calendar.HOUR, -1);
    cal.set(Calendar.MINUTE, 0);
    Date oneHourBack = cal.getTime();
    long increment = 60000;
    long startTime = oneHourBack.getTime();
    long time = startTime;
    if (noneMissing) {      
      for (int i = 0; time <= temptime; i++) {
        time = startTime + (i * increment);
        String path = Cluster.getDateAsYYYYMMDDHHMNPath(time);
        int filesCount = 2;
        Path minDir = new Path(listPath, path);
        fs.mkdirs(minDir);
        for (int j = 0; j < filesCount; j++) {
          createFilesData(fs, minDir, j);
        }
      }
    } else {
      if (prevHourMissing) {
        for (int i = 0; time <= temptime; i++) {
          if (i != 29) {
            time = startTime + (i * increment);
            String path = Cluster.getDateAsYYYYMMDDHHMNPath(time);
            int filesCount = 2;
            Path minDir = new Path(listPath, path);
            fs.mkdirs(minDir);
            for (int j = 0; j < filesCount; j++) {
              createFilesData(fs, minDir, j);
            }
          } else {
            time = startTime + (i * increment);
            String path = Cluster.getDateAsYYYYMMDDHHMNPath(time);
            Path minDir = new Path(listPath, path);
            prevHourMissingDirsExpectedResults.add(minDir);
          }
        }
      } else {
        for (int i = 0; time <= temptime; i++) {
          if (i != 61) {
            time = startTime + (i * increment);
            LOG.info("Time "+(new Date(time)).toString());
            String path = Cluster.getDateAsYYYYMMDDHHMNPath(time);
            int filesCount = 2;
            Path minDir = new Path(listPath, path);
            fs.mkdirs(minDir);
            for (int j = 0; j < filesCount; j++) {
              createFilesData(fs, minDir, j);
            }
          } else {
            time = startTime + (i * increment);
            String path = Cluster.getDateAsYYYYMMDDHHMNPath(time);
            Path minDir = new Path(listPath, path);
            lastHourMissingDirsExpectedResults.add(minDir);
          }
        }
      }
    }
  }
  
  public void createTestData() throws Exception {
    String  listPath ;
    int dirCount =2;
    for (int j = 1; j <= dirCount; ++j) {
      for ( String rootdir : rootDirs) {
        for (String basedir : baseDirs) {
          for (String inorderstream : inorderStream) {
            listPath = rootdir + File.separator + basedir + File.separator +
                inorderstream + File.separator;
            createMinDirs(listPath, false, j);
          }
          for (String outoforderstream : outoforderStream) {
            listPath = rootdir + File.separator + basedir + File.separator +
                outoforderstream + File.separator;
            createMinDirs(listPath, false, j);
            Thread.sleep(1000);
            createMinDirs(listPath, true, j);
          }
          for (String nonemissing : noneMissingStream) {
            listPath = rootdir + File.separator + basedir + File.separator
                + nonemissing + File.separator;
            createMinDirsForHoleValidation(listPath, true, false, j);
          }
          for (String missingPrevHours : missingPrevHoursStream) {
            listPath = rootdir + File.separator + basedir + File.separator
                + missingPrevHours + File.separator;
            createMinDirsForHoleValidation(listPath, false, true, j);
          }
          for (String missingLastHour : missingLastHourStream) {
            listPath = rootdir + File.separator + basedir + File.separator
                + missingLastHour + File.separator;
            createMinDirsForHoleValidation(listPath, false, false, j);
          }
        }
      }
      Thread.sleep(1000);
    }
  }

  public static Path createFilesData(FileSystem fs,
      Path minDir, int fileNum) throws Exception {
    Path filenameStr = new Path("file" + fileNum );
    Path path = new Path(minDir, filenameStr);
    LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
    FSDataOutputStream streamout = fs.create(path);
    streamout.writeBytes("Creating Test data for teststream "
        + path);
    streamout.close(); 
    return path;   
  }
  
  private void  checkAllElements(List<Path> totalOutOfOrderDirs, Set<Path> 
  		outoforderExpectedResults) {
  	Iterator it = outoforderExpectedResults.iterator();
  	while (it.hasNext()) {
  		Assert.assertTrue(totalOutOfOrderDirs.contains(it.next()));
  	}
  }
  
  private void checkAllElementsOfSets(Set<Path> totalMissingDirs,
      Set<Path> missingExpectedResults) {
    Iterator it = missingExpectedResults.iterator();
    while (it.hasNext()) {
      Assert.assertTrue(totalMissingDirs.contains(it.next()));
    }
  }

  private List<Path> checkResults(String [] rootDirs, List<String> baseDirs, 
  		List<String> streamNames, OrderlyCreationOfDirs obj) throws Exception {
  	List<Path> outOfOrderDirs = new ArrayList<Path>();
  	Set<Path> notCreatedDirs = new HashSet<Path>();
  	for (String rootDir : rootDirs) {
  		for (String baseDir : baseDirs) {
        obj.pathConstruction(rootDir, baseDir, streamNames, outOfOrderDirs,
            notCreatedDirs);
  		}
  	}
  	return outOfOrderDirs;
  }
  
  private Set<Path> checkResultsForMissingDirs(String[] rootDirs,
      List<String> baseDirs, List<String> streamNames, OrderlyCreationOfDirs obj)
      throws Exception {
    List<Path> outOfOrderDirs = new ArrayList<Path>();
    Set<Path> notCreatedDirs = new HashSet<Path>();
    for (String rootDir : rootDirs) {
      for (String baseDir : baseDirs) {
        obj.pathConstruction(rootDir, baseDir, streamNames, outOfOrderDirs,
            notCreatedDirs);
      }
    }
    return notCreatedDirs;
  }
  
  @Test
  public void TestOrderlyCreation() throws Exception {
  	OrderlyCreationOfDirs obj = new OrderlyCreationOfDirs();
  	//empty stream
  	List<Path> outOfOrderDirs = checkResults(rootDirs, baseDirs, emptyStream, 
  			obj);
  	checkAllElements(outOfOrderDirs, inorderExpectedResults);
  	
  	//inorder stream
  	outOfOrderDirs = checkResults(rootDirs, baseDirs, inorderStream, obj);
  	checkAllElements(outOfOrderDirs, inorderExpectedResults);
  	
  	//outoforder stream
  	outOfOrderDirs = checkResults(rootDirs, baseDirs, outoforderStream, obj);
  	checkAllElements(outOfOrderDirs, outoforderExpectedResults);
  	Assert.assertEquals(outOfOrderDirs.size(), outoforderExpectedResults.size());

  	//all streams together
  	List<String> totalStreams = new ArrayList<String>();
  	totalStreams.add("empty");
  	totalStreams.add("inorder");
  	totalStreams.add("outoforder");
  	List<Path> totalOutOfOrderDirs = checkResults(rootDirs, baseDirs, 
  			totalStreams, obj);
  	checkAllElements(totalOutOfOrderDirs, outoforderExpectedResults);
  	Assert.assertEquals(totalOutOfOrderDirs.size(), outoforderExpectedResults.
  			size()); 

  	//streamnames are optional here
  	String args[] = {("file:///tmp/test/" + className + "/1/,file:///tmp/test/"
  			+ className +"/2/"), ("streams,streams_local") } ;
  	totalOutOfOrderDirs = new ArrayList<Path>();
  	totalOutOfOrderDirs = obj.run(args);
  	checkAllElements(totalOutOfOrderDirs, outoforderExpectedResults);
  	Assert.assertEquals(totalOutOfOrderDirs.size(), outoforderExpectedResults.
  			size()); 
  	LOG.info("checking");

  	// base dirs as optional
  	String arg[] =  {("file:///tmp/test/" + className + "/1/,file:///tmp/test/"
  			+ className +"/2/")};
  	totalOutOfOrderDirs = new ArrayList<Path>();
  	totalOutOfOrderDirs = obj.run(arg);
  	checkAllElements(totalOutOfOrderDirs, outoforderExpectedResults);
  	Assert.assertEquals(totalOutOfOrderDirs.size(), outoforderExpectedResults.
  			size()); 
  	
    // none missing stream
    Set<Path> missingDirs = checkResultsForMissingDirs(rootDirs, baseDirs,
        noneMissingStream, obj);
    checkAllElementsOfSets(missingDirs, noneMissingDirsExpectedResults);

    // path from prev hour missing stream
    missingDirs = checkResultsForMissingDirs(rootDirs, baseDirs,
        missingPrevHoursStream, obj);
    checkAllElementsOfSets(missingDirs, prevHourMissingDirsExpectedResults);
    Assert.assertEquals(missingDirs.size(),
        prevHourMissingDirsExpectedResults.size());

    // path from last hour missing stream
    missingDirs = checkResultsForMissingDirs(rootDirs, baseDirs,
        missingLastHourStream, obj);
    checkAllElementsOfSets(missingDirs, lastHourMissingDirsExpectedResults);
    Assert.assertEquals(missingDirs.size(),
        lastHourMissingDirsExpectedResults.size());
  }
}

