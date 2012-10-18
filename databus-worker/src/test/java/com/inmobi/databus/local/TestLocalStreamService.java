package com.inmobi.databus.local;

import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.text.NumberFormat;
import java.util.ArrayList;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.inmobi.databus.AbstractServiceTest;
import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.PublishMissingPathsTest;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.utils.CalendarHelper;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

public class TestLocalStreamService extends LocalStreamService implements
    AbstractServiceTest {
  private static Logger LOG = Logger.getLogger(TestLocalStreamService.class);
  private Cluster srcCluster = null;
  private CheckpointProvider provider = null;
  private static final int NUM_OF_FILES = 25;
  private List<String> tmpFilesList = null;
  private Map<String, List<String>> files = new HashMap<String, List<String>>();
  private Map<String, Set<String>> prevfiles = new HashMap<String, Set<String>>();
  private Calendar behinddate = null;
  private FileSystem fs = null;
  private Date todaysdate = null;
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }
  
  public static String getDateAsYYYYMMDDHHmm(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return dateFormat.format(date);
  }

  public static List<String> createScribeData(FileSystem fs, String streamName,
      String pathName, int filesCount) throws Exception {
    
    Path createPath = new Path(pathName);
    fs.mkdirs(createPath);
    List<String> filesList = new ArrayList<String>();
    
    for (int j = 0; j < filesCount; ++j) {
      Thread.sleep(1000);
      String filenameStr = new String(streamName + "-"
          + getDateAsYYYYMMDDHHmm(new Date()) + "_" + idFormat.format(j));
      filesList.add(j, filenameStr);
      Path path = new Path(createPath, filesList.get(j));
      
      LOG.debug("Creating Test Data with filename [" + filesList.get(j) + "]");
      FSDataOutputStream streamout = fs.create(path);
      streamout.writeBytes("Creating Test data for teststream "
          + filesList.get(j));
      
      streamout.close();
      
      Assert.assertTrue(fs.exists(path));
    }
    
    return filesList;
  }
  
  public void doRecursiveListing(Path dir, Set<Path> listing,
  		FileSystem fs) throws IOException {
  	FileStatus[] fileStatuses = fs.listStatus(dir);
  	if (fileStatuses == null || fileStatuses.length == 0) {
  		LOG.debug("No files in directory:" + dir);
  	} else {
  		for (FileStatus file : fileStatuses) {
  			if (file.isDir()) {
  				doRecursiveListing(file.getPath(), listing, fs);	
  			} else {
  				listing.add(file.getPath().getParent());
  			}
  		}
  	}
  }

  @Override
  protected void preExecute() throws Exception {
    try {
      // PublishMissingPathsTest.testPublishMissingPaths(this, true);
      behinddate = new GregorianCalendar();
      behinddate.add(Calendar.HOUR_OF_DAY, -2);
      
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        Set<String> prevfilesList = new TreeSet<String>();
        String pathName = srcCluster.getDataDir() + File.separator
            + sstream.getValue().getName() + File.separator
            + srcCluster.getName() + File.separator;
        
        FileStatus[] fStats = fs.listStatus(new Path(pathName));
        
        LOG.debug("Adding Previous Run Files in Path: " + pathName);
        for (FileStatus fStat : fStats) {
          LOG.debug("Previous File: " + fStat.getPath().getName());
          prevfilesList.add(fStat.getPath().getName());
        }
        
        List<String> filesList = createScribeData(fs, sstream.getValue()
            .getName(), pathName, NUM_OF_FILES);
        
        files.put(sstream.getValue().getName(), filesList);
        prevfiles.put(sstream.getValue().getName(), prevfilesList);
        
        String dummycommitpath = srcCluster.getLocalFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
        fs.mkdirs(new Path(dummycommitpath));
      }
      
      {
        LOG.debug("Creating Tmp Files for LocalStream");
        String pathName = srcCluster.getTmpPath() + File.separator
            + this.getName() + File.separator;
        tmpFilesList = createScribeData(fs, this.getName(), pathName,
            NUM_OF_FILES);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PreExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PreExecute");
    }
    todaysdate = new Date();
  }
  
  @Override
  protected void postExecute() throws Exception {
    try {
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        LOG.debug("Verifying Tmp Files for LocalStream Stream "
            + sstream.getValue().getName());
        String pathName = srcCluster.getDataDir() + File.separator
            + sstream.getValue().getName() + File.separator
            + srcCluster.getName() + File.separator;
        for (int j = 0; j < tmpFilesList.size(); ++j) {
          Path tmppath = new Path(pathName + File.separator
              + tmpFilesList.get(j));
          Assert.assertFalse(fs.exists(tmppath));
        }
        
        LOG.info("Tmp Path does not exist for cluster " + srcCluster.getName());

        List<String> filesList = files.get(sstream.getValue().getName());
        Set<String> prevfilesList = prevfiles.get(sstream.getValue().getName());

        Path trashpath = srcCluster.getTrashPathWithDateHour();
        String streamPrefix = srcCluster.getLocalFinalDestDirRoot()
        		+ sstream.getValue().getName();
        Set<Path> listOfPaths = new HashSet<Path>();
        doRecursiveListing(new Path(streamPrefix), listOfPaths, fs);
        Path latestPath = null;
        for (Path path : listOfPaths) {
        	if (latestPath== null || (CalendarHelper.getDateFromStreamDir(new 
        			Path(streamPrefix), path).compareTo(CalendarHelper.getDateFromStreamDir
        					(new Path(streamPrefix), latestPath)) > 0)) {
        		latestPath = path;
        	}
        }

        // Make sure all the paths from dummy to mindir are created
        PublishMissingPathsTest.VerifyMissingPublishPaths(fs,
            todaysdate.getTime(), behinddate,
            srcCluster.getLocalFinalDestDirRoot()
                + sstream.getValue().getName());
  
        try {
          String streams_local_dir = latestPath + File.separator + srcCluster.getName(); 
        	LOG.debug("Checking in Path for mapred Output: " + streams_local_dir);
          
          // First check for the previous current file
          if (!prevfilesList.isEmpty()) {
            String prevcurrentFile = (String) prevfilesList.toArray()[prevfilesList
                .size() - 1];
            LOG.debug("Checking Data Replay in mapred Output");
            
            Iterator<String> prevFilesItr = prevfilesList.iterator();
            
            for (int i = 0; i < prevfilesList.size() - 1; ++i) {
              String dataReplayfile = prevFilesItr.next();
              LOG.debug("Checking Data Replay, File [" + dataReplayfile + "]");
              Assert.assertFalse(fs.exists(new Path(streams_local_dir + "-"
                  + dataReplayfile + ".gz")));
            }
            
            LOG.debug("Checking Previous Run Current file in mapred Output ["
                + prevcurrentFile + "]");
            Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                + prevcurrentFile + ".gz")));
          }
          
          for (int j = 0; j < NUM_OF_FILES - 1; ++j) {
            LOG.debug("Checking file in mapred Output [" + filesList.get(j)
                + "]");
            Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                + filesList.get(j) + ".gz")));
          }
          
          CheckpointProvider provider = this.getCheckpointProvider();
          
          String checkpoint = new String(provider.read(sstream.getValue()
              .getName() + srcCluster.getName()));
          
          LOG.debug("Checkpoint for " + sstream.getValue().getName()
              + srcCluster.getName() + " is " + checkpoint);
          
          LOG.debug("Comparing Checkpoint " + checkpoint + " and "
              + filesList.get(NUM_OF_FILES - 2));
          Assert.assertTrue(checkpoint.compareTo(filesList
              .get(NUM_OF_FILES - 2)) == 0);
          
          LOG.debug("Verifying Collector Paths");
          
          Path collectorPath = new Path(srcCluster.getDataDir(), sstream
              .getValue().getName() + File.separator + srcCluster.getName());
          
          for (int j = NUM_OF_FILES - 7; j < NUM_OF_FILES; ++j) {
            LOG.debug("Verifying Collector Path " + collectorPath
                + " Previous File " + filesList.get(j));
            Assert.assertTrue(fs.exists(new Path(collectorPath, filesList
                .get(j))));
          }
          
          LOG.debug("Verifying Trash Paths");
          
          for (String trashfile : prevfilesList) {
            String trashfilename = srcCluster.getName() + "-" + trashfile;
            LOG.debug("Verifying Trash Path " + trashpath + " Previous File "
                + trashfilename);
            Assert.assertTrue(fs.exists(new Path(trashpath, trashfilename)));
          }
          
          // Here 6 is the number of files - trash paths which are excluded
          for (int j = 0; j < NUM_OF_FILES - 7; ++j) {
            if (filesList.get(j).compareTo(checkpoint) <= 0) {
              String trashfilename = srcCluster.getName() + "-"
                  + filesList.get(j);
              LOG.debug("Verifying Trash Path " + trashpath + "File "
                  + trashfilename);
              Assert.assertTrue(fs.exists(new Path(trashpath, trashfilename)));
            } else
              break;
          }
          
        } catch (NumberFormatException e) {
          
        }
      }
      fs.delete(srcCluster.getTrashPathWithDateHour(), true);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PostExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PostExecute");
    }
    
  }
  
  public TestLocalStreamService(DatabusConfig config, Cluster cluster,
      CheckpointProvider provider) {
    super(config, cluster, provider);
    this.srcCluster = cluster;
    this.provider = provider;
    try {
      this.fs = FileSystem.getLocal(new Configuration());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void publishMissingPaths() throws Exception {
    super.publishMissingPaths(fs, srcCluster.getLocalFinalDestDirRoot());
  }
  
  public void runExecute() throws Exception {
    super.execute();
  }
  
  public void runPreExecute() throws Exception {
    preExecute();
  }
  
  public void runPostExecute() throws Exception {
    postExecute();
  }

  @Override
  public Cluster getCluster() {
    return srcCluster;
  }
  
  public CheckpointProvider getCheckpointProvider() {
    return provider;
  }
  
  public FileSystem getFileSystem() {
    return fs;
  }
}

