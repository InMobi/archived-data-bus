package com.inmobi.databus.distcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.AbstractServiceTest;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.PublishMissingPathsTest;
import com.inmobi.databus.SourceStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

public class TestMergedStreamService extends MergedStreamService
    implements AbstractServiceTest {
  private static final Log LOG = LogFactory
      .getLog(TestMergedStreamService.class);
  
  private Cluster destinationCluster = null;
  private Cluster srcCluster = null;
  private FileSystem fs = null;
  private Map<String, List<String>> files = null;
  private Calendar behinddate = new GregorianCalendar();
  private Date todaysdate = null;
  
  public TestMergedStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster) throws Exception {
    super(config, srcCluster, destinationCluster);
    // TODO Auto-generated constructor stub
    this.srcCluster = srcCluster;
    this.destinationCluster = destinationCluster;
    this.fs = FileSystem.getLocal(new Configuration());
    this.behinddate.add(Calendar.MINUTE, -5);
  }
  
  @Override
  protected void preExecute() throws Exception {
    try {
      // PublishMissingPathsTest.testPublishMissingPaths(this, false);
      if (files != null)
        files.clear();
      files = null;
      files = new HashMap<String, List<String>>();
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        LOG.debug("Working for Stream in Merged Stream Service "
            + sstream.getValue().getName());

        List<String> filesList = new ArrayList<String>();
        long mins = (System.currentTimeMillis() - behinddate.getTimeInMillis()) / 60000;
        
        for (int i = 0; i < mins; ++i) {
          String listPath = srcCluster.getLocalFinalDestDirRoot()
              + sstream.getValue().getName() + File.separator
              + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
          
          LOG.debug("Getting List of Files from Path: " + listPath);
          FileStatus[] fStats = fs.listStatus(new Path(listPath));
          behinddate.add(Calendar.MINUTE, 1);
          for (FileStatus fStat : fStats) {
            filesList.add(fStat.getPath().getName());
            LOG.debug("Adding File to Merged filesList "
                + fStat.getPath().getName());
          }
        }
        files.put(sstream.getValue().getName(), filesList);
        
        behinddate.add(Calendar.HOUR_OF_DAY, -2);

        LOG.info("Creating Dummy commit Path for verifying Missing Paths");
        String dummycommitpath = this.destinationCluster.getFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
        fs.mkdirs(new Path(dummycommitpath));
      }
      
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Error in MergedStreamService Test PreExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new RuntimeException("Error in MergedStreamService Test PreExecute");
    }
    todaysdate = null;
    todaysdate = new Date();
    
  }
  
  @Override
  protected void postExecute() throws Exception {
    try {
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        List<String> filesList = files.get(sstream.getValue().getName());
        
        LOG.info("Verifying Missing Paths for Merged Stream");

        if (filesList.size() > 0) {
          PublishMissingPathsTest.VerifyMissingPublishPaths(fs,
              todaysdate.getTime(), behinddate,
              this.destinationCluster.getFinalDestDirRoot()
                  + sstream.getValue().getName());
          

          String commitpath = destinationCluster.getFinalDestDirRoot()
              + sstream.getValue().getName() + File.separator
              + Cluster.getDateAsYYYYMMDDHHPath(todaysdate.getTime());
          FileStatus[] mindirs = fs.listStatus(new Path(commitpath));
          
          LOG.info("Verifying Merged Paths in Stream for directory "
              + commitpath);

          Set<String> commitPaths = new HashSet<String>();
          
          for (FileStatus minutedir : mindirs) {
            FileStatus[] filePaths = fs.listStatus(minutedir.getPath());
            for (FileStatus filePath : filePaths) {
              commitPaths.add(filePath.getPath().getName());
            }
          }
          
          try {
            LOG.debug("Checking in Path for Merged mapred Output, No. of files: "
                + commitPaths.size());
            
            for (int j = 0; j < filesList.size() - 1; ++j) {
              String checkpath = filesList.get(j);
              LOG.debug("Merged Checking file: " + checkpath);
              Assert.assertTrue(commitPaths.contains(checkpath));
            }
          } catch (NumberFormatException e) {
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(
          "Error in MergedStreamService Test PostExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new RuntimeException(
          "Error in MergedStreamService Test PostExecute");
    }
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
  public void publishMissingPaths() throws Exception {
    super.publishMissingPaths(fs, destinationCluster.getFinalDestDirRoot());
  }
  
  @Override
  public Cluster getCluster() {
    return destinationCluster;
  }
  
  public FileSystem getFileSystem() {
    return fs;
  }

}

