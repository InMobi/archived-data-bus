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

public class TestMirrorStreamService extends MirrorStreamService
    implements AbstractServiceTest {
  private static final Log LOG = LogFactory
      .getLog(TestMirrorStreamService.class);
  
  private Cluster destinationCluster = null;
  private Cluster srcCluster = null;
  private FileSystem fs = null;
  private Map<String, List<String>> files = null;
  private Calendar behinddate = new GregorianCalendar();
  private Calendar todaysdate = null;
  private long mergeCommitTime = 0;
  
  public TestMirrorStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster) throws Exception {
    super(config, srcCluster, destinationCluster);
    this.destinationCluster = destinationCluster;
    this.srcCluster = srcCluster;
    this.fs = FileSystem.getLocal(new Configuration());
    this.behinddate.add(Calendar.MINUTE, -5);
  }
  
  @Override
  protected void preExecute() throws Exception {
    try {
      mergeCommitTime = 0;
      todaysdate = behinddate;
      // PublishMissingPathsTest.testPublishMissingPaths(this, false);
      if (files != null)
        files.clear();
      files = null;
      files = new HashMap<String, List<String>>();
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        List<String> filesList = new ArrayList<String>();
        long mins = (System.currentTimeMillis() - behinddate.getTimeInMillis()) / 60000;
        
        for (int i = 0; i < mins; ++i) {
          String listPath = srcCluster.getFinalDestDirRoot()
              + sstream.getValue().getName() + File.separator
              + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
          
          FileStatus[] fStats = fs.listStatus(new Path(listPath));
          
          if ((mergeCommitTime != 0) && fStats.length != 0) {
            mergeCommitTime = behinddate.getTimeInMillis();
          }
          for (FileStatus fStat : fStats) {
            filesList.add(fStat.getPath().getName());
          }
          behinddate.add(Calendar.MINUTE, 1);
        }
        String dummycommitpath = this.srcCluster.getFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
        fs.mkdirs(new Path(dummycommitpath));
        files.put(sstream.getValue().getName(), filesList);
      }
      behinddate.add(Calendar.HOUR_OF_DAY, -2);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertFalse(true);
      throw new RuntimeException("Error in MirrorStreamService Test PreExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new RuntimeException("Error in MergedStreamService Test PreExecute");
    }
    
  }
  
  @Override
  protected void postExecute() throws Exception {
    try {
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        PublishMissingPathsTest.VerifyMissingPublishPaths(fs, mergeCommitTime,
            behinddate, this.destinationCluster.getFinalDestDirRoot()
                + sstream.getValue().getName());
        
        List<String> filesList = files.get(sstream.getValue().getName());
        String commitpath = destinationCluster.getFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + Cluster.getDateAsYYYYMMDDHHPath(todaysdate.getTimeInMillis());
        FileStatus[] mindirs = fs.listStatus(new Path(commitpath));
        
        Set<String> commitPaths = new HashSet<String>();
        
        for (FileStatus minutedir : mindirs) {
          FileStatus[] filePaths = fs.listStatus(minutedir.getPath());
          for (FileStatus filePath : filePaths) {
            commitPaths.add(filePath.getPath().getName());
          }
        }
        
        try {
          LOG.debug("Checking in Path for Mirror mapred Output, No. of files: "
              + commitPaths.size());
          
          for (int j = 0; j < filesList.size() - 1; ++j) {
            String checkpath = filesList.get(j);
            LOG.debug("Mirror Checking file: " + checkpath);
            Assert.assertTrue(commitPaths.contains(checkpath));
          }
        } catch (NumberFormatException e) {
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertFalse(true);
      throw new RuntimeException(
          "Error in MirrorStreamService Test PostExecute");
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
