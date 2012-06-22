package com.inmobi.databus.distcp;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.Stream;
import com.inmobi.databus.Stream.DestinationStreamCluster;
import com.inmobi.databus.TestMiniClusterUtil;
import com.inmobi.databus.Stream.StreamCluster;
import com.inmobi.databus.local.LocalStreamServiceTest;
import com.inmobi.databus.local.LocalStreamServiceTest.TestLocalStreamService;
import com.inmobi.databus.utils.CalendarHelper;

@Test(groups = { "integration" })
public class MergeMirrorStreamTest extends TestMiniClusterUtil {

  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamTest.class);

  private void testPublishMissingPaths(TestService service) throws Exception {

    FileSystem fs = FileSystem.getLocal(new Configuration());
    Calendar behinddate = new GregorianCalendar();
    Calendar todaysdate = new GregorianCalendar();
    String basepublishPaths = service.getDestCluster().getFinalDestDirRoot()
        + "streams_publish" + File.separator;
    String publishPaths = basepublishPaths
        + CalendarHelper.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());

    fs.mkdirs(new Path(publishPaths));

    service.publishMissingPaths(fs);

    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths);

    todaysdate.add(Calendar.HOUR_OF_DAY, 2);

    service.publishMissingPaths(fs);

    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths);

    fs.delete(new Path(basepublishPaths), true);
  }

  private void VerifyMissingPublishPaths(FileSystem fs, long todaysdate,
      Calendar behinddate, String basepublishPaths)
      throws Exception {
    long diff = todaysdate - behinddate.getTimeInMillis();
    while (diff > 60000) {
      String checkcommitpath = basepublishPaths
          + CalendarHelper.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
      LOG.debug("Checking for Created Missing Path: " + checkcommitpath);
      fs.exists(new Path(checkcommitpath));
      behinddate.add(Calendar.MINUTE, 1);
      diff = todaysdate - behinddate.getTimeInMillis();
    }
  }

  /*
   * Here is the basic idea, create two clusters of different rootdir paths run
   * the local stream service to create all the files in streams_local directory
   * run the merge stream service and verify all the paths are visible in
   * primary cluster
   */
  /**
   * @throws Exception
   */
  public void testMergeMirrorStream() throws Exception {
    testMergeMirrorStream("test-mss-databus.xml");
    // Test with 2 mirror sites
    testMergeMirrorStream("test-mss-databus_mirror.xml");
  }

  private void testMergeMirrorStream(String filename) throws Exception {
    final int NUM_OF_FILES = 35;

    DatabusConfigParser configParser = new DatabusConfigParser(
filename);
    DatabusConfig config = configParser.getConfig();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    List<TestLocalStreamService> services = new ArrayList<TestLocalStreamService>();

    for (Map.Entry<String, Cluster> cluster : config.getAllClusters()
        .entrySet()) {
      services.add(new TestLocalStreamService(config, cluster.getValue(),
          new FSCheckpointProvider(cluster.getValue().getCheckpointDir())));
    }
    
    List<String> pathstoRemove = new LinkedList<String>();

    for (Cluster cluster : config.getAllClusters().values()) {
      pathstoRemove.add(cluster.getRootDir());
    }

    for (Map.Entry<String, Stream> sstream : config.getAllStreams().entrySet()) {

      Date todaysdate = null;
      Map<String, List<String>> filesList = new HashMap<String, List<String>>();

      for (TestLocalStreamService service : services) {
        boolean processCluster = false;
        List<String> files = new ArrayList<String>(NUM_OF_FILES);
        Cluster cluster = service.getCluster();
        for (StreamCluster sourceClusters : sstream.getValue()
            .getSourceStreamClusters()) {
          if (cluster.getName()
              .compareTo(sourceClusters.getCluster().getName()) == 0)
            processCluster =true;
        }
        
        if (processCluster) {

        fs.delete(new Path(cluster.getRootDir()), true);
        Path createPath = new Path(cluster.getDataDir(), sstream.getValue()
            .getName() + File.separator + cluster.getName() + File.separator);
        fs.mkdirs(createPath);
        for (int j = 0; j < NUM_OF_FILES; ++j) {
          files.add(j,new String(sstream.getValue().getName() + "-"
              + cluster.getName() + "-"
              + LocalStreamServiceTest.getDateAsYYYYMMDDHHMMSS(new Date())));
          Path path = new Path(createPath, files.get(j));

          FSDataOutputStream streamout = fs.create(path);
          streamout.writeBytes("Creating Test data for cluster "
              + cluster.getName() + " data -> " + files.get(j));
          streamout.close();

          Assert.assertTrue(fs.exists(path));
        }

        filesList.put(cluster.getName(), files);

        service.runOnce();

        todaysdate = new Date();
        String commitpath = cluster.getLocalFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + CalendarHelper.getDateAsYYYYMMDDHHPath(todaysdate.getTime());
        FileStatus[] mindirs = fs.listStatus(new Path(commitpath));

        FileStatus mindir = mindirs[0];

        for (FileStatus minutedir : mindirs) {
          if (mindir.getPath().getName()
              .compareTo(minutedir.getPath().getName()) < 0) {
            mindir = minutedir;
          }
        }

        try {
          Integer.parseInt(mindir.getPath().getName());
          String streams_local_dir = commitpath + mindir.getPath().getName()
              + File.separator + cluster.getName();

          LOG.debug("Checking in Path for mapred Output: " + streams_local_dir);

          for (int j = 0; j < NUM_OF_FILES; ++j) {
            Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                + files.get(j) + ".gz")));
          }
        } catch (NumberFormatException e) {

        }
        //fs.delete(new Path(testRootDir), true);
      }
      }
      
      Stream primaryStream = sstream.getValue();
      Set<Cluster> primaryCluster = new HashSet<Cluster>();
      Cluster destcluster = primaryStream.getPrimaryDestinationCluster();
      
      for (StreamCluster cluster : primaryStream.getSourceStreamClusters()) {
        TestMergeStreamService service = new TestMergeStreamService(config,
            cluster.getCluster(), destcluster);
        testPublishMissingPaths(service);
      }

      for (StreamCluster cluster : primaryStream.getSourceStreamClusters()) {
        primaryCluster.add(cluster.getCluster());
        TestMergeStreamService service = new TestMergeStreamService(config,
            cluster.getCluster(), destcluster);
        service.execute();
      }
      
      Set<StreamCluster> destMirrorClusters = primaryStream
          .getDestinationStreamClusters();
      Set<Cluster> MirrorprimaryCluster = new HashSet<Cluster>();

      for (StreamCluster destStreamCluster : destMirrorClusters) {
        DestinationStreamCluster cluster = (DestinationStreamCluster) destStreamCluster;
        if (!cluster.isPrimary()) {
          TestMirrorStreamService service = new TestMirrorStreamService(config,
              destcluster, cluster.getCluster());
          testPublishMissingPaths(service);
        }
      }



      for (StreamCluster destStreamCluster : destMirrorClusters) {
        DestinationStreamCluster cluster = (DestinationStreamCluster) destStreamCluster;
        if (!cluster.isPrimary()) {
          MirrorprimaryCluster.add(cluster.getCluster());
          TestMirrorStreamService service = new TestMirrorStreamService(config,
              destcluster, cluster.getCluster());

          service.execute();
        }
      }

      {
      String commitpath = destcluster.getFinalDestDirRoot()
          + sstream.getValue().getName() + File.separator
          + CalendarHelper.getDateAsYYYYMMDDHHPath(todaysdate.getTime());
      FileStatus[] mindirs = fs.listStatus(new Path(commitpath));

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
        
          for (Cluster tmpcluster : primaryCluster) {
            List<String> files = filesList.get(tmpcluster.getName());
            for (int j = 0; j < NUM_OF_FILES; ++j) {
              String checkpath = tmpcluster.getName() + "-" + files.get(j)
                  + ".gz";
              LOG.debug("Merged Checking file: " + checkpath);
              Assert.assertTrue(commitPaths.contains(checkpath));
          }
          }
      } catch (NumberFormatException e) {
      }
      }
      
      {
        for (Cluster tmpcluster : MirrorprimaryCluster) {
          String commitpath = tmpcluster.getFinalDestDirRoot()
          + sstream.getValue().getName() + File.separator
          + CalendarHelper.getDateAsYYYYMMDDHHPath(todaysdate.getTime());
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

            for (Map.Entry<String, List<String>> checkFiles : filesList
                .entrySet()) {
              List<String> files = checkFiles.getValue();
              for (int j = 0; j < NUM_OF_FILES; ++j) {
                String checkpath = checkFiles.getKey() + "-"
                    + files.get(j) + ".gz";
                LOG.debug("Mirror Checking file: " + checkpath);
                Assert.assertTrue(commitPaths.contains(checkpath));
              }
              }
          } catch (NumberFormatException e) {

          }
      }
      }
    }
    for (String path : pathstoRemove) {
      fs.delete(new Path(path), true);
    }
    fs.close();
  }
  
  static interface TestService {
    Cluster getDestCluster();
    void publishMissingPaths(FileSystem fs) throws Exception;
  }

  public static class TestMergeStreamService extends MergedStreamService implements TestService{
    
    private Cluster destinationCluster = null;

    public TestMergeStreamService(DatabusConfig config, Cluster srcCluster,
        Cluster destinationCluster) throws Exception {
      super(config, srcCluster, destinationCluster);
      // TODO Auto-generated constructor stub
      this.destinationCluster = destinationCluster;
    }

    @Override
    public void publishMissingPaths(FileSystem fs) throws Exception {
      super.publishMissingPaths(fs, destinationCluster.getFinalDestDirRoot());
    }

    @Override
    public Cluster getDestCluster() {
      return destinationCluster;
    }

  }

  public static class TestMirrorStreamService extends MirrorStreamService implements TestService{

    private Cluster destinationCluster = null;

    public TestMirrorStreamService(DatabusConfig config, Cluster srcCluster,
        Cluster destinationCluster) throws Exception {
      super(config, srcCluster, destinationCluster);
      this.destinationCluster = destinationCluster;
      // TODO Auto-generated constructor stub
    }

    @Override
    public void publishMissingPaths(FileSystem fs) throws Exception {
      super.publishMissingPaths(fs, destinationCluster.getFinalDestDirRoot());
    }

    @Override
    public Cluster getDestCluster() {
      return destinationCluster;
    }
  }

}
