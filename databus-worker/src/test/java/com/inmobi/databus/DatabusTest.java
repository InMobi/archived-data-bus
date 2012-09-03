package com.inmobi.databus;

import com.inmobi.databus.distcp.TestMirrorStreamService;
import com.inmobi.databus.distcp.TestMergedStreamService;
import com.inmobi.databus.local.TestLocalStreamService;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.util.TimerTask;
import java.util.GregorianCalendar;
import java.util.Calendar;
import java.util.Timer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.HashSet;
import java.util.Map;
import org.testng.annotations.Test;
import com.inmobi.databus.distcp.MergeMirrorStreamTest;
import com.inmobi.databus.distcp.MergedStreamService;
import com.inmobi.databus.distcp.MirrorStreamService;
import com.inmobi.databus.local.LocalStreamService;

import java.util.Set;

public class DatabusTest extends TestMiniClusterUtil {
  
  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamTest.class);
  
  // @BeforeSuite
  public void setup() throws Exception {
    super.setup(2, 2, 1);
  }
  
  // @AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }

  public static class DatabusServiceTest extends Databus {
    public DatabusServiceTest(DatabusConfig config,
        Set<String> clustersToProcess) {
      super(config, clustersToProcess);
    }
  
    @Override
    protected LocalStreamService getLocalStreamService(DatabusConfig config,
        Cluster cluster) {
      return new TestLocalStreamService(config, cluster,
          new FSCheckpointProvider(cluster.getCheckpointDir()));
    }
    
    @Override
    protected MergedStreamService getMergedStreamService(DatabusConfig config,
        Cluster srcCluster, Cluster dstCluster) throws Exception {
      return new TestMergedStreamService(config,
          srcCluster, dstCluster);
    }
    
    @Override
    protected MirrorStreamService getMirrorStreamService(DatabusConfig config,
        Cluster srcCluster, Cluster dstCluster) throws Exception {
      return new TestMirrorStreamService(config,
          srcCluster, dstCluster);
    }
    
  }
  
  private static DatabusServiceTest testService = null;

  // @Test
  public void testDatabus() throws Exception {
    testDatabus("testDatabusService_simple.xml");
  }
  
  private void testDatabus(String filename) throws Exception {
    DatabusConfigParser configParser = new DatabusConfigParser(filename);
    DatabusConfig config = configParser.getConfig();
    Set<String> clustersToProcess = new HashSet<String>();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    
    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      String jobTracker = super.CreateJobConf().get("mapred.job.tracker");
      cluster.getValue().getHadoopConf().set("mapred.job.tracker", jobTracker);
    }

    for (Map.Entry<String, SourceStream> sstream : config.getSourceStreams()
        .entrySet()) {
      clustersToProcess.addAll(sstream.getValue().getSourceClusters());
    }
    
    testService = new DatabusServiceTest(config, clustersToProcess);
    
    Timer timer = new Timer();
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.MINUTE, 5);
    
    timer.schedule(new TimerTask() { 
      public void run() {
        try {
          LOG.info("Stopping Databus Test Service");
          testService.stop();
          LOG.info("Done stopping Databus Test Service");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, calendar.getTime());
    
    LOG.info("Starting Databus Test Service");
    testService.startDatabus();
    
    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      fs.delete(new Path(cluster.getValue().getRootDir()), true);
    }
    
    LOG.info("Done with Databus Test Service");
  }

}
