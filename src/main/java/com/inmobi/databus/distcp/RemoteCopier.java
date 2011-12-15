package com.inmobi.databus.distcp;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.DatabusConfig.ReplicatedStream;
import com.inmobi.databus.DatabusConfig.Stream;

public class RemoteCopier extends AbstractCopier {

  private static final Log LOG = LogFactory.getLog(RemoteCopier.class);

  private FileSystem srcFs;
  private FileSystem destFs;

  public RemoteCopier(DatabusConfig config, Cluster srcCluster) {
    super(config, srcCluster);
  }

  protected void addStreamsToFetch() {
    Cluster destCluster = getConfig().getDestinationCluster();
    for (Stream s : destCluster.replicatedStreams) {
      if (s.sourceClusters.contains(getSrcCluster())) {
        streamsToFetch.add(s);
      }
    }
  }

  @Override
  public void run() {
    DistCp distcp = null;
    try {
      srcFs = FileSystem.get(new URI(getSrcCluster().hdfsUrl), 
          getConfig().getHadoopConf());
      destFs = FileSystem.get(
          new URI(getConfig().getDestinationCluster().hdfsUrl), 
          getConfig().getHadoopConf());
      
      Path tmpOut = new Path(getConfig().getTmpPath(), "distcp");
      destFs.mkdirs(tmpOut);
      DistCpOptions options = new DistCpOptions(getSrcPaths(), tmpOut);
      distcp = new DistCp(getConfig().getHadoopConf(), 
          options);
      
      
    } catch (Exception e) {
      LOG.warn(e);
    } finally {
      if (distcp != null) {
        //distcp.
      }
    }
    
    
    
  }

  private List<Path> getSrcPaths() throws IOException {
    List<Path> paths = new ArrayList<Path>();
    for (Stream stream : getStreamsToFetch()) {
      paths.addAll(getSrcPaths((ReplicatedStream) stream));
    }
    return paths;
  }

  private List<Path> getSrcPaths(ReplicatedStream stream) throws IOException {
    List<Path> paths = new ArrayList<Path>();
    Path fetchedTill = new Path(getSrcCluster().hdfsUrl + 
        File.separator + stream.offset);
    FileStatus[] list = srcFs.listStatus(fetchedTill.getParent());
    return paths;
  }
}
