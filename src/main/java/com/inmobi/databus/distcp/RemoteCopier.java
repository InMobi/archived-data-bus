package com.inmobi.databus.distcp;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Cluster;
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
    /*for (Stream s : destCluster.consumeStreams.values()) {
      if (s.sourceClusters.contains(getSrcCluster())) {
        streamsToFetch.add(s);
      }
    }*/
  }

  @Override
  public void run() {
    try {
      srcFs = FileSystem.get(new URI(getSrcCluster().hdfsUrl), 
          getConfig().getHadoopConf());
      destFs = FileSystem.get(
          new URI(getConfig().getDestinationCluster().hdfsUrl), 
          getConfig().getHadoopConf());
      
      Path input = getInputPath();
      Path tmpOut = new Path(getConfig().getTmpPath(), "distcp");
      destFs.mkdirs(tmpOut);
      String[] args = {input.makeQualified(destFs).toString(), 
          tmpOut.toString()};
      DistCp.main(args);
      
      //TODO: if success
      commit();
    } catch (Exception e) {
      LOG.warn(e);
    } 
    
  }

  private void commit() throws IOException {
    
  }

  private Path getInputPath() throws IOException {
    Path input = null;
    FSDataOutputStream out = destFs.create(input);
    for (Stream stream : getStreamsToFetch()) {
      
    }
    return input;
  }
}
