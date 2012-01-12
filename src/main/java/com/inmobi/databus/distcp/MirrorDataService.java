package com.inmobi.databus.distcp;


import com.inmobi.databus.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;

import java.net.*;
import java.util.*;
import java.io.File;

public class MirrorDataService extends AbstractCopier{
  private static final Log LOG = LogFactory.getLog(MirrorDataService.class);
  private FileSystem srcFs;
  private FileSystem destFs;
  private static final int DISTCP_SUCCESS = 0;


  public MirrorDataService(DatabusConfig config, DatabusConfig.Cluster srcCluster,
                           DatabusConfig.Cluster destinationCluster) {
    super(config, srcCluster, destinationCluster);
  }

  @Override
  protected void addStreamsToFetch() {

  }

  @Override
  protected long getRunIntervalInmsec() {
    return 60000; //1 min
  }

  @Override
  protected void fetch() throws Exception {
    //Assumption - Mirror is always of a merged Stream.
    //and there is only 1 instance of a merged Stream
    srcFs = FileSystem.get(new URI(getSrcCluster().getHdfsUrl()),
            getSrcCluster().getHadoopConf());
    destFs = FileSystem.get(
            new URI(getDestCluster().getHdfsUrl()),
            getDestCluster().getHadoopConf());

    Set<String> streamsToMirror = getStreamsToMirror();
    String srcStreamRoot = getSrcCluster().getFinalDestDirRoot();
    String destStreamRoot = getDestCluster().getFinalDestDirRoot();
    for (String stream : streamsToMirror) {
      String srcStreamPath = srcStreamRoot + File.separator + stream;
      String destStreamPath = destStreamRoot + File.separator + stream;
    //  LOG.info("Starting distcp Pull from + " srcStreamPath + " to destination");
    }

  }

  Set<String> getStreamsToMirror() {
    //all mirrored streams on this cluster
    Set<String> mirrorStreamsOnCluster =  getDestCluster().getMirroredStreams();
    Set<String> primaryStreamsOnSrc = getSrcCluster().getPrimaryStreams();
    //mirrored streams on this Cluster from this Source
    mirrorStreamsOnCluster.retainAll(primaryStreamsOnSrc);
    return mirrorStreamsOnCluster;
  }

}
