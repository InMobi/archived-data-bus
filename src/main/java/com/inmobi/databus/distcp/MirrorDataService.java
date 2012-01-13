package com.inmobi.databus.distcp;


import com.inmobi.databus.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.tools.*;

import java.io.*;
import java.util.*;

public class MirrorDataService extends DistcpBaseService{
  private static final Log LOG = LogFactory.getLog(MirrorDataService.class);

  public MirrorDataService(DatabusConfig config, DatabusConfig.Cluster srcCluster,
                           DatabusConfig.Cluster destinationCluster) throws Exception{
    super(config, srcCluster, destinationCluster);
  }

  @Override
  protected Path getInputPath() throws IOException {
    return getSrcCluster().getMirrorConsumePath(getDestCluster());
  }

  @Override
  protected void addStreamsToFetch() {
    //do nothing
  }

  @Override
  protected long getRunIntervalInmsec() {
    return 60000; //1 min
  }

  @Override
  protected void fetch() throws Exception {
    /* Assumption - Mirror is always of a merged Stream.There is only 1 instance of a merged Stream
    * 1 Mirror Thread per src DatabusConfig.Cluster from where streams need to be mirrored on destCluster
    *
    */
    try {
      boolean skipCommit = false;
      Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();

      Path tmpOut = new Path(getDestCluster().getTmpPath(), "distcp_mirror" +
              getSrcCluster().getName() + "_" + getDestCluster().getName()).makeQualified(destFs);
      //CleanuptmpOut before every run
      if (destFs.exists(tmpOut))
        destFs.delete(tmpOut, true);
      if (!destFs.mkdirs(tmpOut)) {
        LOG.warn("Cannot create [" + tmpOut + "]..skipping this run");
        return;
      }
      Path tmp = new Path(tmpOut, "tmp");
      if (!destFs.mkdirs(tmp)) {
        LOG.warn("Cannot create [" + tmp + "]..skipping this run");
        return;
      }
      Path inputFilePath = getInputFilePath(consumePaths, tmp);
      if(inputFilePath == null) {
        LOG.warn("No data to pull from " +
                "Cluster [" + getSrcCluster().getHdfsUrl() + "]" +
                " to Cluster [" + getDestCluster().getHdfsUrl() + "]");
        return;
      }

      LOG.warn("Starting a Mirrored distcp pull from [" + inputFilePath.toString() + "] " +
              "Cluster [" + getSrcCluster().getHdfsUrl() + "]" +
              " to Cluster [" + getDestCluster().getHdfsUrl() + "] " +
              " Path [" + tmpOut.toString() + "]");

      String[] args = {"-preserveSrcPath", "-f", inputFilePath.toString(),
              tmpOut.toString()};
       try {
        int exitCode = DistCp.runDistCp(args, getDestCluster().getHadoopConf());
        if (exitCode != DISTCP_SUCCESS)
          skipCommit = true;
      }
      catch(Throwable e) {
        LOG.warn(e.getMessage());
        e.printStackTrace();
        LOG.warn("Problem in distcp skipping commit");
        destFs.delete(tmpOut, true);
        skipCommit = true;
      }
      if (!skipCommit) {
        //Commit Paths in time ordered way
      }




    }
    catch (Exception e) {
      LOG.warn(e);
      LOG.warn(e.getMessage());
      e.printStackTrace();
    }


  }
}
