package com.inmobi.databus.distcp;


import com.inmobi.databus.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.tools.*;

import java.io.*;
import java.util.*;

/* Assumption - Mirror is always of a merged Stream.There is only 1 instance of a merged Stream
* (i)   1 Mirror Thread per src DatabusConfig.Cluster from where streams need to be mirrored on destCluster
* (ii)  Mirror stream and mergedStream can't coexist on same Cluster
* (iii) Mirror stream and merged Stream threads don't race with each other as they work on different
* streams based on assumption(ii)
*/

public class MirrorStreamConsumerService extends DistcpBaseService{
  private static final Log LOG = LogFactory.getLog(MirrorStreamConsumerService.class);

  public MirrorStreamConsumerService(DatabusConfig config, DatabusConfig.Cluster srcCluster,
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

    try {
      boolean skipCommit = false;
      Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();

      Path tmpOut = new Path(getDestCluster().getTmpPath(), "distcp_mirror_" +
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
        LOG.warn("Problem in Mirrored distcp..skipping commit for this run");
        destFs.delete(tmpOut, true);
        skipCommit = true;
      }
      if (!skipCommit) {
        Path srcPath = prepareForCommit(tmpOut);
        Path commitPath = new Path(getDestCluster().getRootDir());
        doLocalCommit(srcPath, commitPath);
        doFinalCommit(consumePaths);
      }
      destFs.delete(tmpOut, true);
      LOG.debug("Cleanup [" + tmpOut + "]");
    }
    catch (Exception e) {
      LOG.warn(e);
      LOG.warn(e.getMessage());
      e.printStackTrace();
    }
  }

  void doLocalCommit(Path srcPath, Path destPath) throws Exception{
    LOG.debug("Renaming ["+ srcPath + "] to ["+ destPath + "]");
    destFs.rename(srcPath, destPath);
  }

  /*
   * @param Path - Path where mirrored files are present
   * @returns Path - commitPath
   */
  Path prepareForCommit(Path tmpOut) {
    //tmpOut would be like - /databus/system/tmp/distcp_mirror_<srcCluster>_<destCluster>/
    //After distcp paths inside tmpOut would be
    // eg: /databus/system/distcp_mirror_ua1_uj1/databus/streams/metric_billing/2012/1/13/15/7
    //commitPath eg: /databus/system/distcp_mirror_ua1_uj1/databus/streams/
    LOG.debug("tmpOut [" + tmpOut + "]");
    Path commitPath = new Path(tmpOut, getSrcCluster().getUnqaulifiedFinalDestDirRoot());
    LOG.debug("CommitPath [" + commitPath + "]");
    return commitPath;
  }
}
