package com.inmobi.databus.distcp;

import com.inmobi.databus.*;
import com.inmobi.databus.DatabusConfig.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.tools.*;

import java.io.*;
import java.util.*;

public class RemoteCopier extends DistcpBaseService {

  private static final Log LOG = LogFactory.getLog(RemoteCopier.class);

  public RemoteCopier(DatabusConfig config, Cluster srcCluster, Cluster destinationCluster) throws Exception{
    super(config, srcCluster, destinationCluster);
  }

  protected void addStreamsToFetch() {
    // nothing to do
  }

  @Override
  protected long getRunIntervalInmsec() {
    return 60000; //1 min
  }


  @Override
  public void fetch() throws Exception{
    try {
      boolean skipCommit = false;
      Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();

      Path tmpOut = new Path(getDestCluster().getTmpPath(), "distcp_" +
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
      LOG.warn("Starting a distcp pull from [" + inputFilePath.toString() + "] " +
              "Cluster [" + getSrcCluster().getHdfsUrl() + "]" +
              " to Cluster [" + getDestCluster().getHdfsUrl() + "] " +
              " Path [" + tmpOut.toString() + "]");

      String[] args = {"-f", inputFilePath.toString(),
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
      Map<String, Set<Path>> committedPaths;
      //if success
      if (!skipCommit) {
        Map<String, List<Path>> categoriesToCommit = prepareForCommit(tmpOut);
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          //category, Set of Paths to commit
          committedPaths = doLocalCommit(commitTime, categoriesToCommit);
        }
        //Prepare paths for MirrorDataService
        commitMirroredConsumerPaths(committedPaths, tmp);
        //Cleanup happens in parallel without sync
        //no race is there in consumePaths, tmpOut
        doFinalCommit(consumePaths);
      }
      //rmr tmpOut   cleanup
      destFs.delete(tmpOut, true);
      LOG.debug("Deleting [" + tmpOut + "]");
    }
    catch (Exception e) {
      LOG.warn(e);
      LOG.warn(e.getMessage());
      e.printStackTrace();
    }
  }

  /*
   * @param Map<String, Set<Path>> commitedPaths - Stream Name, It's committed Path.
   */
  private void commitMirroredConsumerPaths(Map<String, Set<Path>> committedPaths, Path tmp) throws Exception{
    //Map of Stream and clusters where it's mirrored
    Map<String, Set<Cluster>> mirrorStreamConsumers = new HashMap<String, Set<Cluster>>();
    Map<Path, Path> consumerCommitPaths = new LinkedHashMap<Path, Path>();
    //for each stream in committedPaths
    for(String stream : committedPaths.keySet()) {
      //for each cluster
      for (Cluster cluster : getConfig().getClusters().values()) {
        //is this stream mirrored on this cluster
        if (cluster.getMirroredStreams().contains(stream)) {
          Set<Cluster> mirrorConsumers = mirrorStreamConsumers.get(stream);
          if (mirrorConsumers == null)
            mirrorConsumers = new HashSet<Cluster>();
          mirrorConsumers.add(cluster);
          mirrorStreamConsumers.put(stream, mirrorConsumers);
        }
      }
    } //for each stream

    //Commit paths for each consumer
    for(String stream : committedPaths.keySet()) {
      //consumers for this stream
      Set<Cluster> consumers = mirrorStreamConsumers.get(stream);
      Path tmpConsumerPath;
      for (Cluster consumer : consumers) {
        //commit paths for this consumer, this stream
        //adding srcCluster avoids two Remote Copiers creating same filename
        String tmpPath = "src_" + getSrcCluster().getName() +
                "_via_"+ getDestCluster().getName() + "_mirrorto_" +
                consumer.getName() + "_" + stream;
        tmpConsumerPath = new Path(tmp, tmpPath);
        FSDataOutputStream out = destFs.create(tmpConsumerPath);
        for (Path path : committedPaths.get(stream)) {
          out.writeBytes(path.toString());
          out.writeBytes("\n");
        }
        out.close();
        //Two remote copiers will write file for same consumer within the same time
        //adding srcCLuster name avoids that conflict
        Path finalMirrorPath = new Path(getDestCluster().getMirrorConsumePath(consumer),
                tmpPath + "_" + new Long(System.currentTimeMillis()).toString());
        consumerCommitPaths.put(tmpConsumerPath, finalMirrorPath);

      } //for each consumer
    } //for each stream

    //Do the final mirrorCommit
    LOG.info("Committing " + consumerCommitPaths.size() + " paths.");
    FileSystem fs = FileSystem.get(getDestCluster().getHadoopConf());
    for (Map.Entry<Path, Path> entry : consumerCommitPaths.entrySet()) {
      LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
      fs.mkdirs(entry.getValue().getParent());
      fs.rename(entry.getKey(), entry.getValue());
    }
  }


  private void doFinalCommit(Map<Path, FileSystem> consumePaths) throws Exception{
    //commit distcp consume Path from remote cluster
    Set<Map.Entry<Path, FileSystem>> consumeEntries = consumePaths.entrySet();
    for(Map.Entry<Path, FileSystem> consumePathEntry : consumeEntries) {
      FileSystem fileSystem = consumePathEntry.getValue();
      Path consumePath = consumePathEntry.getKey();
      fileSystem.delete(consumePath);
      LOG.debug("Deleting [" + consumePath + "]");
    }

  }

  private Map<String, List<Path>> prepareForCommit(Path tmpOut) throws Exception{
    Map<String, List<Path>> categoriesToCommit = new HashMap<String, List<Path>>();
    FileStatus[] allFiles = destFs.listStatus(tmpOut);
    for(int i=0; i < allFiles.length; i++) {
      String fileName = allFiles[i].getPath().getName();
      if (fileName != null) {
        String category = getCategoryFromFileName(fileName);
        if (category != null) {
          Path intermediatePath = new Path(tmpOut, category);
          if (!destFs.exists(intermediatePath))
            destFs.mkdirs(intermediatePath);
          Path source = allFiles[i].getPath().makeQualified(destFs);

          Path intermediateFilePath =  new Path(intermediatePath.makeQualified(destFs).toString() + File.separator +
                  fileName);
          destFs.rename(source, intermediateFilePath);
          LOG.debug("Moving [" + source + "] to intermediateFilePath [" + intermediateFilePath + "]");
          List<Path> fileList = categoriesToCommit.get(category);
          if( fileList == null)  {
            fileList = new ArrayList<Path>();
            fileList.add(intermediateFilePath.makeQualified(destFs));
            categoriesToCommit.put(category, fileList);
          }
          else {
            fileList.add(intermediateFilePath);
          }
        }
      }
    }
    return categoriesToCommit;
  }

 /*
   * @returns Map<String, Set<Path>> - Map of StreamName, Set of paths committed for stream
   */
  private Map<String, Set<Path>> doLocalCommit(long commitTime, Map<String, List<Path>> categoriesToCommit) throws
          Exception {
    Map<String, Set<Path>> comittedPaths = new HashMap<String, Set<Path>>();
    Set<Map.Entry<String, List<Path>>> commitEntries = categoriesToCommit.entrySet();
    Iterator it = commitEntries.iterator();
    while(it.hasNext()) {
      Map.Entry<String, List<Path>> entry = (Map.Entry<String, List<Path>>) it.next();
      String category  =  entry.getKey();
      List<Path> filesInCategory = entry.getValue();
      for (Path filePath : filesInCategory) {
        Path destParentPath = new Path(getDestCluster().getFinalDestDir(category, commitTime));
        if(!destFs.exists(destParentPath))  {
          destFs.mkdirs(destParentPath);
        }
        destFs.rename(filePath, destParentPath);
        LOG.debug("Moving from intermediatePath [" + filePath + "] to [" + destParentPath + "]");
        Path commitPath = new Path(destParentPath, filePath.getName());
        Set<Path> commitPaths = comittedPaths.get(category);
        if (commitPaths == null) {
          commitPaths = new HashSet<Path>();
        }
        commitPaths.add(commitPath);
        comittedPaths.put(category, commitPaths);
      }
    }
    return comittedPaths;
  }


   protected Path getInputPath() throws IOException {
    return getSrcCluster().getConsumePath(getDestCluster());

  }
}
