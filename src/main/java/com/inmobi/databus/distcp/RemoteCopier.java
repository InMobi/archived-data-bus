package com.inmobi.databus.distcp;

import com.inmobi.databus.*;
import com.inmobi.databus.DatabusConfig.*;
import com.inmobi.databus.utils.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.tools.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class RemoteCopier extends AbstractCopier {

  private static final Log LOG = LogFactory.getLog(RemoteCopier.class);

  private FileSystem srcFs;
  private FileSystem destFs;
  private static final int DISTCP_SUCCESS = 0;


  public RemoteCopier(DatabusConfig config, Cluster srcCluster, Cluster destinationCluster) {
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


      srcFs = FileSystem.get(new URI(getSrcCluster().getHdfsUrl()),
              getSrcCluster().getHadoopConf());
      destFs = FileSystem.get(
              new URI(getDestCluster().getHdfsUrl()),
              getDestCluster().getHadoopConf());
      Set<Path> consumePaths = new HashSet<Path>();
      Path inputFilePath = getInputFilePath(consumePaths);

      if(inputFilePath == null) {
        LOG.warn("No data to pull from " +
                "Cluster [" + getSrcCluster().getHdfsUrl() + "]" +
                " to Cluster [" + getDestCluster().getHdfsUrl() + "]");
        return;
      }

      Path tmpOut = new Path(getDestCluster().getTmpPath(), "distcp-" +
              getSrcCluster().getName() + CalendarHelper.getCurrentDayTimeAsString()).makeQualified(destFs);
      LOG.warn("Starting a distcp pull from [" + inputFilePath.toString() + "] " +
              "Cluster [" + getSrcCluster().getHdfsUrl() + "]" +
              " to Cluster [" + getDestCluster().getHdfsUrl() + "] " +
              " Path [" + tmpOut.toString() + "]");
      destFs.mkdirs(tmpOut);

      String[] args = {"-f", inputFilePath.toString(),
              tmpOut.toString()};
      try {
        int exitCode = DistCp.runDistCp(args, getDestCluster().getHadoopConf());
        if (exitCode != DISTCP_SUCCESS)
          skipCommit = true;
      }
      catch(Throwable e) {
        LOG.warn(e.getMessage());
        LOG.warn(e);
        LOG.warn("Problem in distcp skipping commit");
        destFs.delete(tmpOut, true);
        skipCommit = true;
      }
      Map<String, Set<Path>> commitedPaths;
      //if success
      if (!skipCommit) {
        Map<String, List<Path>> categoriesToCommit = prepareForCommit(tmpOut);
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          //category, Set of Paths to commit
          commitedPaths = doLocalcommit(commitTime, categoriesToCommit);
        }
        //Prepare paths for MirrorDataService
        commitMirroredConsumerPaths(commitedPaths);
        //Cleanup happens in parallel without sync
        //no race is there in consumePaths, tmpOut
        doFinalCommit(consumePaths, tmpOut);
      }
    } catch (Exception e) {
      LOG.warn(e);
      LOG.warn(e.getMessage());
      e.printStackTrace();
    }
  }

  private void commitMirroredConsumerPaths(Map<String, Set<Path>> commitedPaths) throws Exception{
    //Map of Stream and clusters where it's mirrored
    Map<String, Set<Cluster>> mirrorStreamConsumers = new HashMap<String, Set<Cluster>>();
    Map<Path, Path> consumerCommitPaths = new LinkedHashMap<Path, Path>();
    //for each stream in committedPaths
    for(String stream : commitedPaths.keySet()) {
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
    for(String stream : commitedPaths.keySet()) {
      //consumers for this stream
      Set<Cluster> consumers = mirrorStreamConsumers.get(stream);
      Path tmpConsumerPath;
      for (Cluster consumer : consumers) {
        //commit paths for this consumer, this stream
         tmpConsumerPath = new Path(getDestCluster().getTmpPath(), "src-" + getSrcCluster().getName() +
                 "-via-"+ getDestCluster().getName() + "-mirrorto-" +
                consumer.getName() + "-" + stream);
        FSDataOutputStream out = destFs.create(tmpConsumerPath);
        for (Path path : commitedPaths.get(stream)) {
          out.writeBytes(path.toString());
          out.writeBytes("\n");
        }
        out.close();
        synchronized (getDestCluster()) {
        Path finalMirrorPath = new Path(getDestCluster().getMirrorConsumePath(consumer),
                new Long(System.currentTimeMillis()).toString());
         consumerCommitPaths.put(tmpConsumerPath, finalMirrorPath);
          //Two remote copiers will write file for same consumer within the same time
          //sleep for 1msec to avoid filename conflict
          Thread.sleep(1);
        }
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


  private void doFinalCommit(Set<Path> consumePaths, Path tmpOut) throws Exception{
    //commit distcp consume Path from remote cluster
    for(Path consumePath : consumePaths) {
      srcFs.delete(consumePath);
      LOG.debug("Deleting [" + consumePath + "]");
    }
    //rmr tmpOut   cleanup
    destFs.delete(tmpOut, true);
    LOG.debug("Deleting [" + tmpOut + "]");

  }

  private Map<String, List<Path>> prepareForCommit(Path tmpOut) throws Exception{
    Map<String, List<Path>> categoriesToCommit = new HashMap<String, List<Path>>();
    FileStatus[] allFiles = destFs.listStatus(tmpOut);
    for(int i=0; i < allFiles.length; i++) {
      String fileName = allFiles[i].getPath().getName();
      String category = getCategoryFromFileName(fileName);

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
    return categoriesToCommit;
  }

  private Path getInputFilePath(Set<Path> consumePaths) throws IOException {
    Path input = getInputPath();
    if (!srcFs.exists(input))
      return null;
    FileStatus[] fileList = srcFs.listStatus(input);
    if (fileList != null) {
      if(fileList.length > 1) {
        Set<String> sourceFiles = new HashSet<String>();
        //inputPath has have multiple files due to backlog
        //read all and create a tmp file
        for(int i=0; i < fileList.length; i++) {
          Path consumeFilePath = fileList[i].getPath().makeQualified(srcFs);
          consumePaths.add(consumeFilePath);
          FSDataInputStream fsDataInputStream = srcFs.open(consumeFilePath);
          while (fsDataInputStream.available() > 0 ){
            String fileName = fsDataInputStream.readLine();
            if (fileName != null) {
              fileName = fileName.trim();
              sourceFiles.add(fileName);
            }
          }
          fsDataInputStream.close();
        }
        Path tmpPath = new Path(getDestCluster().getTmpPath(), CalendarHelper.getCurrentDayTimeAsString());
        FSDataOutputStream out = destFs.create(tmpPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
        for(String sourceFile: sourceFiles) {
          LOG.debug("Adding sourceFile [" + sourceFile + "] to distcp FinalList");
          writer.write(sourceFile);
          writer.write("\n");
        }
        writer.close();
        LOG.warn("Source File For distCP [" + tmpPath + "]");
        return tmpPath.makeQualified(destFs);
      }
      else if(fileList.length == 1) {
        Path consumePath = fileList[0].getPath().makeQualified(srcFs);
        consumePaths.add(consumePath);
        return consumePath;
      }
      else {
        return null;
      }
    }
    return  null;
  }

  /*
   * @returns Map<String, Set<Path>> - Map of StreamName, Set of paths committed for stream
   */
  private Map<String, Set<Path>> doLocalcommit(long commitTime, Map<String, List<Path>> categoriesToCommit) throws
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
        Path commitPath = new Path(destParentPath, filePath.getName());
        Set<Path> commitPaths;
        if (comittedPaths.get(category) == null) {
          commitPaths = new HashSet<Path>();
          commitPaths.add(commitPath);
          comittedPaths.put(category, commitPaths);
        }
        else {
          comittedPaths.get(category).add(commitPath);
        }
        LOG.debug("Moving from intermediatePath [" + filePath + "] to [" + destParentPath + "]");
      }
    }
    return comittedPaths;
  }



  private String getCategoryFromFileName(String fileName) {
    StringTokenizer tokenizer = new StringTokenizer(fileName, "-");
    tokenizer.nextToken(); //skip collector name
    String catgeory = tokenizer.nextToken();
    return catgeory;

  }

  private Path getInputPath() throws IOException {
    return getSrcCluster().getConsumePath(getDestCluster());

  }
}
