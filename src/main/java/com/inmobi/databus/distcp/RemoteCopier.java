package com.inmobi.databus.distcp;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.datamovement.CalendarHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.tools.DistCp;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.*;
import java.io.File;

public class RemoteCopier extends AbstractCopier {

  private static final Log LOG = LogFactory.getLog(RemoteCopier.class);

  private FileSystem srcFs;
  private FileSystem destFs;
  private static final int DISTCP_SUCCESS = 0;


  public RemoteCopier(DatabusConfig config, Cluster srcCluster, Cluster destinationCluster) {
    super(config, srcCluster, destinationCluster);
  }

  protected void addStreamsToFetch() {

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

      //if success
      if (!skipCommit) {
        Map<String, List<Path>> categoriesToCommit = prepareForCommit(tmpOut);
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          commit(tmpOut, consumePaths, commitTime, categoriesToCommit);
        }
      }
    } catch (Exception e) {
      LOG.warn(e);
      LOG.warn(e.getMessage());
      e.printStackTrace();
    }
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
        consumePaths.add(tmpPath.makeQualified(destFs));
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

  private void commit(Path tmpOut, Set<Path> consumePaths,
                      long commitTime, Map<String, List<Path>> categoriesToCommit) throws Exception {
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
      }
    }
    //commit distcp
    for(Path consumePath : consumePaths) {
      srcFs.delete(consumePath);
      LOG.debug("Deleting [" + consumePath + "]");
    }
    //rmr tmpOut   cleanup
    destFs.delete(tmpOut, true);
    LOG.debug("Deleting [" + tmpOut + "]");

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
