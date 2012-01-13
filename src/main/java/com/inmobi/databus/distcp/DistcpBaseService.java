package com.inmobi.databus.distcp;


import com.inmobi.databus.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.*;
import java.util.*;

public abstract  class DistcpBaseService extends AbstractCopier{
  protected FileSystem srcFs;
  protected FileSystem destFs;
  protected static final int DISTCP_SUCCESS = 0;

  protected static final Log LOG = LogFactory.getLog(RemoteCopier.class);

  public DistcpBaseService(DatabusConfig config, DatabusConfig.Cluster srcCluster, DatabusConfig.Cluster destCluster)
          throws Exception {
    super(config, srcCluster, destCluster);
    srcFs = FileSystem.get(new URI(getSrcCluster().getHdfsUrl()),
            getSrcCluster().getHadoopConf());
    destFs = FileSystem.get(
            new URI(getDestCluster().getHdfsUrl()),
            getDestCluster().getHadoopConf());
  }

  /*
   * return remote Path from where this consumer can consume
   * eg: RemoteCopier - Path eg: hdfs://remoteCluster/databus/system/consumers/<consumerName>
   * eg: MirrorDataService - Path eg: hdfs://remoteCluster/databus/system/mirrors/<consumerName>
   */
  protected abstract Path getInputPath() throws IOException;

  protected String getCategoryFromFileName(String fileName) {
    LOG.debug("Splitting [" + fileName + "] on -");
    if (fileName != null && fileName.length() > 1 && fileName.contains("-")) {
      StringTokenizer tokenizer = new StringTokenizer(fileName, "-");
      tokenizer.nextToken(); //skip collector name
      String catgeory = tokenizer.nextToken();
      return catgeory;
    }
    return null;
  }


  protected Path getInputFilePath(Map<Path, FileSystem> consumePaths, Path tmp) throws IOException {
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
          consumePaths.put(consumeFilePath, srcFs);
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
        //two remote copiers can create the same tmpPath, putting srcCluster avoids the conflict
        Path tmpPath = new Path(tmp, getSrcCluster().getName() + new Long(System
                .currentTimeMillis()).toString());
        FSDataOutputStream out = destFs.create(tmpPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
        for(String sourceFile: sourceFiles) {
          LOG.debug("Adding sourceFile [" + sourceFile + "] to distcp FinalList");
          writer.write(sourceFile);
          writer.write("\n");
        }
        writer.close();
        LOG.warn("Source File For distCP [" + tmpPath + "]");
        consumePaths.put(tmpPath.makeQualified(destFs), destFs);
        return tmpPath.makeQualified(destFs);
      }
      else if(fileList.length == 1) {
        Path consumePath = fileList[0].getPath().makeQualified(srcFs);
        consumePaths.put(consumePath, srcFs);
        return consumePath;
      }
      else {
        return null;
      }
    }
    return  null;
  }
}
