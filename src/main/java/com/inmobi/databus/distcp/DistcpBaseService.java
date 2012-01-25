/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.inmobi.databus.distcp;


import com.inmobi.databus.AbstractService;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public abstract class DistcpBaseService extends AbstractService {

  private final Cluster srcCluster;
  private final Cluster destCluster;
  private final FileSystem srcFs;
  private final FileSystem destFs;
  protected static final int DISTCP_SUCCESS = 0;

  protected static final Log LOG = LogFactory.getLog(MergedStreamService
          .class);

  public DistcpBaseService(DatabusConfig config, String name,
                           Cluster srcCluster,
                           Cluster destCluster) throws Exception {
    super(name + "_" +
            srcCluster.getName() + "_" + destCluster.getName(), config);
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    srcFs = FileSystem.get(new URI(srcCluster.getHdfsUrl()),
            srcCluster.getHadoopConf());
    destFs = FileSystem.get(
            new URI(destCluster.getHdfsUrl()), destCluster.getHadoopConf());
  }

  protected Cluster getSrcCluster() {
    return srcCluster;
  }


  protected Cluster getDestCluster() {
    return destCluster;
  }


  protected FileSystem getSrcFs() {
    return srcFs;
  }


  protected FileSystem getDestFs() {
    return destFs;
  }

  /*
  * return remote Path from where this consumer can consume
  * eg: MergedStreamConsumerService - Path eg:
  * hdfs://remoteCluster/databus/system/consumers/<consumerName>
  * eg: MirrorStreamConsumerService - Path eg:
  * hdfs://remoteCluster/databus/system/mirrors/<consumerName>
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

  protected void doFinalCommit(Map<Path, FileSystem> consumePaths) throws
          Exception {
    //commit distcp consume Path from remote cluster
    Set<Map.Entry<Path, FileSystem>> consumeEntries = consumePaths.entrySet();
    for (Map.Entry<Path, FileSystem> consumePathEntry : consumeEntries) {
      FileSystem fileSystem = consumePathEntry.getValue();
      Path consumePath = consumePathEntry.getKey();
      fileSystem.delete(consumePath);
      LOG.debug("Deleting [" + consumePath + "]");
    }

  }


  protected Path getInputFilePath(Map<Path, FileSystem> consumePaths,
                                  Path tmp) throws IOException {
    Path input = getInputPath();
    if (!srcFs.exists(input))
      return null;
    FileStatus[] fileList = srcFs.listStatus(input);
    if (fileList != null) {
      if (fileList.length > 1) {
        Set<String> sourceFiles = new HashSet<String>();
        //inputPath has have multiple files due to backlog
        //read all and create a tmp file
        for (int i = 0; i < fileList.length; i++) {
          Path consumeFilePath = fileList[i].getPath().makeQualified(srcFs);
          consumePaths.put(consumeFilePath, srcFs);
          FSDataInputStream fsDataInputStream = srcFs.open(consumeFilePath);
          while (fsDataInputStream.available() > 0) {
            String fileName = fsDataInputStream.readLine();
            if (fileName != null) {
              fileName = fileName.trim();
              sourceFiles.add(fileName);
            }
          }
          fsDataInputStream.close();
        }
        Path tmpPath = new Path(tmp, srcCluster.getName() + new Long(System
                .currentTimeMillis()).toString());
        FSDataOutputStream out = destFs.create(tmpPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter
                (out));
        for (String sourceFile : sourceFiles) {
          LOG.debug("Adding sourceFile [" + sourceFile + "] to distcp " +
                  "FinalList");
          writer.write(sourceFile);
          writer.write("\n");
        }
        writer.close();
        LOG.warn("Source File For distCP [" + tmpPath + "]");
        consumePaths.put(tmpPath.makeQualified(destFs), destFs);
        return tmpPath.makeQualified(destFs);
      } else if (fileList.length == 1) {
        Path consumePath = fileList[0].getPath().makeQualified(srcFs);
        consumePaths.put(consumePath, srcFs);
        return consumePath;
      } else {
        return null;
      }
    }
    return null;
  }
}
