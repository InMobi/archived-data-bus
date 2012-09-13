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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;

public abstract class DistcpBaseService extends AbstractService {

  private final Cluster srcCluster;
  private final Cluster destCluster;
  private final FileSystem srcFs;
  private final FileSystem destFs;
  protected static final int DISTCP_SUCCESS = DistCpConstants.SUCCESS;

  protected static final Log LOG = LogFactory.getLog(DistcpBaseService
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

  /**
   * Set Common or default DistCp options here.
   *
   * @param inputPathListing
   * @param target
   * @return options instance
   */

  protected DistCpOptions getDistCpOptions(Path inputPathListing, Path target) {
    DistCpOptions options = new DistCpOptions(inputPathListing, target);
    options.setBlocking(true);
    // If more command line options need to be passed to DistCP then, 
    // Create options instance using OptionParser.parse and set default options
    // on the returned instance.
    //with the arguments as sent in by the Derived Service
    return options;
  }

  protected Boolean executeDistCp(DistCpOptions options)
      throws Exception
  {
    //Add Additional Default arguments to the array below which gets merged
    //with the arguments as sent in by the Derived Service
    Configuration conf = destCluster.getHadoopConf();
    DistCp distCp = new DistCp(conf, options);
    try {
      distCp.execute();
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw e;
    }
    return true;
  }

  /*
  * @return remote Path from where this consumer can consume
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

  @Override
  public long getMSecondsTillNextRun(long currentTime) {
    long runIntervalInSec = (DEFAULT_RUN_INTERVAL/1000);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(new Date(currentTime));
    long currentSec = calendar.get(Calendar.SECOND);
    return (runIntervalInSec - currentSec) * 1000;
  }

  protected void doFinalCommit(Map<Path, FileSystem> consumePaths) throws
      Exception {
    //commit distcp consume Path from remote cluster
    Set<Map.Entry<Path, FileSystem>> consumeEntries = consumePaths.entrySet();
    for (Map.Entry<Path, FileSystem> consumePathEntry : consumeEntries) {
      FileSystem fileSystem = consumePathEntry.getValue();
      Path consumePath = consumePathEntry.getKey();
      fileSystem.delete(consumePath);
      LOG.debug("Deleting/Commiting [" + consumePath + "]");
    }

  }


  /*
   * @param Map<Path, FileSystem> consumePaths - list of paths which have to
   * be pulled populated by this function with the corresponding FileSystem
   * object associated with it
   * @param Path tmp - Temporary Location path on Cluster to where files have
   * to be pulled
   *
   * @return
   */
  protected Path getDistCPInputFile(Map<Path, FileSystem> consumePaths,
                                    Path tmp) throws IOException {
    Path input = getInputPath();
    if (!srcFs.exists(input))
      return null;
    //find all consumePaths which need to be pulled
    FileStatus[] fileList = srcFs.listStatus(input);
    if (fileList != null) {
      Set<String> sourceFiles = new HashSet<String>();
      if (fileList.length > 1) {
        /* inputPath has multiple files due to backlog
         * read all and create a Input file for DISTCP on destinationCluster as
         * an optimization so that distcp doesn't pull this file remotely
         */
        for (int i = 0; i < fileList.length; i++) {
          Path consumeFilePath = fileList[i].getPath().makeQualified(srcFs);
          /* put eachFile name in consumePaths
           * An example of data in consumePaths is
           * /databus/system/consumers/<cluster>/file1..and so on
           */
          consumePaths.put(consumeFilePath, srcFs);
          /*
           * for each consumePath read all minute level paths need to be
           * pulled and add them to sourceFiles
           */
          BufferedReader reader=null;
          try {
            FSDataInputStream fsDataInputStream = srcFs.open(consumeFilePath);
            reader = new BufferedReader(new InputStreamReader
                (fsDataInputStream));
            String fileName = reader.readLine();
            while (fileName != null) {
              fileName = fileName.trim();
              sourceFiles.add(fileName);
              fileName = reader.readLine();
            }
          } finally {
            if(reader != null)
              reader.close();
          }
        }
        Path tmpPath = createInputFileForDISCTP(destFs, srcCluster.getName(), tmp,
            sourceFiles);
        return getFinalPathForDistCP(tmpPath, consumePaths);

      } else if (fileList.length == 1) {
        /*
         * services are running in a streaming fashion,
         * read this file and only add VALID paths to distcp input
         */
        Path consumePath = fileList[0].getPath().makeQualified(srcFs);
        consumePaths.put(consumePath, srcFs);
        FSDataInputStream fsDataInputStream = srcFs.open(consumePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader
            (fsDataInputStream));
        String file = reader.readLine();
        while (file != null) {
          file = reader.readLine().trim();
          sourceFiles.add(file);
        }
        reader.close();
        Path tmpPath = createInputFileForDISCTP(destFs, srcCluster.getName(), tmp,
            sourceFiles);
        return getFinalPathForDistCP(tmpPath, consumePaths);
      }
    }
    return null;
  }

  /*
   * Helper function which returns the final path to be used as input forDistcp
   * Does cleanup of consumePaths at sourceCluster if they are INVALID
   */
  private Path getFinalPathForDistCP(Path tmpPath, Map<Path,
      FileSystem> consumePaths)throws IOException{
    if (tmpPath != null) {
      LOG.warn("Source File For distCP [" + tmpPath + "]");
      consumePaths.put(tmpPath.makeQualified(destFs), destFs);
      return tmpPath.makeQualified(destFs);
    } else {
      /*
      * no valid paths to return. Cleanup consumePaths
      */
      for (Map.Entry<Path, FileSystem> consumePath : consumePaths.entrySet()) {
        Path p = consumePath.getKey();
        FileSystem fs = consumePath.getValue();
        fs.delete(p);
        LOG.info("Deleting invalid consumePath[" + p + "]. ");
      }
      consumePaths.clear();
      return null;
    }
  }

  /*
   * Helper method for getDistCPInputFile
   * if none of the paths are VALID then it does not create an empty file on
   * <clusterName> but returns a null
   * @param FileSystem - where to create file i.e. srcFs or destFs
   * @param String - sourceCluster from where we are pulling files from
   * @param Path - tmpLocation on sourceCluster
   * @param Set<String> - set of sourceFiles need to be pulled
   */
  private Path createInputFileForDISCTP(FileSystem fs, String clusterName,
                                        Path tmp, Set<String> sourceFiles)
      throws IOException {
    Set<String> validFiles = new HashSet<String>();
    for (String sourceFile : sourceFiles) {
      /*
       * To avoid data-loss in all services we publish the paths to
       * consumers directory first before publishing on HDFS for
       * finalConsumption. In a distributed transaction failure it's
       * possible that some of these paths do not exist. Do isExistence
       * check before adding them as DISTCP input otherwise DISTCP
       * jobs can fail continously thereby blocking Merge/Mirror
       * stream to run further
       */
      Path p = new Path(sourceFile);
      if (fs.exists(p)) {
        LOG.info("Adding sourceFile [" + sourceFile + "] to distcp " +
            "FinalList");
        validFiles.add(sourceFile);
      } else {
        LOG.info("Skipping [" + sourceFile + "] to pull as it's an " +
            "INVALID PATH");
      }
    }

    if (validFiles.size() > 0) {
      BufferedWriter writer = null;
      Path tmpPath = null;
      try {
      tmpPath = new Path(tmp, clusterName + new Long(System
          .currentTimeMillis()).toString());
      FSDataOutputStream out = fs.create(tmpPath);
     writer = new BufferedWriter(new OutputStreamWriter
          (out));
      for (String validFile : validFiles) {
        writer.write(validFile);
        writer.write("\n");
      }
      } finally {
        writer.close();
      }
      return tmpPath;
    }
    return null;
  }


}
