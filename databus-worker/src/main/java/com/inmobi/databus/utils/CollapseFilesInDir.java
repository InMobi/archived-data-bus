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
package com.inmobi.databus.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;


public class CollapseFilesInDir {
  private static Logger LOG = Logger.getLogger(CollapseFilesInDir.class);

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    configuration.set("fs.default.name", args[0]);
    String dir = args[1];
    FileSystem fs = FileSystem.get(configuration);
    FileStatus[] fileList = fs.listStatus(new Path(dir));
    if (fileList != null) {
      if (fileList.length > 1) {
        Set<Path> sourceFiles = new HashSet<Path>();
        Set<String> consumePaths = new HashSet<String>();
        //inputPath has have multiple files due to backlog
        //read all and create a tmp file
        for (int i = 0; i < fileList.length; i++) {
          Path consumeFilePath = fileList[i].getPath().makeQualified(fs);
          sourceFiles.add(consumeFilePath);
          FSDataInputStream fsDataInputStream = fs.open(consumeFilePath);
          while (fsDataInputStream.available() > 0) {
            String fileName = fsDataInputStream.readLine();
            if (fileName != null) {
              consumePaths.add(fileName.trim());
              System.out.println("Adding [" + fileName + "] to pull");
            }
          }
          fsDataInputStream.close();
        }
        Path finalPath = new Path(dir, new Long(System
                .currentTimeMillis()).toString());
        FSDataOutputStream out = fs.create(finalPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter
                (out));
        for (String consumePath : consumePaths) {
          System.out.println("Adding sourceFile [" + consumePath + "] to distcp " +
                  "FinalList");
          writer.write(consumePath);
          writer.write("\n");
        }
        writer.close();
        LOG.warn( "Final File - [" + finalPath + "]");
        for(Path deletePath : sourceFiles) {
          System.out.println("Deleting - [" + deletePath + "]");
          fs.delete(deletePath);
        }
      }
    }
  }
}
