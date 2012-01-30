package com.inmobi.databus;


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
