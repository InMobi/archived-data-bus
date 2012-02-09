package com.inmobi.databus.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Queue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.DatabusConfig;

public class PartitionReader extends AbstractService {

  private final PartitionCheckpoint partition;
  private final Queue<QueueEntry> buffer;
  private final Path collectorDir;
  private final FileSystem fs;
  private final String streamName;
  private String currentFile;
  private String currentScribeFile;
  

  PartitionReader(PartitionCheckpoint partition, DatabusConfig config, 
      Queue<QueueEntry> buffer, String streamName) {
    super(partition.toString(), config, 1000);
    this.partition = partition;
    this.buffer = buffer;
    this.streamName = streamName;
    Path streamDir = new Path(partition.getId().getCluster().getDataDir(), streamName);
    this.collectorDir = new Path(streamDir, partition.getId().getCollector());
    this.currentFile = partition.getFileName();
    try {
      this.fs = FileSystem.get(partition.getId().getCluster().getHadoopConf());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    super.stop();
    thread.interrupt();
  }

  @Override
  protected void execute() throws Exception {
    //System.out.println("Reading more data uu");
    Path file = getNextFile();
    if (file == null) {
      return;
    }
    this.currentFile = file.getName();
    System.out.println("Reading file " + file);
    try {
      FSDataInputStream in = fs.open(file);
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line = reader.readLine();
      //System.out.println("readling line oo" + line);
      while (line != null) {
        buffer.add(new QueueEntry(new Message(line.getBytes()), partition.getId(), file
            .getName(), in.getPos()));
        line = reader.readLine();
        while (line == null) {
          Path current = new Path(collectorDir, streamName + "_current");
          FSDataInputStream inS = fs.open(current);
          String currentScribeFile = inS.readLine().trim();
          inS.close();
          if (currentFile.equals(currentScribeFile)) {
            Thread.sleep(1000);
            line = reader.readLine();
          } else {
            break;
          }
        }
      }
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private String getCurrentFile(FileStatus[] files) throws IOException{
    for (FileStatus fileStatus : files) {
      if (fileStatus.getPath().getName().endsWith("current")) {
        FSDataInputStream in = fs.open(fileStatus.getPath());
        String currentFileName = in.readLine().trim();
        in.close();
        return currentFileName;
      }
    }
    return null;
  }

  private Path getNextFile() throws Exception {
    return getFileList(currentFile, fs);
    //return new Path(
      //  "/databus/data/rtbi_metrics/gs3103.red.uj1.inmobi.com/rtbi_metrics-2012-02-08-10-35_00000");
  }

  private Path getFileList(String currentFileName, FileSystem fs)
      throws Exception {
    //System.out.println("collectordir " + collectorDir);
    FileStatus[] files = fs.listStatus(collectorDir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        if (p.getName().endsWith("current")
            || p.getName().equals("scribe_stats"))
          return false;
        return true;
      }
    });
    String[] fileNames = new String[files.length];
    int i = 0;
    for (FileStatus s : files) {
      
      fileNames[i++] = s.getPath().getName();
    }

    //System.out.println("files " + files);
    Arrays.sort(fileNames);
    if (currentFileName == null) {
      return files[0].getPath();
    }
    int currentFileIndex;
    currentFileIndex = Arrays.binarySearch(fileNames, currentFileName);
    if (currentFileIndex == (files.length - 1)) {
      return null;
    }
    return files[++currentFileIndex].getPath();

  }

  /*class PathComparator implements Comparator {

    @Override
    public int compare(Object o, Object o1) {
      FileStatus file1 = (FileStatus) o;
      FileStatus file2 = (FileStatus) o1;
      if (file1.getPath().getName().compareTo(file2.getPath().getName()) > 0)
        return 1;
      else if (file1.getPath().getName().compareTo(file2.getPath().getName()) < 0) {
        return -1;
      } else
        return 0;
    }
  }*/
}
