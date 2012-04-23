package com.inmobi.databus;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Stores the Checkpoint in the filesystem
 */
public class FSCheckpointProvider implements CheckpointProvider {

  private final FileSystem fs;
  private final Path baseDir;

  public FSCheckpointProvider(String dir) {
    this.baseDir = new Path(dir);
    try {
      fs = baseDir.getFileSystem(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] read(String key) {
    Path currentCheckpoint = new Path(baseDir, key + ".ck");
    byte[] buffer = null;
    try {
      if (!fs.exists(currentCheckpoint)) {
        return null;
      }
      BufferedInputStream in = new BufferedInputStream(
          fs.open(currentCheckpoint));
      buffer = new byte[in.available()];
      in.read(buffer);
      in.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return buffer;
  }

  @Override
  public void checkpoint(String key, byte[] checkpoint) {
    Path newCheckpoint = new Path(baseDir, key + ".ck.new");
    try {

      FSDataOutputStream out = fs.create(newCheckpoint);
      out.write(checkpoint);
      out.close();
      Path currentCheckpoint = new Path(baseDir, key + ".ck");
      fs.delete(currentCheckpoint);
      fs.rename(newCheckpoint, currentCheckpoint);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      fs.close();
    } catch (IOException e) {

    }
  }

}
