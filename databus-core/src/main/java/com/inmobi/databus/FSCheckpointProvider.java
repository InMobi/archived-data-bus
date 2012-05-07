package com.inmobi.databus;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Stores the Checkpoint in the filesystem
 */
public class FSCheckpointProvider implements CheckpointProvider {
  private static final Log LOG = LogFactory.getLog(FSCheckpointProvider.class);

  private final FileSystem fs;
  private final Path baseDir;

  public FSCheckpointProvider(String dir) {
    this.baseDir = new Path(dir);
    try {
      fs = baseDir.getFileSystem(new Configuration());
      if (baseDir != null && !fs.exists(baseDir)) {
        fs.create(baseDir);
      }
    } catch (IOException e) {
      LOG.warn("Could not initialize checkpoint provider", e);
      throw new RuntimeException(e);
    }
    LOG.info("CheckPoint provider initialized with baseDir: " + baseDir);
  }

  @Override
  public byte[] read(String key) {
    Path currentCheckpoint = getCheckpointPath(key);
    byte[] buffer = null;
    try {
      LOG.info("checkpoint path:" + currentCheckpoint);
      if (!fs.exists(currentCheckpoint)) {
        LOG.info("No checkpoint to read");
        return null;
      }
      BufferedInputStream in = new BufferedInputStream(
      fs.open(currentCheckpoint));
      buffer = new byte[in.available()];
      in.read(buffer);
      in.close();
    } catch (IOException e) {
      LOG.warn("Could not read checkpoint ", e);
      throw new RuntimeException(e);
    }
    return buffer;
  }

  private Path getCheckpointPath(String key) {
    return new Path(baseDir, key + ".ck");
  }

  private Path getNewCheckpointPath(String key) {
    return new Path(baseDir, key + ".ck.new");
  }

  @Override
  public void checkpoint(String key, byte[] checkpoint) {
    Path newCheckpoint = getNewCheckpointPath(key);
    try {

      FSDataOutputStream out = fs.create(newCheckpoint);
      out.write(checkpoint);
      out.close();
      Path currentCheckpoint = getCheckpointPath(key);
      fs.delete(currentCheckpoint, true);
      fs.rename(newCheckpoint, currentCheckpoint);
      LOG.info("checkpoint created at " + currentCheckpoint);
    } catch (IOException e) {
      LOG.warn("Could not checkpoint ", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      fs.close();
    } catch (IOException e) {
      LOG.warn("Could not close ", e);
    }
  }

}
