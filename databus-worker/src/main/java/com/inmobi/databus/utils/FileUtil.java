package com.inmobi.databus.utils;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class FileUtil {
  private static final Log LOG = LogFactory.getLog(FileUtil.class);

  public static void gzip(Path src, Path target, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream out = fs.create(target);
    GzipCodec gzipCodec = (GzipCodec) ReflectionUtils.newInstance(
              GzipCodec.class, conf);
    Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);
    OutputStream compressedOut = gzipCodec.createOutputStream(out,
              gzipCompressor);
    FSDataInputStream in = fs.open(src);
    try {
      IOUtils.copyBytes(in, compressedOut, conf);
    } catch (Exception e) {
      LOG.error("Error in compressing ", e);
    } finally {
      in.close();
      CodecPool.returnCompressor(gzipCompressor);
      compressedOut.close();
      out.close();
    }
  }
}
