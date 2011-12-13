package com.inmobi.databus.consume;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.GzipCodec;

public class TestCompression {

  public static void main(String[] args) throws Exception {
    FileOutputStream out = new FileOutputStream("/tmp/b");
    GzipCodec codec = new GzipCodec();
    OutputStream compressedOut = codec.createOutputStream(out);
    FileInputStream in = new FileInputStream("/tmp/a");
    byte[] bytes = new byte[256];
    while (in.read(bytes) != -1) {
      System.out.println("writing " + bytes);
      compressedOut.write(bytes);
    }
    in.close();
    compressedOut.close();
  }
}
