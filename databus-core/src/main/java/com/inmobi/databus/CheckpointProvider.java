package com.inmobi.databus;


public interface CheckpointProvider {

  byte[] read(String key);

  void checkpoint(String key, byte[] checkpoint);

  void close();
}
