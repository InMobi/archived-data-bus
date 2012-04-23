package com.inmobi.databus;

/*
 * Interface to store and retrieve checkpoints.
 */
public interface CheckpointProvider {

  /*
   * Read the checkpoint for the given key. If no checkpoint is found, null
   * is returned.
   */
  byte[] read(String key);

  /*
   * Stores the checkpoint for the given key.
   */
  void checkpoint(String key, byte[] checkpoint);

  /*
   * Closes the provider.
   */
  void close();
}
