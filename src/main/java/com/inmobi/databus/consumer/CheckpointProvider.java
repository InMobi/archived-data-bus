package com.inmobi.databus.consumer;

public interface CheckpointProvider {

  Checkpoint read(String consumer, String streamName);

  void checkpoint(String consumer, String streamName, Checkpoint checkpoint);
}
