package com.inmobi.databus.consumer;

import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;

public class ConsumerFactory {

  public static StreamingConsumer create(String configFile, String consumerName,
      String streamName, Checkpoint startCheckpoint) throws Exception {
    DatabusConfigParser configParser = new DatabusConfigParser(
        configFile);
    DatabusConfig config = configParser.getConfig();
    CheckpointProvider checkpointProvider = new ZKCheckpointProvider();
    return new StreamingConsumer(config, consumerName, streamName,
        startCheckpoint, checkpointProvider);
  }

  public static StreamingConsumer create(String configFile, String consumerName, 
      String streamName) throws Exception {
    return create(configFile, consumerName, streamName, null);
  }
}
