package com.inmobi.databus.consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.Service;


public class StreamingConsumer implements Service {

  private final DatabusConfig config;
  private final String streamName;
  private final String consumerName;
  private final CheckpointProvider checkpointProvider;
  private final BlockingQueue<QueueEntry> buffer =
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Checkpoint startCheckpoint;
  private Checkpoint currentCheckpoint;
  
  private final Map<PartitionId, PartitionReader> readers =
      new HashMap<PartitionId, PartitionReader>();

  StreamingConsumer(DatabusConfig config, String consumerName,
      String streamName, Checkpoint startCheckpoint, 
      CheckpointProvider checkpointProvider) {
    this.config = config;
    this.consumerName = consumerName;
    this.streamName = streamName;
    this.checkpointProvider = checkpointProvider;
    this.startCheckpoint = startCheckpoint;
  }

  public synchronized Message next() throws InterruptedException {
    QueueEntry entry = buffer.take();
    currentCheckpoint.set(entry.partitionId, 
        new PartitionCheckpoint(entry.partitionId, 
            entry.fileName, entry.offset));
    return entry.message;
  }

  public synchronized Checkpoint getCurrentCheckpoint() {
    return currentCheckpoint;
  }

  public synchronized void rollback() {
    //restart the service, consumer will start streaming from the last saved
    //checkpoint
    stop();
    start();
  }

  public synchronized void commit() {
    checkpointProvider.checkpoint(consumerName, streamName, 
        currentCheckpoint);
  }

  @Override
  public synchronized void start() {
    if (startCheckpoint != null) {
      this.currentCheckpoint = startCheckpoint;
    } else {
      this.currentCheckpoint = 
          checkpointProvider.read(consumerName, streamName);
      if (currentCheckpoint == null) {
        Map<PartitionId, PartitionCheckpoint> partitionsChkPoints =
            new HashMap<PartitionId, PartitionCheckpoint>();
        this.currentCheckpoint = new Checkpoint(partitionsChkPoints);
        for (String c : config.getSourceStreams().get(streamName).getSourceClusters()) {
          Cluster cluster = config.getClusters().get(c);
          try {
            //System.out.println("----here 11---");
            FileSystem fs = FileSystem.get(cluster.getHadoopConf());
            Path path = new Path(cluster.getDataDir(), streamName);
            System.out.println(path);
            FileStatus[] list = fs.listStatus(path);
            for (FileStatus status : list) {
              String collector = status.getPath().getName();
              System.out.println("collector is " + collector);
              PartitionId id = new PartitionId(cluster, collector);
              partitionsChkPoints.put(id, new PartitionCheckpoint(id, null, -1));
            }
          } catch(IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    for (PartitionCheckpoint partition : 
              getCurrentCheckpoint().getPartitionsCheckpoint().values()) {
      PartitionReader reader = new PartitionReader(partition, config, buffer, 
          streamName);
      readers.put(partition.getId(), reader);
      System.out.println("Starting partition reader " + partition.getId());
      reader.start();
    }
  }

  @Override
  public synchronized void stop() {
    for (PartitionReader reader : readers.values()) {
      reader.stop();
    }
  }

  @Override
  public synchronized void join() throws Exception {
    for (PartitionReader reader : readers.values()) {
      reader.join();
    }
  }
}
