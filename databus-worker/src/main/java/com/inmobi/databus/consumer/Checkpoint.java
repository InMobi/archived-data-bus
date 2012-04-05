package com.inmobi.databus.consumer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class Checkpoint implements Writable {

  //map of partitionId to partition
  private final Map<PartitionId, PartitionCheckpoint> partitionsChkPoint;

  Checkpoint(Map<PartitionId, PartitionCheckpoint> partitionsChkPoint) {
    this.partitionsChkPoint = partitionsChkPoint;
  }

  public Map<PartitionId, PartitionCheckpoint> getPartitionsCheckpoint() {
    return partitionsChkPoint;
  }

  void set(PartitionId partitionId, PartitionCheckpoint partCheckpoint) {
    partitionsChkPoint.put(partitionId, partCheckpoint);
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // TODO Auto-generated method stub
    
  }
}
