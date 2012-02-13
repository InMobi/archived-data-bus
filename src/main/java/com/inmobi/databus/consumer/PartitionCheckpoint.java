package com.inmobi.databus.consumer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.inmobi.databus.Cluster;

public class PartitionCheckpoint implements Writable {
  private final PartitionId id;
  private final String fileName;
  private final long offset;

  PartitionCheckpoint(Cluster cluster, String collector, String fileName,
      long offset) {
    this(new PartitionId(cluster, collector), fileName, offset);
  }

  PartitionCheckpoint(PartitionId id, String fileName,
      long offset) {
    this.id = id;
    this.fileName = fileName;
    this.offset = offset;
  }

  public PartitionId getId() {
    return id;
  }

  public String getFileName() {
    return fileName;
  }
  public long getOffset() {
    return offset;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // TODO Auto-generated method stub
    
  }
  @Override
  public void write(DataOutput arg0) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  public String toString() {
    return "";
  }
}
