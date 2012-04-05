package com.inmobi.databus.consumer;

public class Message {

  private final byte[] data;
  //todo: any headers can go in here
  
  Message(byte[] data) {
    this.data = data;
  }

  public byte[] getData() {
    return data;
  }
}
