package com.inmobi.databus.consumer;

public class ConsoleStreamer {

  public static void main(String[] args) throws Exception {
    String config = args[0];
    String consumerName = args[1];
    String stream = args[2];
    StreamingConsumer consumer = ConsumerFactory.create(
        config, consumerName, stream);
    consumer.start();
    
    while (true) {
      Message msg = consumer.next();
      System.out.println("MESSAGE:" + new String(msg.getData()));
    }
  }
}
