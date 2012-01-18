package com.inmobi.databus;


import org.apache.log4j.Logger;

public class DatabusShutdownHook implements Runnable {
  private static Logger LOG = Logger.getLogger(Databus.class);

  final Databus databus;
  public DatabusShutdownHook(Databus databus) {
    this.databus = databus;
  }

  @Override
  public void run(){
    try {
      databus.stop();
      LOG.info("Databus shutdown complete");
    } catch (Exception e) {
      LOG.warn("Error in Databus shutdown");
    }
  }
}

