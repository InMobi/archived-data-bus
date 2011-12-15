package com.inmobi.databus;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.consume.DataConsumer;
import com.inmobi.databus.distcp.RemoteCopier;

public class Databus {
  private DatabusConfig config;

  public Databus() {
    this.config = new DatabusConfig();
  }

  public void start() throws Exception {
    List<AbstractCopier> copiers = new ArrayList<AbstractCopier>();
    for (Cluster c : config.getClusters().values()) {
      AbstractCopier copier = null; 
      if (c.equals(config.getDestinationCluster())) {
        copier = new DataConsumer(config);
      } else {
        copier = new RemoteCopier(config, c);
      }
      copiers.add(copier);
      copier.start();
    }

    for (AbstractCopier copier : copiers) {
      copier.join();
    }
    
    //cleanup
    FileSystem fs = FileSystem.get(config.getHadoopConf());
    fs.delete(config.getTmpPath());
  }

  public static void main(String[] args) throws Exception {
    Databus databus = new Databus();
    databus.start();
  }
}
