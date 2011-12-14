package com.inmobi.databus.distcp;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;

import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.consume.ConsumerJob;

public class ParallelDistCp {

  private static final Log LOG = LogFactory.getLog(ParallelDistCp.class);
  private DatabusConfig databusConf;
  private FileSystem fs;

  public ParallelDistCp(DatabusConfig databusConf, Configuration conf) 
      throws IOException {
    this.databusConf = databusConf;
    this.fs = FileSystem.get(conf);
  }

  public void start() throws IOException {
    FileStatus[] fileStatus = 
        fs.listStatus(new Path(ConsumerJob.DISTCP_INPUT));
    for (FileStatus status : fileStatus) {
      Thread thread = new Thread(new DistcpRunnable(status.getPath()));
      thread.start();
    }
  }


  class DistcpRunnable implements Runnable {
    Path input;
    DistcpRunnable(Path input) {
      this.input = input;
    }

    @Override
    public void run() {
      Path lock = null;
      try {
        lock = new Path(input, ".lock");
        if (fs.exists(lock)) {
          LOG.info("Distcp for destination " + input.getName() + 
              " in progress.");
          return;
        }
        fs.mkdirs(lock);
        
        //TODO: create the args
        String[] args = new String[]{
            "-preserveSrcPath",
            
            };
        DistCp.main(args);
        
        //TODO:copy all done files to destination cluster

        //remove the distcp input
        fs.delete(input);
      } catch (IOException e) {
        LOG.error("Distcp failed for destination input " + input.getName(),
            e);
        //TODO: delete the lock file so it is retried in next run
        try {
          fs.delete(lock);
        } catch (IOException e1) {
          LOG.error("Could not delete lock file.", e1);
        }
      }
    }
    
  }
}
