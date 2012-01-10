package com.inmobi.databus;

import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.DatabusConfig.Stream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractCopier implements Runnable {

  private static final Log LOG = LogFactory.getLog(AbstractCopier.class);

  private final Cluster srcCluster;
  private final Cluster destCluster;
  private final String name;
  protected DatabusConfig config;
  protected final Set<Stream> streamsToFetch = new HashSet<Stream>();
  protected Thread thread;
  protected volatile boolean stopped = false;



  public AbstractCopier(DatabusConfig config, Cluster srcCluster, 
      Cluster destCluster) {
    this.config = config;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;

		this.name = getClass().getName() + "_" +
           srcCluster.getName() + "_" + destCluster.getName();
    addStreamsToFetch();
  }

  public Cluster getSrcCluster() {
    return srcCluster;
  }

  public Cluster getDestCluster() {
    return destCluster;
  }

  public DatabusConfig getConfig() {
    return config;
  }

  public String getName() {
    return name;
  }

  protected abstract void addStreamsToFetch();

  protected abstract long getRunIntervalInmsec();

  protected abstract void fetch() throws Exception;

  @Override
  public void run() {
    while (!stopped && !thread.isInterrupted()) {
      long startTime = System.currentTimeMillis();
      try {
        fetch();
      } catch (Exception e) {
        LOG.warn(e);
      }
      long finishTime = System.currentTimeMillis();
      long elapsedTime = finishTime - startTime;
      if (elapsedTime < getRunIntervalInmsec()) {
        try {
          long sleep = getRunIntervalInmsec() - elapsedTime;
          LOG.info("Sleeping for " + sleep);
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          LOG.warn("thread interrupted " + thread.getName(), e);
          return;
        }
      }
    }
  }

  public Set<Stream> getStreamsToFetch() {
    return streamsToFetch;
  }

  public synchronized void start() {
    thread = new Thread(this, this.name);
    LOG.info("Starting thread " + thread.getName());
    thread.start();
  }

  public synchronized void stop() {
    stopped = true;
    thread.interrupt();
  }

  public synchronized void join() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("thread interrupted " + thread.getName());
    }
  }
} 
