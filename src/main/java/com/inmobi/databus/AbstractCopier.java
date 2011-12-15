package com.inmobi.databus;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.DatabusConfig.Stream;

public abstract class AbstractCopier implements Runnable {

  private static final Log LOG = LogFactory.getLog(AbstractCopier.class);

  private final Cluster srcCluster;
  private final DatabusConfig config;
  protected final Set<Stream> streamsToFetch = new HashSet<Stream>();
  private Thread thread;

  public AbstractCopier(DatabusConfig config, Cluster srcCluster) {
    this.config = config;
    this.srcCluster = srcCluster;
    addStreamsToFetch();
  }

  public Cluster getSrcCluster() {
    return srcCluster;
  }

  public DatabusConfig getConfig() {
    return config;
  }

  protected abstract void addStreamsToFetch();

  public Set<Stream> getStreamsToFetch() {
    return streamsToFetch;
  }

  public void start() {
    thread = new Thread(this, srcCluster.name);
    thread.start();
  }

  public void join() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("thread interrupted " + thread.getName());
    }
  }
}
