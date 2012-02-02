package com.inmobi.databus;
/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractService implements Service, Runnable {

  private static final Log LOG = LogFactory.getLog(AbstractService.class);
  private static final long DEFAULT_RUN_INTERVAL = 60000;

  private final String name;
  private final DatabusConfig config;
  private final long runIntervalInMsec;
  protected Thread thread;
  protected volatile boolean stopped = false;

  public AbstractService(String name, DatabusConfig config) {
    this(name, config, DEFAULT_RUN_INTERVAL);
  }

  public AbstractService(String name, DatabusConfig config,
                         long runIntervalInMsec) {
    this.config = config;
    this.name = name;
    this.runIntervalInMsec = runIntervalInMsec;
  }

  public DatabusConfig getConfig() {
    return config;
  }

  public String getName() {
    return name;
  }

  protected abstract void execute() throws Exception;

  @Override
  public void run() {
    while (!stopped && !thread.isInterrupted()) {
      long startTime = System.currentTimeMillis();
      try {
        LOG.info("Starting the run");
        execute();
        if (stopped || thread.isInterrupted())
          return;
      } catch (Exception e) {
        LOG.warn("Error in run", e);
       }
      long finishTime = System.currentTimeMillis();
      long elapsedTime = finishTime - startTime;
      if (elapsedTime < runIntervalInMsec) {
        try {
          long sleep = runIntervalInMsec - elapsedTime;
          LOG.info("Sleeping for " + sleep);
          Thread.sleep(sleep);

        } catch (InterruptedException e) {
          LOG.warn("thread interrupted " + thread.getName(), e);
          return;
        }
      }
    }
  }

  @Override
  public synchronized void start() {
    thread = new Thread(this, this.name);
    LOG.info("Starting thread " + thread.getName());
    thread.start();
  }

  @Override
  public void stop() {
    stopped = true;
    LOG.info(Thread.currentThread().getName() + " stopped [" + stopped + "]");
  }

  @Override
  public synchronized void join() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("thread interrupted " + thread.getName());
    }
  }
} 
