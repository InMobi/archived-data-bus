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
package com.inmobi.databus;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.testng.*;
import org.testng.annotations.Test;

@Test
public class ClusterTest {
  private static Logger LOG = Logger.getLogger(ClusterTest.class);

  public void getLocalDestDir() {
    Cluster cluster = buildCluster();
    try {
      LOG.debug("getLocalDestDir ::");
      String expectedPath = "hdfs://localhost:8020/databus/streams_local" +
      "/testCategory/1970/02/07/18/18/";
      String path = cluster.getLocalDestDir("testCategory", 3242890100L);
      LOG.debug("Expected Path [" + expectedPath + "]");
      LOG.debug("Path [" + path + "]");
      assert expectedPath.equals(path);
    } catch (IOException e) {
      e.printStackTrace();
      assert false;
    }
  }

  public void getDateTimeDestDir() {
    LOG.debug("getDateTimeDestDir ::");
    Cluster cluster = buildCluster();
    String expectedPath = "testCategory/1970/09/05/16/43/";
    String path = cluster.getDateTimeDestDir("testCategory", 21381231232L);
    LOG.debug("Path [" + path + "]");
    LOG.debug("Expected Path [" + expectedPath + "]");
    assert expectedPath.equals(path);
  }

  public void getFinalDestDir() {
    LOG.debug("getFinalDestDir ::");
    Cluster cluster = buildCluster();
    String path = null;
    try {
      String expectedPath =
      "hdfs://localhost:8020/databus/streams/testCategory/1970/09/05/16/43/";
      path = cluster.getFinalDestDir("testCategory", 21381231232L);
      LOG.debug("Path [" + path + "]");
      LOG.debug("Expected Path [" + expectedPath + "]");
      assert expectedPath.equals(path);

    } catch (IOException e) {
      e.printStackTrace();
      assert false;
    }
  }

  public void getFinalDestDirTillHour() {
    LOG.debug("getFinalDestDirTillHour ::");
    Cluster cluster = buildCluster();
    String path = null;
    try {
      String expectedPath =
      "hdfs://localhost:8020/databus/streams/testCategory/1970/09/05/16/";
      path = cluster.getFinalDestDirTillHour("testCategory", 21381231232L);
      LOG.debug("Path [" + path + "]");
      LOG.debug("Expected Path [" + expectedPath + "]");
      assert expectedPath.equals(path);
    } catch (IOException e) {
      e.printStackTrace();
      assert false;
    }
  }

  public static Cluster buildCluster() {
    return new Cluster("testCluster", "databus", "hdfs://localhost:8020", "http://localhost:8025", null, null);
  }

  public static Cluster buildLocalCluster() {
    return new Cluster("localCluster", "databus", "file://tmp/",
    "http://localhost:1822", null, null);
  }
}
