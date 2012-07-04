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

import java.io.File;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

@Test
public class ClusterTest {
  private static Logger LOG = Logger.getLogger(ClusterTest.class);

  public void getLocalDestDir() {
    try {
      Cluster cluster = buildCluster();
      LOG.debug("getLocalDestDir ::");
      String expectedPath = "hdfs://localhost:8020/databus/streams_local"
          + "/testCategory/1970/02/07/18/18/";
      Calendar calendar = new GregorianCalendar(1970, 1, 7, 18, 18); // month
                                                                     // starts
                                                                     // from 0
                                                                     // :P
      String path = cluster.getLocalDestDir("testCategory",
 calendar.getTime());
      LOG.debug("Expected Path [" + expectedPath + "]");
      LOG.debug("Path [" + path + "]");
      assert expectedPath.equals(path);
    } catch (Exception e) {
      e.printStackTrace();
      assert false;
    }
  }

  public void getDateTimeDestDir() {
    LOG.debug("getDateTimeDestDir ::");
    try {
      Cluster cluster = buildCluster();
      String expectedPath = "testCategory/1970/09/05/16/43/";
      Calendar calendar = new GregorianCalendar(1970, 8, 5, 16, 43);
      String path = cluster.getDateTimeDestDir("testCategory",
          calendar.getTimeInMillis());
      LOG.debug("Path [" + path + "]");
      LOG.debug("Expected Path [" + expectedPath + "]");
      assert expectedPath.equals(path);
    } catch (Exception e) {
      e.printStackTrace();
      assert false;
    }
  }

  public void getFinalDestDir() {
    LOG.debug("getFinalDestDir ::");
    try {
      Cluster cluster = buildCluster();
      String path = null;
      String expectedPath = "hdfs://localhost:8020/databus/streams/testCategory/1970/09/05/16/43/";
      Calendar calendar = new GregorianCalendar(1970, 8, 5, 16, 43);
      path = cluster
          .getFinalDestDir("testCategory", calendar.getTimeInMillis());
      LOG.debug("Path [" + path + "]");
      LOG.debug("Expected Path [" + expectedPath + "]");
      assert expectedPath.equals(path);

    } catch (Exception e) {
      e.printStackTrace();
      assert false;
    }
  }

  public void getFinalDestDirTillHour() {
    LOG.debug("getFinalDestDirTillHour ::");
    try {
      Cluster cluster = buildCluster();
      String path = null;
      String expectedPath = "hdfs://localhost:8020/databus/streams/testCategory/1970/09/05/16/";
      Calendar calendar = new GregorianCalendar(1970, 8, 5, 16, 0);
      path = cluster.getFinalDestDirTillHour("testCategory",
          calendar.getTimeInMillis());
      LOG.debug("Path [" + path + "]");
      LOG.debug("Expected Path [" + expectedPath + "]");
      assert expectedPath.equals(path);
    } catch (Exception e) {
      e.printStackTrace();
      assert false;
    }
  }

  public static Cluster buildCluster() throws Exception {
    Map<String, String> clusterElementsMap = new HashMap<String, String>();
    clusterElementsMap.put("name", "testCluster");
    clusterElementsMap.put("hdfsurl", "hdfs://localhost:8020");
    clusterElementsMap.put("jturl", "http://localhost:8021");
    clusterElementsMap.put("jobqueuename", "default");
    return new Cluster(clusterElementsMap, "databus", null, null);
  }

  public static Cluster buildLocalCluster() throws Exception {
    return buildLocalCluster(null, null, null);
  }

  public static Cluster buildLocalCluster(String rootdir, String clusterName,
      String hdfsUrl, String jtUrl, Set<String> sourcestreams,
      Map<String, DestinationStream> consumestreams) throws Exception {
    if (jtUrl == null)
      jtUrl = "http://localhost:8021";

    if (hdfsUrl == null)
      hdfsUrl = "file:///tmp/" + new Random().nextLong() + File.separator;

    if (clusterName == null)
      clusterName = "localCluster";

    Map<String, String> clusterElementsMap = new HashMap<String, String>();
    clusterElementsMap.put("name", clusterName);
    clusterElementsMap.put("hdfsurl", hdfsUrl);
    clusterElementsMap.put("jturl", jtUrl);
    clusterElementsMap.put("jobqueuename", "default");
    return new Cluster(clusterElementsMap, rootdir,
        ((consumestreams == null) ? (new HashMap<String, DestinationStream>())
            : consumestreams),
        ((sourcestreams == null) ? (new HashSet<String>())
            : sourcestreams));
  }

  public static Cluster buildLocalCluster(String clusterName, String hdfsUrl,
      String jtUrl) throws Exception {
    return buildLocalCluster("databus", clusterName, hdfsUrl, jtUrl, null, null);
  }
}
