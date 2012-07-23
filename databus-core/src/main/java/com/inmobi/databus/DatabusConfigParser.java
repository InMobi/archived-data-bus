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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

public class DatabusConfigParser implements DatabusConfigParserTags {

  private static Logger logger = Logger.getLogger(DatabusConfigParser.class);
  private Map<String, SourceStream> streamMap = new HashMap<String, SourceStream>();
  private Map<String, Cluster> clusterMap = new HashMap<String, Cluster>();
  private Map<String, List<DestinationStream>> clusterConsumeStreams = new HashMap<String, List<DestinationStream>>();

  private Map<String, String> defaults = new HashMap<String, String>();
  private int defaultRetentionInHours = 48;
  private int defaultTrashRetentionInHours = 24;

  public DatabusConfigParser(String fileName) throws Exception {
    parseXmlFile(fileName);
  }

  public DatabusConfig getConfig() {
    DatabusConfig config = new DatabusConfig(streamMap, clusterMap, defaults);
    return config;
  }

  private void parseXmlFile(String fileName) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document dom;
    if (fileName == null) {
      fileName = "databus.xml";
    }
    File file = new File(fileName);
    // see if file exists in cwd or is an absolute path
    if (file.exists()) {
      dom = db.parse(file);
    } else { // load file from classpath
      dom = db.parse(ClassLoader.getSystemResourceAsStream(fileName));
    }
    if (dom != null)
      parseDocument(dom);
    else
      throw new Exception("databus.xml file not found");
  }

  private void parseDocument(Document dom) throws Exception {
    Element docEle = dom.getDocumentElement();
    // read configs
    readDefaultPaths(docEle);
    // read the streams now
    readAllStreams(docEle);
    // read all clusterinfo
    readAllClusters(docEle);

  }

  private void readDefaultPaths(Element docEle) throws Exception {
    NodeList configList = docEle.getElementsByTagName(DEFAULTS);
    if (configList != null && configList.getLength() > 0) {
      String rootDir = getTextValue((Element) configList.item(0), ROOTDIR);
      if (rootDir == null)
        throw new ParseException("rootdir element not found in defaults", 0);
      
      defaults.put(ROOTDIR, rootDir);
      String retention = getTextValue((Element) configList.item(0),
          RETENTION_IN_HOURS);
      if (retention != null) {
        defaultRetentionInHours = Integer.parseInt(retention);
      }
      defaults.put(RETENTION_IN_HOURS, String.valueOf(defaultRetentionInHours));

      String trashretention = getTextValue((Element) configList.item(0),
          TRASH_RETENTION_IN_HOURS);
      if (trashretention != null) {
        defaultTrashRetentionInHours = Integer.parseInt(trashretention);
      }
      defaults.put(TRASH_RETENTION_IN_HOURS,
          String.valueOf(defaultTrashRetentionInHours));

      logger.debug("rootDir = " + rootDir + " global retentionInHours "
          + defaultRetentionInHours + " global trashretentionInHours "
          + defaultTrashRetentionInHours);
    }
  }

  private void readAllClusters(Element docEle) throws Exception {
    NodeList tmpClusterList = docEle.getElementsByTagName(CLUSTER);
    if (tmpClusterList != null && tmpClusterList.getLength() > 0) {
      for (int i = 0; i < tmpClusterList.getLength(); i++) {
        Element el = (Element) tmpClusterList.item(i);
        Cluster Cluster = getCluster(el);
        clusterMap.put(Cluster.getName(), Cluster);
      }
    }

  }

  private Cluster getCluster(Element el) throws Exception {
    NamedNodeMap elementsmap = el.getAttributes();
    Map<String, String> clusterelementsmap = new HashMap<String, String>(
        elementsmap.getLength());
    for (int i = 0; i < elementsmap.getLength(); ++i) {
      Attr attribute = (Attr) elementsmap.item(i);
      logger.info(attribute.getName() + ":" + attribute.getValue());
      clusterelementsmap.put(attribute.getName(), attribute.getValue());
    }

    String cRootDir = defaults.get(ROOTDIR);
    NodeList list = el.getElementsByTagName(ROOTDIR);
    if (list != null && list.getLength() == 1) {
      Element elem = (Element) list.item(0);
      cRootDir = elem.getTextContent();
    }

    Map<String, DestinationStream> consumeStreams = new HashMap<String, DestinationStream>();
    logger.debug("getting consume streams for Cluster ::"
        + clusterelementsmap.get(NAME));
    List<DestinationStream> consumeStreamList = getConsumeStreams(clusterelementsmap
        .get(NAME));
    if (consumeStreamList != null && consumeStreamList.size() > 0) {
      for (DestinationStream consumeStream : consumeStreamList) {
        consumeStreams.put(consumeStream.getName(), consumeStream);
      }
    }

    if (cRootDir == null)
      cRootDir = defaults.get(ROOTDIR);

    return new Cluster(clusterelementsmap, cRootDir, consumeStreams,
        getSourceStreams(clusterelementsmap.get(NAME)));
  }

  private Set<String> getSourceStreams(String clusterName) throws Exception {
    Set<String> srcStreams = new HashSet<String>();
    Set<Map.Entry<String, SourceStream>> entrySet = streamMap.entrySet();
    Iterator it = entrySet.iterator();
    while (it.hasNext()) {
      Map.Entry entry = (Map.Entry) it.next();
      String streamName = (String) entry.getKey();
      SourceStream streamDetails = (SourceStream) entry.getValue();
      if (streamDetails.getSourceClusters().contains(clusterName)) {
        srcStreams.add(streamName);
      }

    }
    return srcStreams;
  }

  private void readAllStreams(Element docEle) throws Exception {
    NodeList tmpstreamList = docEle.getElementsByTagName(STREAM);
    if (tmpstreamList != null && tmpstreamList.getLength() > 0) {
      for (int i = 0; i < tmpstreamList.getLength(); i++) {
        // for each stream
        Element el = (Element) tmpstreamList.item(i);
        SourceStream stream = getStream(el);
        streamMap.put(stream.getName(), stream);
      }
    }

  }

  private SourceStream getStream(Element el) throws Exception {
    Map<String, Integer> sourceStreams = new HashMap<String, Integer>();
    // get sources for each stream
    String streamName = el.getAttribute(NAME);
    NodeList sourceList = el.getElementsByTagName(SOURCE);
    for (int i = 0; i < sourceList.getLength(); i++) {
      Element source = (Element) sourceList.item(i);
      // for each source
      String clusterName = getTextValue(source, NAME);
      int rententionInHours = getRetention(source, RETENTION_IN_HOURS);
      logger.debug(" StreamSource :: streamname " + streamName
          + " retentioninhours " + rententionInHours + " " + "clusterName "
          + clusterName);
      sourceStreams.put(clusterName, new Integer(rententionInHours));
    }
    // get all destinations for this stream
    readConsumeStreams(streamName, el);
    return new SourceStream(streamName, sourceStreams);
  }

  private void readConsumeStreams(String streamName, Element el)
      throws Exception {
    NodeList consumeStreamNodeList = el.getElementsByTagName(DESTINATION);
    for (int i = 0; i < consumeStreamNodeList.getLength(); i++) {
      Element replicatedConsumeStream = (Element) consumeStreamNodeList.item(i);
      // for each source
      String clusterName = getTextValue(replicatedConsumeStream, NAME);
      int retentionInHours = getRetention(replicatedConsumeStream,
          RETENTION_IN_HOURS);
      String isPrimaryVal = getTextValue(replicatedConsumeStream, PRIMARY);
      Boolean isPrimary;
      if (isPrimaryVal != null && isPrimaryVal.equalsIgnoreCase("true"))
        isPrimary = new Boolean(true);
      else
        isPrimary = new Boolean(false);
      logger.info("Reading Stream Destination Details :: Stream Name "
          + streamName + " cluster " + clusterName + " retentionInHours "
          + retentionInHours + " isPrimary " + isPrimary);
      DestinationStream consumeStream = new DestinationStream(streamName,
          retentionInHours, isPrimary);
      if (clusterConsumeStreams.get(clusterName) == null) {
        List<DestinationStream> consumeStreamList = new ArrayList<DestinationStream>();
        consumeStreamList.add(consumeStream);
        clusterConsumeStreams.put(clusterName, consumeStreamList);
      } else {
        List<DestinationStream> consumeStreamList = clusterConsumeStreams
            .get(clusterName);
        consumeStreamList.add(consumeStream);
        clusterConsumeStreams.put(clusterName, consumeStreamList);
      }
    }
  }

  private List<DestinationStream> getConsumeStreams(String clusterName) {
    return clusterConsumeStreams.get(clusterName);
  }

  private String getTextValue(Element ele, String tagName) {
    String textVal = null;
    NodeList nl = ele.getElementsByTagName(tagName);
    if (nl != null && nl.getLength() > 0) {
      Element el = (Element) nl.item(0);
      textVal = el.getFirstChild().getNodeValue();
    }
    return textVal;
  }

  /**
   * Calls getTextValue and returns a int value
   */
  private int getRetention(Element ele, String tagName) {
    String ob = getTextValue(ele, tagName);
    if (ob == null) {
      return defaultRetentionInHours;
    }
    return Integer.parseInt(ob);
  }

  public static void main(String[] args) {
    try {
      DatabusConfigParser databusConfigParser;
      if (args.length >= 1)
        databusConfigParser = new DatabusConfigParser(args[0]);
      else
        databusConfigParser = new DatabusConfigParser(null);

      DatabusConfig config = databusConfigParser.getConfig();

      Map<String, Cluster> clustermap = config.getClusters();

      for (Map.Entry<String, Cluster> clusterentry : clustermap.entrySet()) {
        Cluster cluster = clusterentry.getValue();
        logger.debug("Cluster: " + clusterentry.getKey());
        logger.debug("Cluster Name: " + cluster.getName());
        logger.debug("HDFS URL: " + cluster.getHdfsUrl());
        logger.debug("JT Url: "
            + cluster.getHadoopConf().get("mapred.job.tracker"));
        logger.debug("Job Queue Name: " + cluster.getJobQueueName());
        logger.debug("Root Directory: " + cluster.getRootDir());
      }

      // databusConfigParser.parseXmlFile();
    } catch (Exception e) {
      e.printStackTrace();
      logger.debug(e);
      logger.debug(e.getMessage());
    }
  }

}
