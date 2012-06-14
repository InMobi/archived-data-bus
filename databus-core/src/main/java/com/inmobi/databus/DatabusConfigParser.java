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

  private Map<String, Stream> Streams = new HashMap<String, Stream>();
  private Map<String, Cluster> Clusters = new HashMap<String, Cluster>();
  private Map<String, String> Defaults = new HashMap<String, String>();

  private int defaultRetentionInHours = 48;
  private int defaultTrashRetentionInHours = 24;

  public DatabusConfigParser(String fileName) throws Exception {
    parseXmlFile(fileName);
  }

  public DatabusConfig getConfig() {
    DatabusConfig config = new DatabusConfig(Streams, Clusters, Defaults);
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
      parseDocument(dom.getDocumentElement());
    else
      throw new Exception("databus.xml file not found");
  }

  private void parseDocument(Element docEle) throws Exception {
    // read configs
    readDefaultPaths(docEle);
    // read all clusterinfo
    readAllClusters(docEle);
    // read the streams now
    readAllStreams(docEle);
  }

  private void readDefaultPaths(Element docEle) throws Exception {
    NodeList configList = docEle.getElementsByTagName(DEFAULTS);
    if (configList != null && configList.getLength() > 0) {
      String rootDir = getTextValue((Element) configList.item(0), ROOTDIR);
      if (rootDir == null)
        throw new ParseException("rootdir element not found in defaults", 0);
      
      Defaults.put(ROOTDIR, rootDir);
      String retention = getTextValue((Element) configList.item(0),
          RETENTION_IN_HOURS);
      if (retention != null) {
        defaultRetentionInHours = Integer.parseInt(retention);
      }
      Defaults.put(RETENTION_IN_HOURS, String.valueOf(defaultRetentionInHours));

      String trashretention = getTextValue((Element) configList.item(0),
          TRASH_RETENTION_IN_HOURS);
      if (trashretention != null) {
        defaultTrashRetentionInHours = Integer.parseInt(trashretention);
      }
      Defaults.put(TRASH_RETENTION_IN_HOURS, trashretention);

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
        Cluster cluster = getCluster(el);
        Clusters.put(cluster.getName(), cluster);
      }
    }

  }

  private Cluster getCluster(Element el) throws Exception {
    NamedNodeMap elementsmap = el.getAttributes();
    Map<String, String> clusterConfiguration = new HashMap<String, String>(
        elementsmap.getLength());
    for (int i = 0; i < elementsmap.getLength(); ++i) {
      Attr attribute = (Attr) elementsmap.item(i);
      logger.info(attribute.getName() + ":" + attribute.getValue());
      clusterConfiguration.put(attribute.getName(), attribute.getValue());
    }

    String cRootDir = Defaults.get(ROOTDIR);
    NodeList list = el.getElementsByTagName(ROOTDIR);
    if (list != null && list.getLength() == 1) {
      Element elem = (Element) list.item(0);
      cRootDir = elem.getTextContent();
    }

    if (cRootDir == null)
      cRootDir = Defaults.get(ROOTDIR);

    clusterConfiguration.put(ROOTDIR, cRootDir);

    return new Cluster(clusterConfiguration);
  }

  private void readAllStreams(Element docEle) throws Exception {
    NodeList tmpstreamList = docEle.getElementsByTagName(STREAM);
    if (tmpstreamList != null && tmpstreamList.getLength() > 0) {
      for (int i = 0; i < tmpstreamList.getLength(); i++) {
        // for each stream
        Element el = (Element) tmpstreamList.item(i);
        String streamName = el.getAttribute(NAME);
        Stream stream = getStream(streamName, el);
        Streams.put(streamName, stream);
      }
    }

  }

  private Stream getStream(String streamName, Element el) throws Exception {
    Stream stream = new Stream(streamName);
    // get sources for each stream
    readSourceStreams(stream, el);
    // get all destinations for this stream
    readDestinationStreams(stream, el);
    return stream;
  }

  private void readSourceStreams(Stream stream, Element el) throws Exception {
    NodeList sourceList = el.getElementsByTagName(SOURCE);
    for (int i = 0; i < sourceList.getLength(); i++) {
      Element source = (Element) sourceList.item(i);
      // for each source
      String clusterName = getTextValue(source, NAME);
      Cluster sourceCluster = Clusters.get(clusterName);
      if (sourceCluster == null)
        throw new ParseException(clusterName
            + " Not Found in Clusters Configration", 0);
      int retentionInHours = getRetention(source, RETENTION_IN_HOURS);
      logger.debug(" StreamSource :: streamname " + stream.getName()
          + " retentioninhours " + retentionInHours + " " + "clusterName "
          + clusterName);
      stream.addSourceCluster(retentionInHours, sourceCluster);
    }
  }

  private void readDestinationStreams(Stream stream, Element el)
      throws Exception {
    NodeList destinationStreamNodeList = el.getElementsByTagName(DESTINATION);
    for (int i = 0; i < destinationStreamNodeList.getLength(); i++) {
      Element replicatedDestinationStream = (Element) destinationStreamNodeList
          .item(i);
      // for each source
      String clusterName = getTextValue(replicatedDestinationStream, NAME);
      Cluster destinationCluster = Clusters.get(clusterName);
      if (destinationCluster == null)
        throw new ParseException(clusterName
            + " Not Found in Clusters Configration", 0);
      int retentionInHours = getRetention(replicatedDestinationStream,
          RETENTION_IN_HOURS);
      String isPrimaryVal = getTextValue(replicatedDestinationStream, PRIMARY);
      Boolean isPrimary;
      if (isPrimaryVal != null && isPrimaryVal.equalsIgnoreCase("true"))
        isPrimary = new Boolean(true);
      else
        isPrimary = new Boolean(false);
      logger.info("Reading Stream Destination Details :: Stream Name "
          + stream.getName() + " cluster " + clusterName + " retentionInHours "
          + retentionInHours + " isPrimary " + isPrimary);
      stream.addDestinationCluster(retentionInHours, destinationCluster,
          isPrimary);
    }
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
}
