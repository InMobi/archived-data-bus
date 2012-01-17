package com.inmobi.databus;

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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class DatabusConfigParser {

  private static Logger logger = Logger.getLogger(DatabusConfigParser.class);
  private Document dom;
  private Map<String, SourceStream> streamMap = new HashMap<String, SourceStream>();
  private Map<String, Cluster> clusterMap = new HashMap<String, Cluster>();
  private Map<String, List<DestinationStream>> clusterConsumeStreams =
      new HashMap<String, List<DestinationStream>>();

  private String inputDir;
  private String publishDir;
  private String defaultZkConnectString;
  private String rootDir;
  private int defaultRetentionInDays = 2;

  public DatabusConfigParser(String fileName) throws Exception {
    parseXmlFile(fileName);
  }

  public DatabusConfig getConfig() {
    DatabusConfig config = new DatabusConfig(defaultZkConnectString, streamMap,
        clusterMap);
    return config;
  }

  private void parseXmlFile(String fileName) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    if (fileName == null)
      dom = db.parse(ClassLoader.getSystemResourceAsStream("databus.xml"));
    else
      dom = db.parse(fileName);
    if (dom != null)
      parseDocument();
    else
      throw new Exception("databus.xml file not found");
  }

  private void parseDocument() {
    Element docEle = dom.getDocumentElement();
    // read configs
    readDefaultPaths(docEle);
    // read the streams now
    readAllStreams(docEle);
    // read all clusterinfo
    readAllClusters(docEle);

  }

  private void readDefaultPaths(Element docEle) {
    NodeList configList = docEle.getElementsByTagName("defaults");
    if (configList != null && configList.getLength() > 0) {
      rootDir = getTextValue((Element) configList.item(0), "rootdir");
      inputDir = getTextValue((Element) configList.item(0), "inputdir");
      publishDir = getTextValue((Element) configList.item(0), "publishdir");
      defaultZkConnectString = getTextValue((Element) configList.item(0),
          "zookeeperconnectstring");
      String retention = getTextValue((Element) configList.item(0),
          "retentionindays");
      if (retention != null) {
        defaultRetentionInDays = Integer.parseInt(retention);
      }

      logger.debug("rootDir = " + rootDir + " inputDir " + inputDir
          + " publishDir " + publishDir + " zkConnectString "
          + defaultZkConnectString + " global retentionInDays "
          + defaultRetentionInDays);
    }
  }

  private void readAllClusters(Element docEle) {
    NodeList tmpClusterList = docEle.getElementsByTagName("cluster");
    if (tmpClusterList != null && tmpClusterList.getLength() > 0) {
      for (int i = 0; i < tmpClusterList.getLength(); i++) {
        Element el = (Element) tmpClusterList.item(i);
        Cluster Cluster = getCluster(el);
        clusterMap.put(Cluster.getName(), Cluster);
      }
    }

  }

  private Cluster getCluster(Element el) {
    String clusterName = el.getAttribute("name");
    String hdfsURL = el.getAttribute("hdfsurl");
    String jtURL = el.getAttribute("jturl");
    logger.info("clusterName " + clusterName + " hdfsURL " + hdfsURL + " jtUrl"
        + jtURL);
    String cRootDir = rootDir;
    NodeList list = el.getElementsByTagName("rootdir");
    if (list != null && list.getLength() == 1) {
      Element elem = (Element) list.item(0);
      cRootDir = elem.getTextContent();
    }
    String zkConnectString = getZKConnectStringForCluster(el
        .getElementsByTagName("zookeeper"));
    logger.info("zkConnectString [" + zkConnectString + "]");
    Map<String, DestinationStream> consumeStreams = new HashMap<String, DestinationStream>();
    logger.debug("getting consume streams for CLuster ::" + clusterName);
    List<DestinationStream> consumeStreamList = getConsumeStreams(clusterName);
    if (consumeStreamList != null && consumeStreamList.size() > 0) {
      for (DestinationStream consumeStream : consumeStreamList) {
        consumeStreams.put(consumeStream.getName(), consumeStream);
      }
    }
    if (cRootDir == null)
      cRootDir = rootDir;

    return new Cluster(clusterName, cRootDir, hdfsURL, jtURL, consumeStreams,
        getSourceStreams(clusterName), zkConnectString);
  }

  private String getZKConnectStringForCluster(NodeList zkConnectionStringList) {
    String zkConnectString = null;
    if (zkConnectionStringList != null
        && zkConnectionStringList.getLength() == 1) {
      Element elem = (Element) zkConnectionStringList.item(0);
      zkConnectString = getTextValue(elem, "connectionstring");
      logger.debug("getZKConnectStringForCluster [" + zkConnectString + "]");
    }
    return zkConnectString;
  }

  private Set<String> getSourceStreams(String clusterName) {
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

  private void readAllStreams(Element docEle) {
    NodeList tmpstreamList = docEle.getElementsByTagName("stream");
    if (tmpstreamList != null && tmpstreamList.getLength() > 0) {
      for (int i = 0; i < tmpstreamList.getLength(); i++) {
        // for each stream
        Element el = (Element) tmpstreamList.item(i);
        SourceStream stream = getStream(el);
        streamMap.put(stream.getName(), stream);
      }
    }

  }

  private SourceStream getStream(Element el) {
    Map<String, Integer> sourceStreams = new HashMap<String, Integer>();
    // get sources for each stream
    String streamName = el.getAttribute("name");
    NodeList sourceList = el.getElementsByTagName("source");
    for (int i = 0; i < sourceList.getLength(); i++) {
      Element source = (Element) sourceList.item(i);
      // for each source
      String clusterName = getTextValue(source, "name");
      int rententionInDays = getRetention(source, "retentionindays");
      logger.debug(" StreamSource :: streamname " + streamName
          + " retentionindays " + rententionInDays + " " + "clusterName "
          + clusterName);
      sourceStreams.put(clusterName, new Integer(rententionInDays));
    }
    // get all destinations for this stream
    readConsumeStreams(streamName, el);
    return new SourceStream(streamName, sourceStreams);
  }

  private void readConsumeStreams(String streamName, Element el) {
    NodeList consumeStreamNodeList = el.getElementsByTagName("destination");
    for (int i = 0; i < consumeStreamNodeList.getLength(); i++) {
      Element replicatedConsumeStream = (Element) consumeStreamNodeList.item(i);
      // for each source
      String clusterName = getTextValue(replicatedConsumeStream, "name");
      int retentionInDays = getRetention(replicatedConsumeStream,
          "retentionindays");
      String isPrimaryVal = getTextValue(replicatedConsumeStream, "primary");
      Boolean isPrimary;
      if (isPrimaryVal != null && isPrimaryVal.equalsIgnoreCase("true"))
        isPrimary = new Boolean(true);
      else
        isPrimary = new Boolean(false);
      logger.info("Reading Stream Destination Details :: Stream Name "
          + streamName + " cluster " + clusterName + " retentionInDays "
          + retentionInDays + " isPrimary " + isPrimary);
      DestinationStream consumeStream = new DestinationStream(streamName,
          retentionInDays, isPrimary);
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
      return defaultRetentionInDays;
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

      // databusConfigParser.parseXmlFile();
    } catch (Exception e) {
      e.printStackTrace();
      logger.debug(e);
      logger.debug(e.getMessage());
    }
  }

}
