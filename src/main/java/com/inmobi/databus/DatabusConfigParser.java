package com.inmobi.databus;

import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.DatabusConfig.ConsumeStream;
import com.inmobi.databus.DatabusConfig.Stream;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatabusConfigParser {
  
  static Logger logger = Logger.getLogger(DatabusConfigParser.class);
  Document dom;
  Map<String, Stream> streamMap = new HashMap<String, Stream>();
  Map<String, Cluster> clusterMap = new HashMap<String, Cluster>();
  String inputDir;
  String publishDir;
  String fileName;
  String rootDir;

  public Map<String, Stream> getStreamMap() {
    return streamMap;
  }

  public String getRootDir() {

    return rootDir;
  }

  public String getInputDir() {
    return inputDir;
  }

  public String getPublishDir() {
    return publishDir;
  }

  public Map<String, DatabusConfig.Cluster> getClusterMap() {
    return clusterMap;
  }

  public DatabusConfigParser(String fileName) throws Exception {
    this.fileName = fileName;
    parseXmlFile();
  }

  public void parseXmlFile() throws Exception {
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
    readConfigPaths(docEle);
    // read the streams now
    readAllStreams(docEle);
    // read all clusterinfo
    readAllClusters(docEle);

  }

  private void readConfigPaths(Element docEle) {
    NodeList configList = docEle.getElementsByTagName("Config");
    if (configList != null && configList.getLength() > 0) {
      rootDir = getTextValue((Element) configList.item(0), "RootDir");
      inputDir = getTextValue((Element) configList.item(0), "InputDir");
      publishDir = getTextValue((Element) configList.item(0), "PublishDir");
      logger.debug("rootDir = " + rootDir + " inputDir " + inputDir
          + " publishDir " + publishDir);
    }
  }

  private void readAllClusters(Element docEle) {
    NodeList tmpClusterList = docEle.getElementsByTagName("Cluster");
    if (tmpClusterList != null && tmpClusterList.getLength() > 0) {
      for (int i = 0; i < tmpClusterList.getLength(); i++) {
        Element el = (Element) tmpClusterList.item(i);
        Cluster Cluster = getCLuster(el);
        clusterMap.put(Cluster.getName(), Cluster);
      }
    }

  }

  private Cluster getCLuster(Element el) {
    String clusterName = el.getAttribute("name");
    String hdfsURL = el.getAttribute("hdfsUrl");
    logger.debug("clusterName " + clusterName + " hdfsURL " + hdfsURL);
    Map<String, ConsumeStream> consumeStreams 
        = new HashMap<String, ConsumeStream>();
    NodeList consumeStreamList = el.getElementsByTagName("ConsumeStream");
    for (int i = 0; i < consumeStreamList.getLength(); i++) {
      Element replicatedConsumeStream = (Element) consumeStreamList.item(i);
      // for each source
      String streamName = getTextValue(replicatedConsumeStream, "name");
      int retentionHours = getIntValue(replicatedConsumeStream,
          "retentionHours");
      logger.debug("Reading Cluster :: Stream Name " + streamName
          + " retentionHours " + retentionHours);
      ConsumeStream consumeStream = new ConsumeStream(streamName,
          retentionHours);
      consumeStreams.put(streamName, consumeStream);
    }
    return new Cluster(clusterName, hdfsURL, consumeStreams);
  }

  private void readAllStreams(Element docEle) {
    NodeList tmpstreamList = docEle.getElementsByTagName("Stream");
    if (tmpstreamList != null && tmpstreamList.getLength() > 0) {
      for (int i = 0; i < tmpstreamList.getLength(); i++) {
        // for each stream
        Element el = (Element) tmpstreamList.item(i);
        DatabusConfig.Stream stream = getStream(el);
        streamMap.put(stream.name, stream);
      }
    }

  }

  private DatabusConfig.Stream getStream(Element el) {
    Set<String> sourceClusters = new HashSet<String>();
    // get sources for each stream
    String streamName = el.getAttribute("streamname");
    NodeList sourceList = el.getElementsByTagName("Source");
    for (int i = 0; i < sourceList.getLength(); i++) {
      Element source = (Element) sourceList.item(i);
      // for each source
      String clusterName = getTextValue(source, "name");
      logger.debug(" streamname " + streamName + " clusterName " + clusterName);
      sourceClusters.add(clusterName);
    }
    return new DatabusConfig.Stream(streamName, sourceClusters);
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
  private Integer getIntValue(Element ele, String tagName) {
    // in production application you would catch the exception
    return Integer.parseInt(getTextValue(ele, tagName));
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
