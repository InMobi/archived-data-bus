package com.inmobi.databus;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 15/12/11
 * Time: 6:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class DatabusConfigParser {

    class Cluster {
        public Cluster(String name, String hdfsURL, List<ConsumeStream> consumeStreams) {
            this.name =name;
            this.hdfsURL = hdfsURL;
            this.consumeStreams = consumeStreams;

        }
        String name;

        public String getName() {
            return name;
        }

        public String getHdfsURL() {
            return hdfsURL;
        }


        public List<ConsumeStream> getConsumeStreams() {
            return consumeStreams;
        }

        String hdfsURL;
        List<ConsumeStream> consumeStreams;

    }

    class ConsumeStream {
        String streamName;

        public ConsumeStream(String streamNane, int retentionHours) {
            this.streamName = streamNane;
            this.retentionHours = retentionHours;
        }

        public String getStreamName() {
            return streamName;
        }

        public int getRetentionHours() {
            return retentionHours;
        }


        int retentionHours;

    }

    Document dom;
    Map<String, DatabusConfig.Stream> streamMap = new HashMap<String, DatabusConfig.Stream>();
    Map<String, Cluster> clusterMap = new HashMap<String,Cluster>();
    String rootDir;
    String inputDir;
    String publishDir;


    private void parseXmlFile() throws Exception{
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        String fileName = Thread.currentThread().getContextClassLoader().getResource("databus.xml").getFile();
        dom = db.parse("employees.xml");
        parseDocument();
    }

    private void parseDocument(){
        Element docEle = dom.getDocumentElement();
        rootDir = docEle.getElementsByTagName("RootDir").item(0).getNodeValue();
        inputDir = docEle.getElementsByTagName("inputDir").item(0).getNodeValue();
        publishDir = docEle.getElementsByTagName("publishDir").item(0).getNodeValue();
        //read the streams now
        readAllStreams(docEle);
        //read all clusterinfo
        readAllClusters(docEle);

    }

    private void readAllClusters(Element docEle) {
        NodeList tmpClusterList = docEle.getElementsByTagName("Cluster");
        if (tmpClusterList !=null && tmpClusterList.getLength() > 0 ) {
            for (int i=0; i < tmpClusterList.getLength(); i++) {
                Element el = (Element) tmpClusterList.item(i);
                Cluster cluster = getCLuster(el);
                clusterMap.put(cluster.getName(), cluster);
            }
        }

    }

    private Cluster getCLuster(Element el) {
        String clusterName = el.getAttribute("name");
        String hdfsURL = el.getAttribute("hdfsUrl");
        List<ConsumeStream> consumeStreams = new ArrayList<ConsumeStream>();

        NodeList consumeStreamList = el.getElementsByTagName("ConsumeStream");
        for (int i=0; i < consumeStreamList.getLength(); i++) {
            Element replicatedConsumeStream = (Element) consumeStreamList.item(i);
            // for each source
            String streamName =  getTextValue(replicatedConsumeStream, "name");
            int retentionHours = getIntValue(replicatedConsumeStream, "retentionHours");

            ConsumeStream consumeStream = new ConsumeStream(streamName, retentionHours);
            consumeStreams.add(consumeStream);
        }
        return new Cluster(clusterName, hdfsURL, consumeStreams);
    }



    private void readAllStreams(Element docEle) {
        NodeList tmpstreamList = docEle.getElementsByTagName("Stream");
        if(tmpstreamList != null && tmpstreamList.getLength() > 0) {
            for(int i = 0 ; i < tmpstreamList.getLength();i++) {
                // for each stream
                Element el = (Element) tmpstreamList.item(i);
                DatabusConfig.Stream stream = getStream(el);
                streamMap.put(stream.name, stream);
            }
        }

    }

    private DatabusConfig.Stream getStream(Element el) {
        Set<String> sourceClusters = new HashSet<String>();
        //get sources for each stream
        String streamName = el.getAttribute("streamname");
        NodeList sourceList = el.getElementsByTagName("Source");
        for (int i=0; i < sourceList.getLength(); i++) {
            Element source = (Element) sourceList.item(i);
            // for each source
            String clusterName =  getTextValue(source, "name");
            sourceClusters.add(clusterName);
        }
        return new DatabusConfig.Stream(streamName, sourceClusters);
    }


    private String getTextValue(Element ele, String tagName) {
        String textVal = null;
        NodeList nl = ele.getElementsByTagName(tagName);
        if(nl != null && nl.getLength() > 0) {
            Element el = (Element)nl.item(0);
            textVal = el.getFirstChild().getNodeValue();
        }
        return textVal;
    }

    /**
     * Calls getTextValue and returns a int value
     */
    private Integer getIntValue(Element ele, String tagName) {
        //in production application you would catch the exception
        return Integer.parseInt(getTextValue(ele, tagName));
    }

}
