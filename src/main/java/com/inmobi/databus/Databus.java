package com.inmobi.databus;

import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.consume.DataConsumer;
import com.inmobi.databus.distcp.RemoteCopier;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.util.*;

public class Databus {
    static Logger logger = Logger.getLogger(Databus.class);
    private DatabusConfig config;
    private String myClusterName;

    public Databus(String myClusterName) throws Exception{
        this.config = new DatabusConfig();
        this.myClusterName = myClusterName;
        DatabusConfigParser configParser = new DatabusConfigParser();
        config.setRootDir(configParser.getRootDir());
        config.setStreams(configParser.getStreamMap());
        config.setClusters(createClusterMap(configParser));
    }

    private Map<String, Cluster> createClusterMap(DatabusConfigParser configParser) {
        Map<String, Cluster>  clusterMap = new HashMap<String, Cluster>();
        Map<String, DatabusConfigParser.ClusterDetails> clusterDetailsMap = configParser.getClusterMap();
        Map<String, DatabusConfig.Stream> streamMap = configParser.getStreamMap();
        Set<Map.Entry<String,DatabusConfigParser.ClusterDetails>> entrySet = clusterDetailsMap.entrySet();

        for(Map.Entry<String, DatabusConfigParser.ClusterDetails> entry: entrySet) {
            //for each cluster
            String clusterName = entry.getKey();
            DatabusConfigParser.ClusterDetails clusterDetails = entry.getValue();

            List<DatabusConfigParser.ConsumeStream> consumeStreams = clusterDetails.getConsumeStreams();
            Set<DatabusConfig.ReplicatedStream> replicatedStreams = new HashSet();
            for (DatabusConfigParser.ConsumeStream consumeStream : consumeStreams) {
                DatabusConfig.Stream stream = streamMap.get(consumeStream.getStreamName());

                DatabusConfig.ReplicatedStream replicatedStream =
                        new DatabusConfig.ReplicatedStream(consumeStream.getStreamName(),
                                stream.getSourceClusters(), consumeStream.getRetentionHours());
                replicatedStreams.add(replicatedStream);
            }
            Cluster cluster = new Cluster(clusterDetails.getName(), clusterDetails.getHdfsURL(), replicatedStreams);

        }
        return clusterMap;

    }

    public void start() throws Exception {
        List<AbstractCopier> copiers = new ArrayList<AbstractCopier>();
        for (Cluster c : config.getClusters().values()) {
            AbstractCopier copier = null;
            if (myClusterName.equalsIgnoreCase(config.getDestinationCluster().getName())) {
                logger.warn("Starting data consumer for Cluster[" + myClusterName + "]");
                copier = new DataConsumer(config);
            } else {
                logger.warn("Starting remote copier for cluser [" + config.getDestinationCluster().getName() + "]");
                copier = new RemoteCopier(config, c);
            }
            copiers.add(copier);
            copier.start();
        }

        for (AbstractCopier copier : copiers) {
            copier.join();
        }

        //cleanup
        FileSystem fs = FileSystem.get(config.getHadoopConf());
        fs.delete(config.getTmpPath());
    }

    public static void main(String[] args) throws Exception {
        String myClusterName = null;
        if (args != null && args.length >=1)
            myClusterName = args[0];
        else {
            logger.warn("Specify this cluster name.");
            return;
        }
        Databus databus = new Databus(myClusterName);
        databus.start();
    }
}
