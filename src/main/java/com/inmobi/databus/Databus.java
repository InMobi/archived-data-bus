package com.inmobi.databus;

import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.consume.DataConsumer;
import com.inmobi.databus.distcp.RemoteCopier;
import org.apache.log4j.Logger;

import java.util.*;

public class Databus {
	static Logger logger = Logger.getLogger(Databus.class);
	private DatabusConfig config;
	private String myClusterName;

	public Databus(String myClusterName, String databusconfigFile)
					throws Exception {
		DatabusConfigParser configParser;
		this.myClusterName = myClusterName;
		if (databusconfigFile == null)
			configParser = new DatabusConfigParser(null);
		else
			configParser = new DatabusConfigParser(databusconfigFile);
		Map<String, Cluster> clusterMap = configParser.getClusterMap();
		this.config = new DatabusConfig(configParser.getRootDir(),
						configParser.getStreamMap(), clusterMap, clusterMap.get(myClusterName));
		logger.debug("my cluster details " + clusterMap.get(myClusterName));
	}

	public void start() throws Exception {
		List<AbstractCopier> copiers = new ArrayList<AbstractCopier>();
		logger.warn("My clusterName is [" + myClusterName + "] "	+ config.getDestinationCluster().getName());
		logger.warn("Total clusters " + config.getClusters().size());
		//Run dataconsumer if this cluster is source of any stream
		if (isSourceOfAnyStream(myClusterName)){
			logger.warn("Starting data consumer for Cluster[" + myClusterName + "]" +
							" as it's generating some streams.");
			AbstractCopier copier = new DataConsumer(config);
			copiers.add(copier);
			copier.start();
		}
		//Start remote copier for each cluster from where this cluster wants to consume a stream
		//Remote Copier Algo - Go in sourceCluster and check if anything is available for destination
		//cluster. If YES distcp pull to destination into tmp and mv to finaldest.
		Set<String> clustersForStreamsToBeConsumed  = getClustersForStreamsConsumed(myClusterName);
		Iterator clustersToStartRemoteCopier = clustersForStreamsToBeConsumed.iterator();
		while (clustersToStartRemoteCopier.hasNext()) {
			String clusterName = (String) clustersToStartRemoteCopier.next();
			Cluster cluster  = config.getClusters().get(clusterName);
			if(!cluster.getName().equalsIgnoreCase(myClusterName)) {
				logger.warn("Starting remote copier for cluster [" + cluster.getName() + "]");
				AbstractCopier copier = new RemoteCopier(config, cluster);
				copiers.add(copier);
				copier.start();
			}
		}
		for ( AbstractCopier copier : copiers) {
			copier.join();
		}
	}


	private Set<String> getClustersForStreamsConsumed(String myClusterName) {
		Set<String> clustersForStreamsToBeConsumed = new HashSet<String>();
		Cluster myCluster = config.getClusters().get(myClusterName);
		Map<String,DatabusConfig.ConsumeStream> consumeStreamMap = myCluster.getConsumeStreams();
		Set<Map.Entry<String, DatabusConfig.ConsumeStream>> consumeStreams = consumeStreamMap.entrySet();
		Iterator it = consumeStreams.iterator();
		while (it.hasNext()) {
			Map.Entry<String, DatabusConfig.ConsumeStream> streamEntry = (Map.Entry<String, DatabusConfig.ConsumeStream>)
							it.next();
			String streamName = streamEntry.getKey();
			DatabusConfig.ConsumeStream consumeStream = streamEntry.getValue();
			clustersForStreamsToBeConsumed.addAll(getClustersNameForStream(streamName));
		}
		return clustersForStreamsToBeConsumed;
	}

	private Set<String> getClustersNameForStream(String streamName) {
		if(streamName == null)
			return  null;
		return 	config.getStreams().get(streamName).getSourceClusters();
	}


	private boolean isSourceOfAnyStream(String clusterName) {
		Map<String, DatabusConfig.Stream> streams = config.getStreams();
		Set<Map.Entry<String, DatabusConfig.Stream>> entrySet  = streams.entrySet();
		Iterator it = entrySet.iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			String streamName = (String) entry.getKey();
			DatabusConfig.Stream streamDetails = (DatabusConfig.Stream) entry.getValue();
			if(streamDetails.getSourceClusters().contains(clusterName))
				return true;
		}
		return false;
	}


	public static void main(String[] args) throws Exception {
		String myClusterName = null;
		Databus databus;
		if (args != null && args.length >= 1)
			myClusterName = args[0].trim();
		else {
			logger.warn("Specify this cluster name.");
			return;
		}
		if (args.length <= 1)
			databus = new Databus(myClusterName, null);
		else {
			String databusconfigFile = args[1].trim();
			databus = new Databus(myClusterName, databusconfigFile);
		}
		databus.start();
	}
}
