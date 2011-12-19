package com.inmobi.databus.distcp;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Cluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class RemoteCopier extends AbstractCopier {

	private static final Log LOG = LogFactory.getLog(RemoteCopier.class);

	private FileSystem srcFs;
	private FileSystem destFs;

	public RemoteCopier(DatabusConfig config, Cluster srcCluster) {
		super(config, srcCluster);
	}

	protected void addStreamsToFetch() {
		Cluster destCluster = getConfig().getDestinationCluster();
		for (DatabusConfig.ConsumeStream s : destCluster.consumeStreams.values()) {
			if (getConfig().getStreams().get(s.name).getSourceClusters().contains(getSrcCluster())) {
				streamsToFetch.add(getConfig().getStreams().get(s.name));
			}
		}
	}

	@Override
	public void run() {
		try {

			srcFs = FileSystem.get(new URI(getSrcCluster().hdfsUrl),
							getConfig().getHadoopConf());
			destFs = FileSystem.get(
							new URI(getConfig().getDestinationCluster().hdfsUrl),
							getConfig().getHadoopConf());

			Path inputFilePath = getInputFilePath();
			if(inputFilePath == null) {
				LOG.warn("No data to pull from [" + inputFilePath.toString() + "]" +
								"Cluster [" + getSrcCluster().hdfsUrl + "]" +
								" to Cluster [" + getConfig().getDestinationCluster().hdfsUrl + "]");
				return;
			}

			Path tmpOut = new Path(getConfig().getTmpPath(), "distcp");
			LOG.warn("Starting a distcp pull from [" + inputFilePath.toString() + "] " +
							"Cluster [" + getSrcCluster().hdfsUrl + "]" +
							" to Cluster [" + getConfig().getDestinationCluster().hdfsUrl + "] " +
			        " Path [" + tmpOut.toString() + "]");
			//destFs.mkdirs(tmpOut);
		 /*
			String[] args = {input.makeQualified(destFs).toString(),
							tmpOut.toString()};
			DistCp.main(args);

			//TODO: if success
			commit();*/
		} catch (Exception e) {
			LOG.warn(e);
		}


	}

	private Path getInputFilePath() throws IOException {
		Path input = getInputPath();
		FileStatus[] fileList = srcFs.listStatus(input);
		if(fileList.length > 1) {
			//inputPath has have multiple files due to backlog
			//read all and create a tmp file

		 return null;
		}
		else if(fileList.length == 1) {
			return fileList[0].getPath();
		}
		else {
			return null;
		}
	}

	private void commit() throws IOException {

	}

	private Path getInputPath() throws IOException {
		Path input = new Path(srcFs.getUri().toString(), DatabusConfig.CONSUMER +
						File.separator +  getConfig().getDestinationCluster().getName());
		return input;
	}
}
