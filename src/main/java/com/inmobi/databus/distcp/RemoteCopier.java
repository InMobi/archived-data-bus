package com.inmobi.databus.distcp;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Cluster;

public class RemoteCopier extends AbstractCopier {

	private static final Log LOG = LogFactory.getLog(RemoteCopier.class);

	private FileSystem srcFs;
	private FileSystem destFs;

	public RemoteCopier(DatabusConfig config, 
	    Cluster srcCluster, Cluster destCluster) {
		super(config, srcCluster, destCluster);
	}

	protected void addStreamsToFetch() {
		Cluster destCluster = getDestCluster();
		for (DatabusConfig.ConsumeStream s : destCluster.getConsumeStreams().values()) {
			if (getConfig().getStreams().get(s.getName()).getSourceClusters().contains(getSrcCluster())) {
				streamsToFetch.add(getConfig().getStreams().get(s.getName()));
			}
		}
	}

  @Override
  public void fetch() throws Exception {

    srcFs = FileSystem.get(new URI(getSrcCluster().getHdfsUrl()), getSrcCluster()
        .getHadoopConf());
    destFs = FileSystem.get(new URI(getDestCluster().getHdfsUrl()), getDestCluster()
        .getHadoopConf());

    /*
    Path inputFilePath = getInputFilePath();
    if (inputFilePath == null) {
      LOG.warn("No data to pull from [" + inputFilePath.toString() + "]"
          + "Cluster [" + getSrcCluster().hdfsUrl + "]" + " to Cluster ["
          + getDestCluster().hdfsUrl + "]");
      return;
    }

    Path tmpOut = new Path(getConfig().getTmpPath(), "distcp");
    LOG.warn("Starting a distcp pull from [" + inputFilePath.toString() + "] "
        + "Cluster [" + getSrcCluster().hdfsUrl + "]" + " to Cluster ["
        + getDestCluster().hdfsUrl + "] " + " Path [" + tmpOut.toString() + "]");
        */
    // destFs.mkdirs(tmpOut);
    /*
     * String[] args = {input.makeQualified(destFs).toString(),
     * tmpOut.toString()}; DistCp.main(args);
     * 
     * //TODO: if success commit();
     */

  }

	/*private Path getInputFilePath() throws IOException {
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
/*
	private Path getInputPath() throws IOException {
		Path input = new Path(srcFs.getUri().toString(), DatabusConfig.CONSUMER +
						File.separator +  getDestCluster().getName());
		return input;
	}*/
}
