package com.inmobi.databus.distcp;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.datamovement.CalendarHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.tools.DistCp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.*;

public class RemoteCopier extends AbstractCopier {

	private static final Log LOG = LogFactory.getLog(RemoteCopier.class);

	private FileSystem srcFs;
	private FileSystem destFs;

	public RemoteCopier(DatabusConfig config, Cluster srcCluster) {
		super(config, srcCluster);
	}

	protected void addStreamsToFetch() {
		/*
		Cluster destCluster = getConfig().getDestinationCluster();
		for (DatabusConfig.ConsumeStream s : destCluster.consumeStreams.values()) {
			if (getConfig().getStreams().get(s.name).getSourceClusters().contains(getSrcCluster())) {
				streamsToFetch.add(getConfig().getStreams().get(s.name));
			}
		}
		*/
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

			Path tmpOut = new Path(getConfig().getTmpPath(), "distcp-" + getSrcCluster().getName()).makeQualified(destFs);
			LOG.warn("Starting a distcp pull from [" + inputFilePath.toString() + "] " +
							"Cluster [" + getSrcCluster().hdfsUrl + "]" +
							" to Cluster [" + getConfig().getDestinationCluster().hdfsUrl + "] " +
							" Path [" + tmpOut.toString() + "]");
			destFs.mkdirs(tmpOut);

			String[] args = {"-f", inputFilePath.toString(),
							tmpOut.toString()};

			DistCp.main(args);

			//if success
			commit(inputFilePath, tmpOut, DatabusConfig.PUBLISH_DIR);
		} catch (Exception e) {
			LOG.warn(e);
		}


	}

	private Path getInputFilePath() throws IOException {
		Path input = getInputPath();
		FileStatus[] fileList = srcFs.listStatus(input);
		if(fileList.length > 1) {
			Set<String> sourceFiles = new HashSet<String>();
			//inputPath has have multiple files due to backlog
			//read all and create a tmp file
			for(int i=0; i < fileList.length; i++) {
				FSDataInputStream fsDataInputStream = srcFs.open(fileList[i].getPath().makeQualified(srcFs));
				while (fsDataInputStream.available() > 0 ){
					String fileName = fsDataInputStream.readLine();
					if (fileName != null) {
						fileName = fileName.trim();
						sourceFiles.add(fileName);
					}
				}
				fsDataInputStream.close();
			}
			Path tmpPath = new Path(input, CalendarHelper.getCurrentDayTimeAsString());
			FSDataOutputStream out = srcFs.create(tmpPath);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
			for(String sourceFile: sourceFiles) {
				writer.write(sourceFile);
				writer.write("\n");
			}
			writer.close();
			return tmpPath.makeQualified(srcFs);
		}
		else if(fileList.length == 1) {
			return fileList[0].getPath().makeQualified(srcFs);
		}
		else {
			return null;
		}
	}

	private void commit(Path inputFilePath, Path tmpOut,
											String publishDir) throws IOException {
		Map<Path, Path> commitPaths = new HashMap<Path, Path>();
		//move tmpout to publishDir
		long commitTime = System.currentTimeMillis();
		FileStatus[] allFiles = destFs.listStatus(tmpOut);
		for(int i=0; i < allFiles.length; i++) {
			String fileName = allFiles[i].getPath().getName();
			String category = getCategoryFromFileName(fileName);
			Path destinationpath = new Path(config.getFinalDestDir(category, commitTime)).makeQualified(destFs);
			commitPaths.put(allFiles[i].getPath().makeQualified(destFs),
							new Path(destinationpath + File.separator + allFiles[i].getPath().getName()));

		}
		Set<Map.Entry<Path, Path> > commitEntries = commitPaths.entrySet();
		Iterator it = commitEntries.iterator();
		while(it.hasNext()) {
			Map.Entry<Path, Path> entry = (Map.Entry<Path, Path>) it.next();
			Path source =  entry.getKey();
			Path destination = entry.getValue();
			Path destParentPath = new Path(destination.getParent().makeQualified(destFs).toString());
			if(!destFs.exists(destParentPath))
					destFs.mkdirs(destParentPath);
			destFs.rename(source, destParentPath);
			LOG.debug("Moving [" + source.toString() + "] to [" + destParentPath.toString() + "]");
		}

		//rmr tmpOut
		destFs.delete(tmpOut, true);
		LOG.debug("Deleting [" + tmpOut + "]");
		//rmr inputFilePath.getParent() this is from srcFs
		srcFs.delete(inputFilePath.getParent(), true);
		LOG.debug("Deleteing [" + inputFilePath.getParent() + "]");


	}

	private String getCategoryFromFileName(String fileName) {
		StringTokenizer tokenizer = new StringTokenizer(fileName, "-");
		tokenizer.nextToken(); //skip collector name
		String catgeory = tokenizer.nextToken();
		return catgeory;

	}

	private Path getInputPath() throws IOException {
		Path input = new Path(srcFs.getUri().toString(), DatabusConfig.CONSUMER +
						File.separator +  getConfig().getDestinationCluster().getName());
		return input;
	}
}
