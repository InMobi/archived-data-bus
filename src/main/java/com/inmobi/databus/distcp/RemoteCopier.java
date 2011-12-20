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


	public RemoteCopier(DatabusConfig config, Cluster srcCluster, Cluster destinationCluster) {
		super(config, srcCluster, destinationCluster);
	}

	protected void addStreamsToFetch() {

	}



	@Override
	public void fetch() throws Exception{
		try {
			boolean skipCommit = false;

			srcFs = FileSystem.get(new URI(getSrcCluster().getHdfsUrl()),
							getSrcCluster().getHadoopConf());
			destFs = FileSystem.get(
							new URI(getDestCluster().getHdfsUrl()),
							getDestCluster().getHadoopConf());

			Path inputFilePath = getInputFilePath();

			if(inputFilePath == null) {
				LOG.warn("No data to pull from [" + inputFilePath.toString() + "]" +
								"Cluster [" + getSrcCluster().getHdfsUrl() + "]" +
								" to Cluster [" + getDestCluster().getHdfsUrl() + "]");
				return;
			}

			Path tmpOut = new Path(getDestCluster().getTmpPath(), "distcp-" +
							getSrcCluster().getName() + CalendarHelper.getCurrentDayTimeAsString()).makeQualified(destFs);
			LOG.warn("Starting a distcp pull from [" + inputFilePath.toString() + "] " +
							"Cluster [" + getSrcCluster().getHdfsUrl() + "]" +
							" to Cluster [" + getDestCluster().getHdfsUrl() + "] " +
							" Path [" + tmpOut.toString() + "]");
			destFs.mkdirs(tmpOut);

			String[] args = {"-f", inputFilePath.toString(),
							tmpOut.toString()};
			try {
				DistCp.main(args);
			}
			catch(Exception e) {
				LOG.warn(e.getMessage());
				LOG.warn(e);
				LOG.warn("Problem in distcp skipping commit");
				destFs.delete(tmpOut, true);
				skipCommit = true;
			}
			//if success
			if (!skipCommit) {
				synchronized (RemoteCopier.class) {
					commit(inputFilePath, tmpOut);
				}
			}
		} catch (Exception e) {
			LOG.warn(e);
		}
	}

	private Path getInputFilePath() throws IOException {
		Path input = getInputPath();
		FileStatus[] fileList = srcFs.listStatus(input);
		if (fileList != null) {
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
					LOG.debug("Adding sourceFile [" + sourceFile + "] to distcp FinalList");
					writer.write(sourceFile);
					writer.write("\n");
				}
				writer.close();
				LOG.debug("Source File For distCP [" + tmpPath + "]");
				return tmpPath.makeQualified(srcFs);
			}
			else if(fileList.length == 1) {
				return fileList[0].getPath().makeQualified(srcFs);
			}
			else {
				return null;
			}
		}
		return  null;
	}

	private void commit(Path inputFilePath, Path tmpOut
	) throws Exception {
		Map<String, Path> categoryToCommit = new HashMap<String, Path>();
		//move tmpout intermediate dir per category to achive atomic move to publish dir
		String minute = CalendarHelper.getCurrentMinute();
		FileStatus[] allFiles = destFs.listStatus(tmpOut);
		for(int i=0; i < allFiles.length; i++) {
			String fileName = allFiles[i].getPath().getName();
			String category = getCategoryFromFileName(fileName);

			Path intermediatePath = new Path(tmpOut, category + File.separator + minute );
			destFs.mkdirs(intermediatePath);
			Path source = allFiles[i].getPath().makeQualified(destFs);
			destFs.rename(source, intermediatePath);
			LOG.debug("Moving [" + source + "] to intermediatePath [" + intermediatePath + "]");
			if(categoryToCommit.get(category) == null)  {
				categoryToCommit.put(category, intermediatePath);
			}

		}
		Set<Map.Entry<String, Path> > commitEntries = categoryToCommit.entrySet();
		Iterator it = commitEntries.iterator();
		while(it.hasNext()) {
			Map.Entry<String, Path> entry = (Map.Entry<String, Path>) it.next();
			String category  =  entry.getKey();
			Path categorySrcPath = entry.getValue();
			long commitTime = System.currentTimeMillis();
			Path destParentPath = new Path(getDestCluster().getFinalDestDirTillHour(category, commitTime));
			if(!destFs.exists(destParentPath))  {
				destFs.mkdirs(destParentPath);
			}
			destFs.rename(categorySrcPath, destParentPath);
			LOG.debug("Moving from intermediatePath [" + categorySrcPath + "] to [" + destParentPath + "]");
		}
		//rmr inputFilePath.getParent() this is from srcFs
		//commit distcp
		srcFs.delete(inputFilePath.getParent(), true);
		LOG.debug("Deleting [" + inputFilePath.getParent() + "]");
		//rmr tmpOut   cleanup
		destFs.delete(tmpOut, true);
		LOG.debug("Deleting [" + tmpOut + "]");
		Thread.sleep(60000);

	}



	private String getCategoryFromFileName(String fileName) {
		StringTokenizer tokenizer = new StringTokenizer(fileName, "-");
		tokenizer.nextToken(); //skip collector name
		String catgeory = tokenizer.nextToken();
		return catgeory;

	}

	private Path getInputPath() throws IOException {
		return getSrcCluster().getConsumePath(getDestCluster());

	}
}
