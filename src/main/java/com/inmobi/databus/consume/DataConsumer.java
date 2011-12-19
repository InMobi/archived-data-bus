package com.inmobi.databus.consume;

import com.inmobi.databus.AbstractCopier;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfig.Cluster;
import com.inmobi.databus.DatabusConfig.Stream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class DataConsumer extends AbstractCopier {

	private static final Log LOG = LogFactory.getLog(DataConsumer.class);
	private Path tmpPath;
	private Path tmpJobInputPath;
	private Path tmpJobOutputPath;

	public DataConsumer(DatabusConfig config) {
		super(config, config.getDestinationCluster());
		this.tmpPath = new Path(config.getDestinationCluster().hdfsUrl +
						File.separator + config.getTmpPath(), "dataconsumer-" +
						config.getDestinationCluster().name + "_" + System.currentTimeMillis());
		this.tmpJobInputPath = new Path(tmpPath, "jobIn");
		this.tmpJobOutputPath = new Path(tmpPath, "jobOut");
	}

	protected void addStreamsToFetch() {
		for (Stream s : getConfig().getStreams().values()) {
			if (s.sourceClusters.contains(getSrcCluster())) {
				streamsToFetch.add(s);
			}
		}
	}

	@Override
	public void run() {
		Map<FileStatus, String> fileListing = new HashMap<FileStatus, String>();
		try {
			createMRInput(tmpJobInputPath, fileListing);
			if (fileListing.size() == 0) {
				LOG.info("Nothing to do!");
				return;
			}
			Job job = createJob(tmpJobInputPath);
			job.waitForCompletion(true);
			if (job.isSuccessful()) {
				long commitTime = System.currentTimeMillis();
				Map<Path, Path> commitPaths = prepareForCommit(commitTime,
								fileListing);
				commit(commitPaths);
				LOG.info("Committed successfully for " + commitTime);
			}
		} catch(Exception e) {
			LOG.warn("Failed to fetch from local", e);
		}

	}

	private Map<Path, Path> prepareForCommit(long commitTime,
																					 Map<FileStatus, String> fileListing) throws IOException {
		FileSystem fs = FileSystem.get(getConfig().getHadoopConf());


		//find final destination paths
		Map<Path, Path> mvPaths = new LinkedHashMap<Path, Path>();
		FileStatus[] categories = fs.listStatus(tmpJobOutputPath);
		for (FileStatus categoryDir : categories) {
			Path destDir = new Path(getConfig().getFinalDestDir(
							categoryDir.getPath().getName(), commitTime));
			FileStatus[] files = fs.listStatus(categoryDir.getPath());
			for (FileStatus file : files) {
				Path destPath = new Path(destDir, file.getPath().getName());
				mvPaths.put(file.getPath(), destPath);
			}
		}

		//find input files for consumers
		Map<Path, Path> consumerCommitPaths = new HashMap<Path, Path>();
		for (Cluster cluster : getConfig().getClusters().values()) {
			if (cluster.equals(getConfig().getDestinationCluster())) {
				continue;
			}
			Path tmpConsumerPath = new Path(tmpPath, cluster.name);
			FSDataOutputStream out = fs.create(tmpConsumerPath);
			for (Path destPath : mvPaths.values()) {
				String category = getCategoryFromDestPath(destPath);
				if(cluster.consumeStreams.containsKey(category)) {
					out.writeBytes(destPath.toString());
					out.writeBytes("\n");
				}
			}
			out.close();
			Path finalConsumerPath = new Path(getConfig().
							getConsumePath(getConfig().getDestinationCluster(), cluster),
							Long.toString(commitTime));
			consumerCommitPaths.put(tmpConsumerPath, finalConsumerPath);

		}

		//find trash paths
		Map<Path, Path> trashPaths = new LinkedHashMap<Path, Path>();
		Path trash = getConfig().getTrashPath();
		for (FileStatus src : fileListing.keySet()) {
			String category = getCategoryFromSrcPath(src.getPath());
			Path target = new Path(trash, category + "_" + src.getPath().getName());
			trashPaths.put(src.getPath(), target);
		}

		Map<Path, Path> commitPaths = new LinkedHashMap<Path, Path>();
		if (mvPaths.size() == trashPaths.size()) {//validate the no of files
			commitPaths.putAll(mvPaths);
			commitPaths.putAll(consumerCommitPaths);
			//TODO:
			//commitPaths.putAll(trashPaths);
		}
		return commitPaths;
	}

	private void commit(Map<Path, Path> commitPaths) throws IOException {
		LOG.info("Committing " + commitPaths.size() + " paths.");
		FileSystem fs = FileSystem.get(getConfig().getHadoopConf());
		for (Map.Entry<Path, Path> entry : commitPaths.entrySet()) {
			LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
			fs.mkdirs(entry.getValue().getParent());
			fs.rename(entry.getKey(), entry.getValue());
		}
	}

	private void createMRInput(Path inputPath, Map<FileStatus, String> fileListing)
					throws IOException {
		FileSystem fs = FileSystem.get(getConfig().getHadoopConf());

		createListing(fs, fs.getFileStatus(getConfig().getDataDir()), fileListing,
						new HashSet<String>());

		FSDataOutputStream out = fs.create(inputPath);
		Iterator<Entry<FileStatus, String>> it = fileListing.entrySet().iterator();
		while (it.hasNext()) {
			Entry<FileStatus, String> entry = it.next();
			out.writeBytes(entry.getKey().getPath().toString());
			out.writeBytes("\t");
			out.writeBytes(entry.getValue());
			out.writeBytes("\n");
		}
		out.close();
	}

	private void createListing(FileSystem fs, FileStatus fileStatus,
														 Map<FileStatus, String> results, Set<String> excludes) throws IOException {

		if (fileStatus.isDir()) {
			for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
				createListing(fs, stat, results, excludes);
			}
		} else {
			String fileName = fileStatus.getPath().getName();
			if (fileName.endsWith("current")) {
				FSDataInputStream in = fs.open(fileStatus.getPath());
				String currentFileName = in.readLine();
				in.close();
				excludes.add(currentFileName);
			} else if ("scribe_stats".equalsIgnoreCase(fileName.trim())) {
				excludes.add(fileName);
			} else if (!excludes.contains(fileName)) {
				Path src = fileStatus.getPath().makeQualified(fs);
				String category = getCategoryFromSrcPath(src);
				String destDir = getCategoryJobOutTmpPath(category).toString();
				//String destDir = getConfig().getDestDir(category);
				results.put(fileStatus, destDir);
			}
		}
	}

	private String getCategoryFromSrcPath(Path src) {
		return src.getParent().getParent().getName();
	}

	private String getCategoryFromDestPath(Path dest) {
		return dest.getParent().getParent().getParent().
						getParent().getParent().getParent().getName();
	}

	private Path getCategoryJobOutTmpPath(String category) {
		return new Path(tmpJobOutputPath, category);
	}

	private Job createJob(Path inputPath) throws IOException {
		String jobName = "consumer";
		Configuration conf = config.getHadoopConf();
		Job job = new Job(conf);
		job.setJobName(jobName);
		KeyValueTextInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setJarByClass(CopyMapper.class);
		job.setMapperClass(CopyMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputFormatClass(NullOutputFormat.class);
		job.getConfiguration().set("mapred.map.tasks.speculative.execution",
						"false");

		return job;
	}
}
