package com.inmobi.databus.consume;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConsumerJob implements Tool {

    private Configuration conf;
    static final String STAGING = "/databus/system/staging";
    static final String DISTCP_INPUT = "/databus/system/distcp";
    static final String PUBLISH_DIR = "/databus/data/";

    private static final Log LOG = LogFactory.getLog(ConsumerJob.class);

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] arg) throws Exception {
        Path stagingP = new Path(STAGING);
        FileSystem fs = FileSystem.get(getConf());
        boolean stagingDirExists = FileSystem.get(getConf()).exists(stagingP);
        if (stagingDirExists) {
            System.out.println("Other wf in progress..exiting!");
            return 0;
        }

        Path inputPath = new Path(stagingP, "input");

        String consumeDir = arg[0];
        Map<FileStatus, String> fileListing = new HashMap<FileStatus, String>();
        createListing(fs, fs.getFileStatus(new Path(consumeDir)),
                fileListing, new HashSet<String>());

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

        List<Path> donePaths = getPathsToCreateDone(fileListing.values());
        //System.exit(1);
        Job job = createJob(inputPath);
        job.waitForCompletion(true);
        if (job.isSuccessful()) {
            postProcess(fileListing.values(), donePaths, job.getJobID());
        }
        return 0;
    }

    private void postProcess(Collection<String> destDirs, List<Path> donePaths,
                             JobID jobId)
            throws IOException {
        FileSystem fs = FileSystem.get(getConf());

        //create done files and done file input for distcp job
        Path distcpDoneInputFile = new Path(DISTCP_INPUT, jobId.toString() +
                "_done");
        FSDataOutputStream outDone = fs.create(distcpDoneInputFile);
        for (Path doneFile : donePaths) {
            LOG.info("Creating done file " + doneFile);
            fs.create(doneFile).close();
            outDone.writeBytes(doneFile.toString());
            outDone.writeBytes("\n");
        }
        outDone.close();

        //create the input for distcp jobs

        Path distcpInputFile = new Path(DISTCP_INPUT, jobId.toString());
        FSDataOutputStream out = fs.create(distcpInputFile);
        for (String path : destDirs) {
            out.writeBytes(path);
            out.writeBytes("\n");
        }
        out.close();



        //TODO:delete the staging dir

    }

    private List<Path> getPathsToCreateDone(Collection<String> destDirs)
            throws IOException {
        List<Path> result = new ArrayList<Path>();
        FileSystem fs = FileSystem.get(getConf());
        for (String dir : destDirs) {
            Path p = new Path(dir);
            LOG.info("Going to create a done file in directory [" + p.makeQualified(fs).getName());
            result.add(new Path(p, ".done").makeQualified(fs));
            doneFiles(fs, p, result, 1);
        }
        return result;
    }

    private void doneFiles(FileSystem fs, Path path, List<Path> result,
                           int level) throws IOException {
        Path p = path;
        while (level < 5) {
            p = p.makeQualified(fs);
            if (fs.exists(p)) {
                return;
            }
            LOG.info("Checking all level of parents for path [" + p.getName() + " to create DONE file");
            LOG.info("listing " + p.getParent());
            FileStatus[] listing = fs.listStatus(p.getParent());
            if (listing != null && listing.length > 0 && level < 3) {
                List<FileStatus> list = Arrays.asList(listing);
                Collections.sort(list, new ModificationTimeComparator());
                FileStatus lastPath = list.get(list.size() - 1);
                Path donePath = new Path(lastPath.getPath(), ".done");
                donePath = donePath.makeQualified(fs);
                LOG.info("adding path to create DONE file [" + donePath + "]");
                result.add(donePath);
            } else {
                //doneFiles(fs, p.getParent(), result, level++);
                p = p.getParent();
                level++;
            }
        }
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
            }
            else if("scribe_stats".equalsIgnoreCase(fileName.trim())) {
                excludes.add(fileName);
            }
            else if (!excludes.contains(fileName)){
                String destDir = getDestDir(fs, fileStatus.getPath().makeQualified(fs));
                results.put(fileStatus, destDir);
            }
        }
    }

    private String getDestDir(FileSystem fs, Path src)
            throws IOException {
        String category = src.getParent().getParent().getName();
        FileStatus status = fs.getFileStatus(src);
        long time = status.getModificationTime();
        Date date = new Date(time);
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        String month = Integer.toString(calendar.get(Calendar.MONTH)+1);
        String dest = PUBLISH_DIR + File.separator +
                category + File.separator +
                calendar.get(Calendar.YEAR) + File.separator +
                month + File.separator +
                calendar.get(Calendar.DAY_OF_MONTH) + File.separator +
                calendar.get(Calendar.HOUR_OF_DAY) + File.separator +
                calendar.get(Calendar.MINUTE);
        return dest;
    }

    protected Job createJob(Path inputPath) throws IOException {
        String jobName = "consumer";
        Job job = new Job(getConf());
        job.setJobName(jobName);
        KeyValueTextInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(CopyMapper.class);
        job.setMapperClass(CopyMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");

        return job;
    }

    class ModificationTimeComparator implements Comparator<FileStatus> {

        @Override
        public int compare(FileStatus o1, FileStatus o2) {
            return (int) (o1.getModificationTime() - o2.getModificationTime());
        }

    }

    public static void main(String[] args) throws Exception {
        ConsumerJob job = new ConsumerJob();
        ToolRunner.run(job, args);
    }
}
