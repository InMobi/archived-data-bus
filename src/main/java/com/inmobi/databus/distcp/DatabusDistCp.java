package com.inmobi.databus.distcp;

import com.inmobi.databus.consume.ConsumerJob;
import com.inmobi.databus.datamovement.CalendarHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.tools.DistCp;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.*;

public class DatabusDistCp {
    private Configuration conf = new Configuration();
    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    private static final Logger LOG =
            Logger.getLogger(DatabusDistCp.class);


    private void doDistCp(String sourceNameNode, String categoryList, String destination, String fileListDirectory) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.default.name", sourceNameNode);
        FileSystem fs = FileSystem.get(getConf());
        Set<String> categories = getCategories(categoryList);
        Path dirPath = new Path(fileListDirectory);
        FileStatus[] fileStatuses = fs.listStatus(dirPath);
        Path datafileToDistcp = makeFileToTransfer(categories, destination, fileStatuses, fs, dirPath, "_done", true);
        //String dataFileArgs = " -preserveSrcPath -f " + datafileToDistcp.makeQualified(fs).toString() + " " + destination;

        //distcp the data file
        String[] args = {"-preserveSrcPath", "-f", datafileToDistcp.makeQualified(fs).toString(), destination};
        DistCp.main(args);

        //Path donefileToDistcp = makeFileToTransfer(categories, destination, fileStatuses, fs, dirPath, "_done", false);
        //distcp the done file
        //String doneFileArgs = "-f " + donefileToDistcp.makeQualified(fs).toString();
        //args = new String[]{"-preserveSrcPath", doneFileArgs, destination};
        //DistCp.main(args);

        // delete the fileListDirectory
        fs.delete(dirPath, true);



    }

    private Path makeFileToTransfer(Set<String> categories, String destination,
                                    FileStatus[] fileStatuses , FileSystem fs,
                                    Path dirPath, String filterFileName, boolean isExclude) throws Exception{
        //make all files list except the DONE file list
        List<String> allFileNames = new ArrayList<String>();
        List<FileStatus> dataFiles = new ArrayList<FileStatus>();
        for (int i=0; i < fileStatuses.length; i++) {
            FileStatus fileStatus = fileStatuses[i];
            String name = fileStatus.getPath().makeQualified(fs).getName();
            if (name.contains("~"))
                continue; //skip this file
            if (isExclude) {
                if (!name.contains(filterFileName)) {
                    LOG.info("Adding [" + fileStatus.toString() + "] to check");
                    dataFiles.add(fileStatus);
                }
            }
            else {
                if (name.contains(filterFileName)) {
                    LOG.info("Adding [" + fileStatus.toString() + "] to check");
                    dataFiles.add(fileStatus);
                }
            }
        }
        // read all files and create one final file
        for (FileStatus fileStatus : dataFiles) {
            LOG.info("Reading file =" + fileStatus.toString());
            FSDataInputStream fsDataInputStream = fs.open(fileStatus.getPath().makeQualified(fs));
            while (fsDataInputStream.available() > 0 ){
                String fileName = fsDataInputStream.readLine();
                if (fileName != null) {
                    fileName = fileName.trim();
                    Iterator<String> it = categories.iterator();
                    while(it.hasNext()) {
                        String category = it.next();
                        if (fileName.contains(category)) {
                            LOG.info("Adding fileName [" + fileName + "] to final List");
                            allFileNames.add(fileName);
                            break;
                        }
                    }
                }
            }
            fsDataInputStream.close();
        }
        LOG.info("Creating final List file in [" + dirPath.toString() + "]");
        Path  finalFilePath = null;
        if (!isExclude)
             finalFilePath = new Path(dirPath, (CalendarHelper.getCurrentDayTimeAsString() + "_files_to_move_done"));
        else
             finalFilePath = new Path(dirPath, (CalendarHelper.getCurrentDayTimeAsString() + "_files_to_move"));

        FSDataOutputStream out = fs.create(finalFilePath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
        for(String fileName : allFileNames) {
            LOG.info("Writing [" + fileName + "] to final list");
            writer.write(fileName);
            writer.write("\n");
        }
        out.sync();
        writer.close();


        return finalFilePath;

    }

    private Set<String> getCategories(String categoryList) {
        Set<String> categories = new HashSet<String>();
        StringTokenizer tokenizer = new StringTokenizer(categoryList, ",");
        while(tokenizer.hasMoreTokens()) {
            categories.add(tokenizer.nextToken().trim());
        }
        return categories;
    }

    public static void main(String[] args) {
        if (args == null || args.length < 3) {
            LOG.info("Incorrect usage");
            return;
        }
        //comma seperate category list eg: c1, c2, .., cn
        String catgeoryList = args[0].trim();
        LOG.warn("catgeoryList = " + catgeoryList);
        //fully qualified hdfs dest path
        String destination = args[1].trim();
        LOG.warn("destination = " + destination);
        //directory to read the files from
        String fileListDirectory = args[2].trim();
        LOG.warn("fileListDirectory = "+ fileListDirectory);
        //String nameNode of source from where distcp is to be done
        String sourceNameNode = args[3].trim();
        LOG.warn("sourceNameNode =" + sourceNameNode);
        DatabusDistCp databusDistCp = new DatabusDistCp();
        try {
            databusDistCp.doDistCp(sourceNameNode, catgeoryList, destination, fileListDirectory);
        }
        catch (Exception e) {
            LOG.warn(e);
            e.printStackTrace();
        }

    }
}
