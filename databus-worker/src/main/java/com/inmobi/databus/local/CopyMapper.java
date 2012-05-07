/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.inmobi.databus.local;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.inmobi.databus.utils.FileUtil;

import java.io.File;
import java.io.IOException;

public class CopyMapper extends Mapper<Text, Text, Text, Text> {

  private static final Log LOG = LogFactory.getLog(CopyMapper.class);

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
          InterruptedException {
    Path src = new Path(key.toString());
    String dest = value.toString();
    String collector = src.getParent().getName();
    String category = src.getParent().getParent().getName();

    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path target = getTempPath(context, src, category, collector);
    FileUtil.gzip(src, target, context.getConfiguration());
    // move to final destination
    fs.mkdirs(new Path(dest).makeQualified(fs));
    Path destPath = new Path(dest + File.separator + collector + "-"
            + src.getName() + ".gz");
    LOG.info("Renaming file " + target + " to " + destPath);
    fs.rename(target, destPath);

  }

  private Path getTempPath(Context context, Path src, String category,
                           String collector) {
    Path tempPath = new Path(getTaskAttemptTmpDir(context), category + "-"
            + collector + "-" + src.getName() + ".gz");
    return tempPath;
  }

  private Path getTaskAttemptTmpDir(Context context) {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    return new Path(getJobTmpDir(context, attemptId.getJobID()),
            attemptId.toString());
  }

  private Path getJobTmpDir(Context context, JobID jobId) {
    return new Path(
            new Path(context.getConfiguration().get("localstream.tmp.path")),
            jobId.toString());
  }
}
