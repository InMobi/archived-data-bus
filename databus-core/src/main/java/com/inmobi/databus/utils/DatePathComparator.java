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
package com.inmobi.databus.utils;

import java.util.Comparator;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class DatePathComparator implements Comparator<FileStatus> {

  static Logger LOG = Logger.getLogger(DatePathComparator.class);

  @Override
  public int compare(FileStatus fileStatus, FileStatus fileStatus1) {

    /*
    * Path eg:
    * /databus/system/distcp_mirror_srcCluster_destCluster/databus/streams
    * /<stream-Name>/2012/1/13/15/7/<hostname>-<streamName>-2012-01-16-07
    * -21_00000.gz
    *
    * in some cases paths can empty without files
    * eg:   /databus/system/distcp_mirror_srcCluster_destCluster/databus
    * /streams/<streamName>/2012/1/13/15/7/
    */

    Path streamDir = null;
    Path streamDir1 = null;
    Date streamDate1 = null;
    Date streamDate = null;

    if (fileStatus != null) {
      if (!fileStatus.isDir())
        streamDir = fileStatus.getPath().getParent();
      else
        streamDir = fileStatus.getPath();
      Path streamDirPrefix = streamDir.getParent().getParent().getParent()
          .getParent().getParent();

      streamDate = CalendarHelper.getDateFromStreamDir(streamDirPrefix,
          streamDir);
      LOG.debug("streamDate [" + streamDate + "]");
    }

    if (fileStatus1 != null) {
      if (!fileStatus1.isDir())
        streamDir1 = fileStatus1.getPath().getParent();
      else
        streamDir1 = fileStatus1.getPath();

      Path streamDirPrefix1 = streamDir1.getParent().getParent().getParent()
          .getParent().getParent();

      streamDate1 = CalendarHelper.getDateFromStreamDir(streamDirPrefix1,
          streamDir1);
      LOG.debug("streamDate1 [" + streamDate1.toString() + "]");
    }

    if (streamDate != null && streamDate1 != null)
      return streamDate.compareTo(streamDate1);
    else
      return -1;

  }
}