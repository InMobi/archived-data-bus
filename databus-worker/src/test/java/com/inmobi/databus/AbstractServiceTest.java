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
package com.inmobi.databus;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.inmobi.databus.distcp.DistcpBaseService;
import com.inmobi.databus.local.LocalStreamService;
import org.apache.hadoop.tools.DistCp;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

@Test
public class AbstractServiceTest {

  private static Logger LOG = Logger.getLogger(ClusterTest.class);

  public void getMSecondsTillNextRun() {

    Date date = new Date(12819279812l);
    SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd:HH:mm:s");
    LOG.info("Test Date [" + format.format(date) +"]");
    LocalStreamService service = new LocalStreamService(null, ClusterTest.buildLocalCluster());

    long mSecondsTillNextMin = service.getMSecondsTillNextRun(date
    .getTime());
    LOG.info("mSecondsTillNextMin = [" + mSecondsTillNextMin + "]");
    Date roundedDate = new Date(date.getTime() + mSecondsTillNextMin);
    LOG.info("Rounded Date to next MIN [" + format.format(roundedDate) + "]");
    assert mSecondsTillNextMin == 21000l;
  }


}
