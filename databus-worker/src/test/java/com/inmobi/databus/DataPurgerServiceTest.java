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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import com.inmobi.databus.purge.DataPurgerService;
import org.testng.annotations.Test;
import org.apache.log4j.Logger;


@Test
public class DataPurgerServiceTest {
  private static Logger LOG = Logger.getLogger(DataPurgerServiceTest.class);
  DateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm");

  public void isPurgeTest1() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.DAY_OF_MONTH, -1);

    boolean status = service.isPurge(date, new Integer(1));
    LOG.info("isPurgeTest1 streamDate [" + dateFormat.format(date.getTime())+
    "] shouldPurge [" + status + "]" );
    assert  status == true;
  }


  public void isPurgeTest2() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.DAY_OF_MONTH, -3);
    boolean status = service.isPurge(date,new Integer(1));
    LOG.info("isPurgeTest2 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert  status == true;
  }


  public void isPurgeTest3() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    boolean status =  service.isPurge(date,new Integer(1));
    LOG.info("isPurgeTest3 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert status  == false;
  }

  public void isPurgeTest4() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.DAY_OF_MONTH, -2);
    boolean status =  service.isPurge(date,new Integer(3));
    LOG.info("isPurgeTest4 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert status  == false;
  }

  public void isPurgeTest5() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.DAY_OF_MONTH, -3);
    boolean status =  service.isPurge(date,new Integer(3));
    LOG.info("isPurgeTest5 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert status  == true;
  }




  private DataPurgerService buildPurgerService() {
    DataPurgerService service;
    try {
      service = new DataPurgerService(null, ClusterTest.buildLocalCluster());
    }
    catch (Exception e) {
      LOG.error("Error in creating DataPurgerService", e);
      return null;
    }
    return service;
  }
}
