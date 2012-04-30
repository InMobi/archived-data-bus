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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultCheckpointProviderFactory implements
CheckpointProviderFactory{
  private static final String DEFAULT_PROVIDER = "com.inmobi.databus" +
  ".FSCheckpointProvider";
  private CheckpointProvider checkPointProvider;
  private static final String CHECKPOINT_PROVIDER = "checkpointprovider";
  private static final String DATABUS_CONFIG = "databus.cfg";

  private static final Log LOG = LogFactory.getLog
  (DefaultCheckpointProviderFactory.class);

  /*
   * loads databus.cfg from classpath, provider to o
   */
  @Override
  public CheckpointProvider create() throws Exception{
    InputStream in = getClass().getClassLoader().getResourceAsStream
    (DATABUS_CONFIG);
    if (in == null)
      checkPointProvider = getDefault();
    else
      checkPointProvider = getProvider(in);
    return checkPointProvider;
  }

  @Override
  public CheckpointProvider create(String config) throws Exception {
     InputStream in = new FileInputStream(config);
     if (in != null)
       checkPointProvider = getProvider(in);
     else
       checkPointProvider = getDefault();
    return checkPointProvider;
  }


  private CheckpointProvider getProvider(InputStream in) throws Exception{
    CheckpointProvider provider = null;
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    try {
      String line;
      do {
        line = reader.readLine();
        String[] keyVal = line.split("=");
        if (keyVal != null && keyVal.length == 2 && keyVal[0].equalsIgnoreCase
        (CHECKPOINT_PROVIDER)) {
          provider = (CheckpointProvider) Class.forName(keyVal[1])
          .newInstance();
         break;
        }
      } while (line != null);
    } catch (Exception e) {
      //return default provider
      return getDefault();
    }
    return provider;
  }


  private CheckpointProvider getDefault() throws Exception{
    CheckpointProvider checkpointProvider = null;
    try {
      //switch to default provider
      checkPointProvider = (CheckpointProvider) Class.forName(DEFAULT_PROVIDER)
      .newInstance();
    } catch (Exception e) {
      LOG.debug("Unable to create default checkpoint provider", e);
      throw new Exception(e);
    }
    return checkpointProvider;
  }
}
