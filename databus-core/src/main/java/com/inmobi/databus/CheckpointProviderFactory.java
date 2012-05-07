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
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CheckpointProviderFactory {
  private static final String DEFAULT_PROVIDER = "com.inmobi.databus" +
  ".FSCheckpointProvider";
  private static final String CHECKPOINT_PROVIDER_DIR = "checkpointprovider" +
  ".dir";
  private static final String CHECKPOINT_PROVIDER = "checkpointprovider";
  private static final String DATABUS_CONFIG = "databus.cfg";

  private static final Log LOG = LogFactory.getLog
  (CheckpointProviderFactory.class);


  public static CheckpointProvider create(String config) throws Exception {
    CheckpointProvider checkPointProvider = null;
    InputStream in = new FileInputStream(config);
    if (in != null)
      checkPointProvider = getProvider(in);
    return checkPointProvider;
  }


  private static CheckpointProvider getProvider(InputStream in) throws Exception{
    CheckpointProvider provider = null;
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    try {
      String line;
      String providerName = null;
      String providerDir = null;
      do {
        line = reader.readLine();
        String[] keyVal = line.split("=");
        if (keyVal != null && keyVal.length == 2 && keyVal[0].equalsIgnoreCase
        (CHECKPOINT_PROVIDER)) {
          //provider = (CheckpointProvider) Class.forName(keyVal[1])
          //.newInstance();
          providerName = keyVal[1];
        }
        if (keyVal != null && keyVal.length == 2 && keyVal[0].equalsIgnoreCase
        (CHECKPOINT_PROVIDER_DIR)) {
          providerDir = keyVal[1];
        }
      } while (line != null);

      if (providerName != null && providerDir != null){
        Class providerClass = Class.forName(providerName);
        Constructor constructor = providerClass.getConstructor(String.class);
        provider = (CheckpointProvider) constructor.newInstance(new Object[]
        {providerDir});
      }
    } catch (Exception e) {
      //return default provider
      LOG.debug("Error creating CheckPointProvider", e);
      return null;
    }
    return provider;
  }



}
