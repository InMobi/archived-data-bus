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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

public class SecureLoginUtil {

  private static final Log LOG = LogFactory.getLog(SecureLoginUtil.class);

  public static void login(String principalConfigKey, String principalValue,
      String keyTabKey, String keyTabValue) throws IOException {
    Configuration conf = new Configuration();
    conf.set(principalConfigKey, principalValue);
    conf.set(keyTabKey, keyTabValue);
    login(conf, principalConfigKey, keyTabKey);
  }

  public static void login(Configuration conf, String principalKey,
      String keyTabKey) throws IOException {
    SecurityUtil.login(conf, keyTabKey, principalKey);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    LOG.info("User logged in :" + ugi);
  }

}
