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
