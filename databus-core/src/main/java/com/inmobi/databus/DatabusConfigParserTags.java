package com.inmobi.databus;

public interface DatabusConfigParserTags {

  public static final String DEFAULTS = "defaults";
  public static final String ROOTDIR = "rootdir";
  public static final String RETENTION_IN_HOURS = "retentioninhours";
  public static final String TRASH_RETENTION_IN_HOURS = "trashretentioninhours";

  public static final String NAME = "name";
  public static final String STREAM = "stream";
  public static final String SOURCE = "source";
  public static final String DESTINATION = "destination";
  public static final String PRIMARY = "primary";

  public static final String CLUSTER = "cluster";
  public static final String JOB_QUEUE_NAME = "jobqueuename";
  public static final String HDFS_URL = "hdfsurl";
  public static final String JT_URL = "jturl";
}
