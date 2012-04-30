package com.inmobi.databus;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestFileCheckpointProvider {

  @Test
  public void test() {
    CheckpointProvider provider = new FSCheckpointProvider("target/");
    Assert.assertNull(provider.read("notpresent"));

    String key = "t1";
    byte[] ck = "test".getBytes();
    provider.checkpoint("t1", ck);
    Assert.assertEquals(provider.read(key), ck);

    byte[] ck1 = "test1".getBytes();
    provider.checkpoint("t1", ck1);
    Assert.assertEquals(provider.read(key), ck1);
    provider.close();
  }

}
