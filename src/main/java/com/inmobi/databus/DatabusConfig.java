package com.inmobi.databus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatabusConfig {

  private String CONFIG = "databus.xml";

  public DatabusConfig() {
    
  }

  public Map<String, Set<String>> getDestinationStreamMap() {
    Map<String, Set<String>> map = new HashMap<String, Set<String>>();
    map.put("category1", new HashSet<String>());
    map.get("category1").add("ua2");
    map.get("category1").add("uj");
    
    map.put("category2", new HashSet<String>());
    map.get("category2").add("er");
    map.get("category2").add("uj");
    return map;
  }

  public Map<String, String> getDestinations() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("ua2", "hdfs://");
    map.put("er", "hdfs://");
    map.put("uj", "hdfs://");
    return map;
  }
}
