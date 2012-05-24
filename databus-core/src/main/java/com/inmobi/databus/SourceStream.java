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

import java.util.Map;
import java.util.Set;

public class SourceStream {
  private final String name;
  //Map of ClusterName, Retention for a stream
  private final Map<String, Integer> sourceClusters;


  public SourceStream(String name, Map<String, Integer> sourceClusters) {
    super();
    this.name = name;
    this.sourceClusters = sourceClusters;
  }

  public int getRetentionInHours(String clusterName) {
    int clusterRetention = sourceClusters.get(clusterName).intValue();
    return clusterRetention;
  }

  public Set<String> getSourceClusters() {
    return sourceClusters.keySet();
  }

  public String getName() {
    return name;
  }


}
