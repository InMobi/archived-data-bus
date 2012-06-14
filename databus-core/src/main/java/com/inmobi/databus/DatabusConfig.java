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

import java.util.HashMap;
import java.util.Map;

public class DatabusConfig {

  private Map<String, Stream> Streams;
  private Map<String, Cluster> Clusters;
  private Map<String, String> Defaults;
  
  public DatabusConfig(Map<String, Stream> Streams,
      Map<String, Cluster> Clusters, Map<String, String> Defaults) {
    this.Streams = Streams;
    this.Clusters = Clusters;
    this.Defaults = Defaults;
  }

  public Map<String, Cluster> getAllClusters() {
    return Clusters;
  }

  public Map<String, Stream> getAllStreams() {
    return Streams;
  }

  public Map<String, String> getDefaults() {
    return Defaults;
  }

}
