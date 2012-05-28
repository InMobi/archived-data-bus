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

public class DatabusConfig {

  private final Map<String, Cluster> clusters;
  private final Map<String, SourceStream> streams;
  private final Map<String, String> defaults;
  
  public DatabusConfig( 
      Map<String, SourceStream> streams,
      Map<String, Cluster> clusterMap, Map<String, String> defaults) {
    this.streams = streams;
    this.clusters = clusterMap;
    this.defaults = defaults;
  }

  public Cluster getPrimaryClusterForDestinationStream(String streamName) {
   for(Cluster cluster : getClusters().values()) {
     if (cluster.getDestinationStreams().containsKey(streamName)) {
       DestinationStream consumeStream = cluster.getDestinationStreams().get(streamName);
       if (consumeStream.isPrimary())
         return cluster;
     }
   }
    return null;
  }

  public Map<String, Cluster> getClusters() {
    return clusters;
  }

  public Map<String, SourceStream> getSourceStreams() {
    return streams;
  }

  public Map<String, String> getDefaults() {
    return defaults;
  }

}
