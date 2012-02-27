  # What is Databus?
  Yet another streaming data transport system

# Why do we need when scribe, kafka, flume and other system exist?
  Our requirements to have data in HDFS and capability to view the same
  stream across D.C.'s and support it's mirrors for BCP didn't exist in kafka
   and flume during our evaluation. Scribe supports capability to bring data
   to HDFS in minute files, however a global streaming view of data across
   datacenters and it's mirrors in different data centers didn't exist.

# Goals
  * Provide a global & local view of data streams generating in different
  clusters and data centers.
  * Improved WAN bandwidth utilization by streaming data in incremental steps
  avoiding sudden bursts/spikes.
  * No loss of data once it reaches local cluster HDFS
  * Provide hot-hot fail over for all data when data center/cluster goes down.
  * Ability to have single primary and multiple mirrors of a stream across
  different clusters/data centers.

# Assumptions
  * Data consumption isn't real time. Granularity of data is in minute level
  files.
  * Latency of 1-2 minutes in local data center, 2-3 minutes in remote data
  center and 4-5 minutes in the mirrored is acceptable.
  * "_" is the recommended character to use in stream Name. You should never
  use "-" in stream names as it's reserved by DATABUS
  * All clusters across data centers should run in the same TIME ZONE. Databus
   uses time as paths for files and for mirroring/merging we need same paths across D.C.'s.
   Recommendation - UTC.

# Concepts
  * Stream - A continuous data source having similar data/records.
  * Local Stream - Minute files compressed on hadoop produced in local data
  centers
  * Merged Stream - Global view of all minute files generated in different
  clusters for same stream
  * Mirrored Stream - Copy of Merged Stream with paths being preserved.
                    
# Databus from 1000ft
![diagram](https://github.com/InMobi/data-bus/blob/master/doc/Databus-HighLevel-Arch.png?raw=true)   
                    

# How to use data bus
  * Download the package from guthub which has all the required JARS
  * configure databus.xml
  * configure databus.cfg
  * Starting - databus.sh start databus.cfg
  * Stopping - databus.sh stop databus.cfg
             

            



