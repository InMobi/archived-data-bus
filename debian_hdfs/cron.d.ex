#
# Regular cron jobs for the scribe-server-hdfs-orig package
#
0 4	* * *	root	[ -x /usr/bin/scribe-server-hdfs-orig_maintenance ] && /usr/bin/scribe-server-hdfs-orig_maintenance
