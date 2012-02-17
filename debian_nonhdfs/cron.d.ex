#
# Regular cron jobs for the scribe-server-orig package
#
0 4	* * *	root	[ -x /usr/bin/scribe-server-orig_maintenance ] && /usr/bin/scribe-server-orig_maintenance
