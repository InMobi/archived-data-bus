package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;


public abstract class CompareDataConsistency {

	/**
	 * This method compares the data consistency between source and destination streams
	 * @return list of inconsistency paths
	 */
	public List<Path> compareDataConsistency(TreeMap<String, Path> sourceStreamFiles, 
			TreeMap<String, Path> destStreamFiles, List<Path> inconsistency) {
		Set<Entry<String, Path>> sourceStreamFileEntries = sourceStreamFiles.
				entrySet();
		Set<Entry<String, Path>> destStreamFileEntries = destStreamFiles.
				entrySet();
		Iterator<Entry<String, Path>> sourcestreamIt = sourceStreamFileEntries.iterator();
		Iterator<Entry<String, Path>> destStreamIt = destStreamFileEntries.iterator();
		String sourceStreamKey = null;
		String destStreamKey = null;
		if (sourcestreamIt.hasNext()) {
			sourceStreamKey = sourcestreamIt.next().getKey();
		}
		if (destStreamIt.hasNext()) {
			destStreamKey = destStreamIt.next().getKey();
		}
		while ((sourceStreamKey != null) && (destStreamKey != null)) {
			if (!sourceStreamKey.equals(destStreamKey)) {
				if(sourceStreamKey.compareTo(destStreamKey) < 0) {
					System.out.println("missing path: " + sourceStreamFiles.get(sourceStreamKey));
					inconsistency.add(sourceStreamFiles.get(sourceStreamKey));
					if (sourcestreamIt.hasNext()) {
						sourceStreamKey = sourcestreamIt.next().getKey();
					} else {
						sourceStreamKey = null;
					}
				} else {
					System.out.println("data replay: " + destStreamFiles.get(destStreamKey));
					inconsistency.add(destStreamFiles.get(destStreamKey));
					if (destStreamIt.hasNext()) {	
						destStreamKey = destStreamIt.next().getKey();
					} else {
						destStreamKey = null;
					}
				}
			} else {
				if (sourcestreamIt.hasNext() && !destStreamIt.hasNext()) {
					sourceStreamKey = sourcestreamIt.next().getKey();
					destStreamKey = null;
				} else if (destStreamIt.hasNext() && !sourcestreamIt.hasNext()) {
					destStreamKey = destStreamIt.next().getKey();
					sourceStreamKey = null;
				} else if (sourcestreamIt.hasNext() && destStreamIt.hasNext()) {
					sourceStreamKey = sourcestreamIt.next().getKey();
					destStreamKey = destStreamIt.next().getKey();
				} else {
					sourceStreamKey = null;
					destStreamKey = null;
				}
			}
		}
		if ((sourceStreamFiles.size() == destStreamFiles.size()) &&
				sourceStreamKey == null && destStreamKey == null) {
			System.out.println("there are no missing files");
		} else {
			if (destStreamKey == null) {
				while (sourceStreamKey != null) {
					inconsistency.add(sourceStreamFiles.get(sourceStreamKey));
					System.out.println("Files to be sent: " +
							sourceStreamFiles.get(sourceStreamKey));
					if (sourcestreamIt.hasNext()) {
						sourceStreamKey = sourcestreamIt.next().getKey();
					} else {
						sourceStreamKey = null;
					}
				}
			} else {
				while (destStreamKey != null) {
					inconsistency.add(destStreamFiles.get(destStreamKey));
					System.out.println("extra files in stream: " +
							destStreamFiles.get(destStreamKey));
					if (destStreamIt.hasNext()) {
						destStreamKey = destStreamIt.next().getKey();
					} else {
						destStreamKey = null;
					}
				}
			}
		}
		return inconsistency;
	}
}


