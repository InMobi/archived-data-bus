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
	public List<Path> compareDataConsistency(TreeMap<String, Path>
		sourceStreamFiles, TreeMap<String, Path> destStreamFiles, List<Path>
			inconsistency) {
		Set<Entry<String, Path>> localStreamFileEntries = sourceStreamFiles.
			entrySet();
		Set<Entry<String, Path>> mergedStreamFileEntries = destStreamFiles.
			entrySet();
		Iterator<Entry<String, Path>> localIt = localStreamFileEntries.iterator();
		Iterator<Entry<String, Path>> mergedIt = mergedStreamFileEntries.iterator();
		String localKey = null;
		String mergedKey = null;
		if (localIt.hasNext()) {
			localKey = localIt.next().getKey();
		}
		if (mergedIt.hasNext()) {
			mergedKey = mergedIt.next().getKey();
		}
		while ((localKey != null) && (mergedKey != null)) {
			if (!localKey.equals(mergedKey)) {
				if(localKey.compareTo(mergedKey) < 0) {
					System.out.println("missing path: " + sourceStreamFiles.get(localKey));
					inconsistency.add(sourceStreamFiles.get(localKey));
					if (localIt.hasNext()) {
						localKey = localIt.next().getKey();
					} else {
						localKey = null;
					}
				} else {
					System.out.println("data replay: " + destStreamFiles.get(mergedKey));
					inconsistency.add(destStreamFiles.get(mergedKey));
					if (mergedIt.hasNext()) {	
						mergedKey = mergedIt.next().getKey();
					} else {
						mergedKey = null;
					}
				}
			} else {
				if (localIt.hasNext() && !mergedIt.hasNext()) {
					localKey = localIt.next().getKey();
					mergedKey = null;
				} else if (mergedIt.hasNext() && !localIt.hasNext()) {
					mergedKey = mergedIt.next().getKey();
					localKey = null;
				} else if (localIt.hasNext() && mergedIt.hasNext()) {
					localKey = localIt.next().getKey();
					mergedKey = mergedIt.next().getKey();
				} else {
					localKey = null;
					mergedKey = null;
				}
			}
		}
		if ((sourceStreamFiles.size() == destStreamFiles.size()) &&
				localKey == null && mergedKey == null) {
			System.out.println("there are no missing files");
		} else {
			if (mergedKey == null) {
				while (localKey != null) {
					inconsistency.add(sourceStreamFiles.get(localKey));
					System.out.println("Files to be sent: " +
						sourceStreamFiles.get(localKey));
					if (localIt.hasNext()) {
						localKey = localIt.next().getKey();
					} else {
						localKey = null;
					}
				}
			} else {
				while (mergedKey != null) {
					inconsistency.add(destStreamFiles.get(mergedKey));
					System.out.println("extra files in stream: " +
						destStreamFiles.get(mergedKey));
					if (mergedIt.hasNext()) {
						mergedKey = mergedIt.next().getKey();
					} else {
						mergedKey = null;
					}
				}
			}
		}
		return inconsistency;
	}
}


