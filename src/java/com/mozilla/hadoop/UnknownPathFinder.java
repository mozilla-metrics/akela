/**
 * Copyright 2010 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mozilla.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.log4j.Logger;

/**
 * UnknownPathFinder is designed to look for paths that are present in HDFS that are unknown
 * by the HBase .META. region. It outputs the paths that are unknown so the user can determine what
 * should be done with them.
 * 
 * @author Xavier Stevens
 */
public class UnknownPathFinder {

    private static final Logger LOG = Logger.getLogger(UnknownPathFinder.class);
	
	/**
	 * Get all of the filesystem paths that HBase .META. knows about
	 * @param hbaseRootDir
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public static Set<String> getRegionPaths(Path hbaseRootDir, byte[] tableName) throws IOException {
		Set<String> pathSet = new HashSet<String>();
		
		Scan s = new Scan();
		int i=0;
		HTable t = null;
		ResultScanner scanner = null;
		try {
			t = new HTable(tableName);
			scanner = t.getScanner(s);
			Result result = null;
			while ((result = scanner.next()) != null) {
				byte[] familyQualifierBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
				HRegionInfo hri = Writables.getHRegionInfo(familyQualifierBytes);
				HTableDescriptor htd = hri.getTableDesc();
				Path p = HTableDescriptor.getTableDir(hbaseRootDir, htd.getName());
				pathSet.add(p.toString() + Path.SEPARATOR + hri.getEncodedName());
				i++;
			}
			LOG.info("# of Known Directories in .META.: " + i);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			if (t != null) {
				try {
					t.close();
				} catch (IOException e) {
					LOG.error("Failed to close table!" , e);
				}
			}
		}
		
		return pathSet;
	}
	
	/**
	 * Walk recursively to get all file paths up to a max depth
	 * @param fs
	 * @param inputPath
	 * @param depth
	 * @param maxDepth
	 * @return
	 * @throws IOException
	 */
	public static Set<String> getAllPaths(FileSystem fs, Path inputPath, int depth, int maxDepth) throws IOException {
		Set<String> retPaths = new HashSet<String>();
		for (FileStatus status : fs.listStatus(inputPath)) {
			if (status.isDir() && depth < maxDepth) {
				retPaths.addAll(getAllPaths(fs, status.getPath(), depth + 1, maxDepth));
			} else {
				String p = status.getPath().toString();
				if (!p.contains("-ROOT-") && !p.contains(".META.") && !p.contains(".logs") && 
					!p.contains(".regioninfo") && !p.contains("compaction.dir") && 
					!p.contains("hbase.version")) {
					retPaths.add(p);
				}
			}
		}
		
		return retPaths;
	}
	
	/**
	 * Get all paths in HDFS under the HBase root directory up to region level depth
	 * @param conf
	 * @param hbaseRootDir
	 * @return
	 * @throws IOException
	 */
	public static Set<String> getFilesystemPaths(Configuration conf, Path hbaseRootDir) throws IOException {
		Set<String> fsPaths = null;
		
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
			fsPaths = getAllPaths(hdfs, hbaseRootDir, 0, 1);
		} finally {
			if (hdfs != null) {
				hdfs.close();
			}
		}
		
		LOG.info("# of Directories in filesystem: " + fsPaths.size());
		
		return fsPaths;
	}
	
	/**
	 * Deletes all of the paths specified
	 * @param conf
	 * @param paths
	 * @return
	 * @throws IOException
	 */
	public static boolean deleteFilesystemPaths(Configuration conf, Collection<String> paths) throws IOException {
		boolean success = true;
		
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
			for (String s : paths) {
				Path p = new Path(s);
				if (!hdfs.delete(p, true)) {
					LOG.info("Failed to delete: " + s);
					success = false;
					break;
				} else {
					LOG.info("Successfully deleted: " + s);
				}
			}
		} finally {
			if (hdfs != null) {
				hdfs.close();
			}
		}
		
		return success;
	}
	
	public static void main(String[] args) throws IOException {
		int retCode = 0;
		
		Configuration hbaseConf = HBaseConfiguration.create(new Configuration());
		Path hbaseRootDir = new Path(hbaseConf.get("hbase.rootdir"));
		Set<String> knownRegionPaths = getRegionPaths(hbaseRootDir, HConstants.META_TABLE_NAME);
		Set<String> fsPaths = getFilesystemPaths(hbaseConf, hbaseRootDir);
		fsPaths.removeAll(knownRegionPaths);
		for (String p : fsPaths) {
			System.out.println(p);
		}
		//deleteFilesystemPaths(hbaseConf, fsPaths);
		
		System.exit(retCode);
	}
	
}
