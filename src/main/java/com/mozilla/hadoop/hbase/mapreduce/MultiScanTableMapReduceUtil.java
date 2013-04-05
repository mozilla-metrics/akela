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

package com.mozilla.hadoop.hbase.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

import com.mozilla.util.DateUtil;
import com.mozilla.util.Pair;

public class MultiScanTableMapReduceUtil {

	private static Log LOG = LogFactory.getLog(MultiScanTableMapReduceUtil.class);
	
	/**
	 * Use this before submitting a TableMap job. It will appropriately set up
	 * the job.
	 * 
	 * @param table
	 *            The table name to read from.
	 * @param scans
	 *            The scan instances with the columns, time range etc.
	 * @param mapper
	 *            The mapper class to use.
	 * @param outputKeyClass
	 *            The class of the output key.
	 * @param outputValueClass
	 *            The class of the output value.
	 * @param job
	 *            The current job to adjust.
	 * @throws IOException
	 *             When setting up the details fails.
	 */
	@SuppressWarnings("rawtypes")
	public static void initMultiScanTableMapperJob(final String table, final Scan[] scans,
			final Class<? extends TableMapper> mapper, final Class<? extends WritableComparable> outputKeyClass,
			final Class<? extends Writable> outputValueClass, final Job job) throws IOException {
		
		job.setInputFormatClass(MultiScanTableInputFormat.class);
	    if (outputValueClass != null) {
	    	job.setMapOutputValueClass(outputValueClass);
	    }
	    if (outputKeyClass != null) {
	    	job.setMapOutputKeyClass(outputKeyClass);
	    }
        if (mapper != null) {
            job.setMapperClass(mapper);
        }
		job.getConfiguration().set(MultiScanTableInputFormat.INPUT_TABLE, table);
		job.getConfiguration().set(MultiScanTableInputFormat.SCANS, convertScanArrayToString(scans));
	}

	/**
	 * Converts base64 scans string back into a Scan array
	 * @param base64
	 * @return
	 * @throws IOException
	 */
	public static Scan[] convertStringToScanArray(final String base64) throws IOException {
		final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(Base64.decode(base64)));

		ArrayWritable aw = new ArrayWritable(Scan.class);
		aw.readFields(dis);
		
		Writable[] writables = aw.get();
		Scan[] scans = new Scan[writables.length];
		for (int i=0; i < writables.length; i++) {
			scans[i] = (Scan)writables[i];
		}

		return scans;
	}

	/**
	 * Converts an array of Scan objects into a base64 string
	 * @param scans
	 * @return
	 * @throws IOException
	 */
	public static String convertScanArrayToString(final Scan[] scans) throws IOException {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);

		ArrayWritable aw = new ArrayWritable(Scan.class, scans);
		aw.write(dos);

		return Base64.encodeBytes(baos.toByteArray());
	}

	/**
	 * Generates an array of scans for different salted ranges for the given dates
	 * This is really only useful for Mozilla Socorro in the way it does row-id hashing
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public static Scan[] generateScans(Calendar startCal, Calendar endCal, List<Pair<String,String>> columns, int caching, boolean cacheBlocks) {
		return generateHexPrefixScans(startCal, endCal, "yyMMdd", columns, caching, cacheBlocks);
	}
	
	/**
	 * Generates an array of scans for hex character and date prefixed ranges for the given dates (e.g. 16 buckets)
	 * @param startCal
	 * @param endCal
	 * @param dateFormat
	 * @param columns
	 * @param caching
	 * @param cacheBlocks
	 * @return
	 */
	public static Scan[] generateHexPrefixScans(Calendar startCal, Calendar endCal, String dateFormat, List<Pair<String,String>> columns, int caching, boolean cacheBlocks) {
		ArrayList<Scan> scans = new ArrayList<Scan>();		
		String[] salts = new String[16];
		for (int i=0; i < 16; i++) {
			salts[i] = Integer.toHexString(i);
		}
		
		SimpleDateFormat rowsdf = new SimpleDateFormat(dateFormat);
		long endTime = DateUtil.getEndTimeAtResolution(endCal.getTimeInMillis(), Calendar.DATE);
		
		while (startCal.getTimeInMillis() < endTime) {
			int d = Integer.parseInt(rowsdf.format(startCal.getTime()));
			
			for (int i=0; i < salts.length; i++) {
				Scan s = new Scan();
				s.setCaching(caching);
				s.setCacheBlocks(cacheBlocks);
				
				// add columns
				for (Pair<String,String> pair : columns) {
					s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
				}
				
				s.setStartRow(Bytes.toBytes(salts[i] + String.format("%06d", d)));
				s.setStopRow(Bytes.toBytes(salts[i] + String.format("%06d", d + 1)));
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding start-stop range: " + salts[i] + String.format("%06d", d) + " - " + salts[i] + String.format("%06d", d + 1));
				}
				
				scans.add(s);
			}
			
			startCal.add(Calendar.DATE, 1);
		}
		
		return scans.toArray(new Scan[scans.size()]);
	}
	
	/**
	 * Generates an array of scans for byte and date prefixed ranges for the given dates (e.g. 256 buckets)
	 * @param startCal
	 * @param endCal
	 * @param dateFormat
	 * @param columns
	 * @param caching
	 * @param cacheBlocks
	 * @return
	 */
	public static Scan[] generateBytePrefixScans(Calendar startCal, Calendar endCal, String dateFormat, List<Pair<String,String>> columns, int caching, boolean cacheBlocks) {
	    return generateBytePrefixScans(startCal, endCal, dateFormat, columns, caching, cacheBlocks, 1);
	}
	
	/**
	 * Same as above version but allows for specifying batch size as well
	 * @param startCal
	 * @param endCal
	 * @param dateFormat
	 * @param columns
	 * @param caching
	 * @param cacheBlocks
	 * @param batch
	 * @return
	 */
	public static Scan[] generateBytePrefixScans(Calendar startCal, Calendar endCal, String dateFormat, List<Pair<String,String>> columns, int caching, boolean cacheBlocks, int batch) {
		ArrayList<Scan> scans = new ArrayList<Scan>();
		
		SimpleDateFormat rowsdf = new SimpleDateFormat(dateFormat);
		long endTime = DateUtil.getEndTimeAtResolution(endCal.getTimeInMillis(), Calendar.DATE);
		
		byte[] temp = new byte[1];
		while (startCal.getTimeInMillis() < endTime) {		
			for (int b=Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
			    int d = Integer.parseInt(rowsdf.format(startCal.getTime()));
			    
				Scan s = new Scan();
				s.setCaching(caching);
				s.setCacheBlocks(cacheBlocks);
				if (batch > 1) {
				    s.setBatch(batch);
				}
				// add columns
				for (Pair<String,String> pair : columns) {
                    s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
                }
				
				temp[0] = (byte) b;
				s.setStartRow(Bytes.add(temp , Bytes.toBytes(String.format("%06d", d))));
				s.setStopRow(Bytes.add(temp , Bytes.toBytes(String.format("%06d", d + 1))));
				if (LOG.isDebugEnabled()) {
                    LOG.info("Adding start-stop range: " + temp + String.format("%06d", d) + " - " + temp + String.format("%06d", d + 1));
                }
				
				scans.add(s);
			}
			
			startCal.add(Calendar.DATE, 1);
		}
		
		return scans.toArray(new Scan[scans.size()]);
	}
}
