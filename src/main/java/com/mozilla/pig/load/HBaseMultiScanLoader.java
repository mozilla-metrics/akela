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

package com.mozilla.pig.load;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableInputFormat;
import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil;

/**
 * A Pig 0.7+ loader for HBase tables using MultiScanTableInputFormat
 */
public class HBaseMultiScanLoader extends LoadFunc {

	private Configuration conf = new Configuration();
	private RecordReader<ImmutableBytesWritable,Result> reader;
	private Scan[] scans;
	private Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>(); // family:qualifier
	
	private static final Log LOG = LogFactory.getLog(HBaseMultiScanLoader.class);

	/**
	 * Constructor. Construct a HBase Table loader to load the cells of the
	 * provided columns within the provided dates.
	 * @param startDate the start date given in yyyyMMdd format
	 * @param stopDate the stop date given in yyyyMMdd format 
	 * @param columnList columns given in a comma-delimited list using colon between family and qualifier
	 */
	public HBaseMultiScanLoader(String startDate, String stopDate, String columnList) {
		String[] colPairs = columnList.split(",");
		for (int i=0; i < colPairs.length; i++) {
			String[] familyQualifier = colPairs[i].split(":");
			if (LOG.isDebugEnabled()) {
				LOG.debug("Adding column to map: " + colPairs[i]);
			}
			if (familyQualifier.length == 2) {
				columns.put(Bytes.toBytes(familyQualifier[0]), Bytes.toBytes(familyQualifier[1]));
			} else {
				columns.put(Bytes.toBytes(familyQualifier[0]), Bytes.toBytes(""));
			}
		}
		
		Calendar startCal = Calendar.getInstance();
		Calendar endCal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		try {
			startCal.setTime(sdf.parse(startDate));
			endCal.setTime(sdf.parse(stopDate));
		} catch (ParseException e) {
			LOG.error("Error parsing start/stop dates", e);
		}

		scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, "yyyyMMdd", columns, 100, true);
	}
	
	/**
	 * Constructor. Construct a HBase Table loader to load the cells of the
	 * provided columns within the provided row ranges.
	 * @param rowRanges scan row ranges given in comma-delimited list using hyphen between startRow and stopRow
	 * @param columnList columns given in a comma-delimited list using colon between family and qualifier
	 */
	public HBaseMultiScanLoader(String rowRanges, String columnList) {
		String[] ranges = rowRanges.split(",");
		scans = new Scan[ranges.length];
		for (int i=0; i < ranges.length; i++) {
			String[] startEnd = ranges[i].split("-");
			if (startEnd.length == 2) {
				Scan s = new Scan();
				if (LOG.isDebugEnabled()) {
					LOG.debug("Using start key: " + startEnd[0] + " end key: " + startEnd[1]);
				}
				s.setStartRow(Bytes.toBytes(startEnd[0]));
				s.setStopRow(Bytes.toBytes(startEnd[1]));
				scans[i] = s;
			}
		}
		
		String[] colPairs = columnList.split(",");
		for (int i=0; i < colPairs.length; i++) {
			String[] familyQualifier = colPairs[i].split(":");
			if (LOG.isDebugEnabled()) {
				LOG.debug("Adding column to map: " + colPairs[i]);
			}
			if (familyQualifier.length == 2) {
				columns.put(Bytes.toBytes(familyQualifier[0]), Bytes.toBytes(familyQualifier[1]));
			} else {
				columns.put(Bytes.toBytes(familyQualifier[0]), Bytes.toBytes(""));
			}
		}
	}

	/**
	 * Generates an array of scans for different salted ranges for the given dates
	 * @param startDate
	 * @param endDate
	 * @return
	 */
//	public static Scan[] generateScans(Calendar startCal, Calendar endCal, Map<byte[], byte[]> columns) {
//		SimpleDateFormat rowsdf = new SimpleDateFormat("yyMMdd");
//		
//		ArrayList<Scan> scans = new ArrayList<Scan>();		
//		String[] salts = new String[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };
//		
//		long endTime = DateUtil.getEndTimeAtResolution(endCal.getTimeInMillis(), Calendar.DATE);
//		
//		while (startCal.getTimeInMillis() < endTime) {
//			int d = Integer.parseInt(rowsdf.format(startCal.getTime()));
//			
//			for (int i=0; i < salts.length; i++) {
//				Scan s = new Scan();
//				// TODO: make caching an option
//				s.setCaching(100);
//				// TODO: make this an option
//				s.setCacheBlocks(false);
//				
//				// add columns
//				for (Map.Entry<byte[], byte[]> col : columns.entrySet()) {
//					s.addColumn(col.getKey(), col.getValue());
//				}
//				
//				s.setStartRow(Bytes.toBytes(salts[i] + String.format("%06d", d)));
//				s.setStopRow(Bytes.toBytes(salts[i] + String.format("%06d", d + 1)));
//				
//				if (LOG.isDebugEnabled()) {
//					LOG.debug("Adding start-stop range: " + salts[i] + String.format("%06d", d) + " - " + salts[i] + String.format("%06d", d + 1));
//				}
//				
//				scans.add(s);
//			}
//			
//			startCal.add(Calendar.DATE, 1);
//		}
//		
//		return scans.toArray(new Scan[scans.size()]);
//	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			if (reader.nextKeyValue()) {
				ImmutableBytesWritable rowKey = reader.getCurrentKey();
				Result result = reader.getCurrentValue();
				Tuple tuple = TupleFactory.getInstance().newTuple(columns.size()+1);
				tuple.set(0, new String(rowKey.get()));
				int i = 1;
				for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
					byte[] v = result.getValue(entry.getKey(), entry.getValue());
					if (v != null) {
						tuple.set(i, new DataByteArray(v));
					}
					i++;
				}
				return tuple;
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return null;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public InputFormat getInputFormat() {
		MultiScanTableInputFormat inputFormat = new MultiScanTableInputFormat();
		inputFormat.setConf(conf);
		return inputFormat;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void prepareToRead(RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	@Override
    public void setLocation(String location, Job job) throws IOException {
        if (location.startsWith("hbase://")) {
        	conf.set(MultiScanTableInputFormat.INPUT_TABLE, location.substring(8));
        } else {
        	conf.set(MultiScanTableInputFormat.INPUT_TABLE, location);
        }
        conf.set(MultiScanTableInputFormat.SCANS, MultiScanTableMapReduceUtil.convertScanArrayToString(scans));
    }

	@Override
	public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
		return location;
	}

}
