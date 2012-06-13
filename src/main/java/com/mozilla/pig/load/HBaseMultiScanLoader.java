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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import com.mozilla.util.Pair;

/**
 * A Pig 0.7+ loader for HBase tables using MultiScanTableInputFormat
 */
public class HBaseMultiScanLoader extends LoadFunc {

	private Configuration conf = new Configuration();
	private RecordReader<ImmutableBytesWritable,Result> reader;
	private Scan[] scans;
	private List<Pair<String,String>> columns = new ArrayList<Pair<String,String>>(); // family:qualifier
	
	private static final Log LOG = LogFactory.getLog(HBaseMultiScanLoader.class);

	/**
	 * @param startDate
	 * @param stopDate
	 * @param dateFormat
	 * @param columnList
	 */
	public HBaseMultiScanLoader(String startDate, String stopDate, String dateFormat, String columnList) {
	    this(startDate, stopDate, dateFormat, columnList, 100, "false");
	}
	
	/**
	 * @param startDate
	 * @param stopDate
	 * @param dateFormat
	 * @param columnList
	 * @param useHexSalts
	 */
	public HBaseMultiScanLoader(String startDate, String stopDate, String dateFormat, String columnList, String useHexSalts) {
	    this(startDate, stopDate, dateFormat, columnList, 100, useHexSalts);
	}
	
	/**
	 * @param startDate
	 * @param stopDate
	 * @param dateFormat
	 * @param columnList
	 * @param useHexSalts
	 */
	public HBaseMultiScanLoader(String startDate, String stopDate, String dateFormat, String columnList, int caching, String useHexSalts) {
		String[] colPairs = columnList.split(",");
		for (int i=0; i < colPairs.length; i++) {
			String[] familyQualifier = colPairs[i].split(":");
			if (LOG.isDebugEnabled()) {
				LOG.debug("Adding column to map: " + colPairs[i]);
			}
			if (familyQualifier.length == 2) {
				columns.add(new Pair<String,String>(familyQualifier[0], familyQualifier[1]));
			} else {
			    columns.add(new Pair<String,String>(familyQualifier[0], ""));
			}
		}
		
		Calendar startCal = Calendar.getInstance();
		Calendar endCal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		
		try {
			startCal.setTime(sdf.parse(startDate));
			endCal.setTime(sdf.parse(stopDate));
		} catch (ParseException e) {
			LOG.error("Error parsing start/stop dates", e);
		}

		boolean useHex = Boolean.parseBoolean(useHexSalts);
		if (useHex) {
		    scans = MultiScanTableMapReduceUtil.generateHexPrefixScans(startCal, endCal, dateFormat, columns, caching, false);
		} else {
		    scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, dateFormat, columns, caching, false);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#getNext()
	 */
	@Override
	public Tuple getNext() throws IOException {
		try {
			if (reader.nextKeyValue()) {
				ImmutableBytesWritable rowKey = reader.getCurrentKey();
				Result result = reader.getCurrentValue();
				Tuple tuple = TupleFactory.getInstance().newTuple(columns.size()+1); 
				tuple.set(0, new DataByteArray(rowKey.get()));
				int i = 1;
				for (Pair<String,String> pair : columns) {
					byte[] v = result.getValue(pair.getFirst().getBytes(), pair.getSecond().getBytes());
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

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#getInputFormat()
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public InputFormat getInputFormat() {
		MultiScanTableInputFormat inputFormat = new MultiScanTableInputFormat();
		inputFormat.setConf(conf);
		return inputFormat;
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.RecordReader, org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit)
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void prepareToRead(RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
	 */
	@Override
    public void setLocation(String location, Job job) throws IOException {
	    job.getConfiguration().setBoolean("pig.noSplitCombination", true);
	    conf = job.getConfiguration();
        HBaseConfiguration.addHbaseResources(conf);
        
        if (location.startsWith("hbase://")) {
        	conf.set(MultiScanTableInputFormat.INPUT_TABLE, location.substring(8));
        } else {
        	conf.set(MultiScanTableInputFormat.INPUT_TABLE, location);
        }
        
        if (conf.get(MultiScanTableInputFormat.SCANS) != null) {
            return;
        }
        
        conf.set(MultiScanTableInputFormat.SCANS, MultiScanTableMapReduceUtil.convertScanArrayToString(scans));
    }

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#relativeToAbsolutePath(java.lang.String, org.apache.hadoop.fs.Path)
	 */
	@Override
	public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
		return location;
	}

}
