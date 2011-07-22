/*
 * Copyright 2011 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.pig.storage;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class DocumentVectorStorage extends StoreFunc {

    private static final Logger LOG = Logger.getLogger(DocumentVectorStorage.class);
    
	@SuppressWarnings("rawtypes")
	protected RecordWriter writer = null;
	
	private Text outputKey = new Text();
	private VectorWritable outputValue = new VectorWritable();
	private final int dimensions;
	
	public DocumentVectorStorage(String dimension) {
	    super();
	    this.dimensions = Integer.parseInt(dimension);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new SequenceFileOutputFormat<Text, VectorWritable>();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putNext(Tuple tuple) throws IOException {
		outputKey.set((String)tuple.get(0));
		Tuple vectorTuple = (Tuple)tuple.get(1);
		Vector vector = new NamedVector(new RandomAccessSparseVector(dimensions, vectorTuple.size()), outputKey.toString());
		for (int i=0; i < vectorTuple.size(); i++) {
		    Object o = vectorTuple.get(i);
		    switch (vectorTuple.getType(i)) {
		        case DataType.INTEGER:
		            // If this is just an integer then we just want to set the index to 1.0
		            vector.set((Integer)o, 1.0);
		            break;
		        case DataType.TUPLE:
		            // If this is a tuple then we want to set the index and the weight
		            Tuple subt = (Tuple)o;
		            vector.set((Integer)subt.get(0), (Double)subt.get(1));
		            break;
		        default:
		            throw new RuntimeException("Unexpected tuple form");
		    }
		    
		}
		outputValue.set(vector);
		try {
			writer.write(outputKey, outputValue);
		} catch (InterruptedException e) {
		    LOG.error("Interrupted while writing", e);
		}
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

}
