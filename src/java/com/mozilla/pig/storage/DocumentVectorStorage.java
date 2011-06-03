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
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

public class DocumentVectorStorage extends StoreFunc {

	@SuppressWarnings("rawtypes")
	protected RecordWriter writer = null;
	
	private Text outputKey = new Text();
	private VectorWritable outputValue = new VectorWritable();
	
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
		Tuple vecTuple = (Tuple)tuple.get(1);
		Vector v = new RandomAccessSparseVector(vecTuple.size());
		for (int i=0; i < vecTuple.size(); i++) {
			Integer idx = (Integer)vecTuple.get(i);
			v.set(i, idx.doubleValue());
		}
		outputValue.set(v);
		try {
			writer.write(outputKey, outputValue);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

}
