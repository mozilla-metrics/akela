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
package com.mozilla.pig.eval.ml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Vectorizer extends EvalFunc<Tuple> {

	private Map<String,Integer> featureIndex;
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();
	
	private void loadFeatureIndex(String featureIndexPath) throws IOException {
		if (featureIndex == null) {
			featureIndex = new HashMap<String,Integer>();
			
			Path p = new Path(featureIndexPath);
			FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
			for (FileStatus status : fs.listStatus(p)) {
				if (!status.isDir()) {
					BufferedReader reader = null;
					try {
						reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
						String line = null;
						int lineNumber = 0;
						while ((line = reader.readLine()) != null) {
							featureIndex.put(line.trim(), lineNumber++);
						}
					} finally {
						if (reader != null) {
							reader.close();
						}
					}
				}
			}
			
			log.info("Loaded feature index with size: " + featureIndex.size());
		}
	}
	
	public Tuple exec(Tuple input) throws IOException {
		if (input == null) {
			return null;
		}
		
		if (input.size() != 2) {
			throw new IOException("Vectorizer requires exactly 2 parameters");
		}
		
		String featureIndexPath = (String)input.get(0);
		if (featureIndex == null) {
			loadFeatureIndex(featureIndexPath);
		}
		
		Tuple output = tupleFactory.newTuple();
		DataBag db = (DataBag)input.get(1);
		for (Tuple t : db) {
		    // Expects each tuple's first element to be the feature
			Integer idx = featureIndex.get((String)t.get(0));
			if (idx != null) {
				output.append(idx);
			}
		}
		
		return output;
	}

}
