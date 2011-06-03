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
package com.mozilla.pig.eval.text;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Normalize extends EvalFunc<Tuple> {

	private static TupleFactory tupleFactory = TupleFactory.getInstance();
	
	private static final Pattern spacePattern = Pattern.compile("\\s+");
	private static final Pattern punctPattern = Pattern.compile("\\p{Punct}");
	
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() > 0) {
			return null;
		}
		
		String normStr = ((String)input.get(0)).trim().toLowerCase();
		normStr = punctPattern.matcher(normStr).replaceAll(" ");
		normStr = spacePattern.matcher(normStr).replaceAll(" ");
		
		Tuple result = tupleFactory.newTuple();
		for (String s : spacePattern.split(normStr)) {
			result.append(s);
		}
		
		return result;
	}
	
}
