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

package com.mozilla.pig.eval.regex;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class FindAll extends EvalFunc<Tuple> {

	private static TupleFactory tupleFactory = TupleFactory.getInstance();
	
	public Tuple exec(Tuple input) throws IOException {
		if (input == null) {
			return null;
		}
		
		if (input.size() != 2) {
			throw new IOException("FindAll requires exactly 2 parameters");
		}

		Pattern p = Pattern.compile((String)input.get(1));
		Matcher m = p.matcher((String)input.get(0));
		if (!m.find()) {
			return null;
		} 
		
		Tuple result = tupleFactory.newTuple();
		// Add the one we just found
		result.append(m.group());
		// Look for more
		while (m.find()) {
			result.append(m.group());
		}
		
		return result;
	}
	
}
