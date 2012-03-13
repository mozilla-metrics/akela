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

package com.mozilla.pig.eval.date;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ConvertDateFormat extends EvalFunc<String> {

	public static enum ERRORS { DateParseError };
	
	private SimpleDateFormat inputSdf;
	private SimpleDateFormat outputSdf;
	
	public ConvertDateFormat(String inputDateFormat, String outputDateFormat) {
	    this.inputSdf = new SimpleDateFormat(inputDateFormat);
	    this.outputSdf = new SimpleDateFormat(outputDateFormat);
	}
	
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}

		String s = null;
		try {
			Date d = inputSdf.parse((String)input.get(0));
			s = outputSdf.format(d);
		} catch (ParseException e) {
			pigLogger.warn(this, "Date parsing error", ERRORS.DateParseError);
		}
		
		return s;
	}

}
