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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.mozilla.pig.eval.date.ConvertDateFormat.ERRORS;

public class FormatDate extends EvalFunc<String> {
	
    private Calendar cal = Calendar.getInstance();
    private SimpleDateFormat sdf;
    
    public FormatDate(String format) {
        sdf = new SimpleDateFormat(format);
    }
    
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}

		String s = null;
		try {
			cal.setTimeInMillis(((Number)input.get(0)).longValue());
			s = sdf.format(cal.getTime());
		} catch (Exception e) {
			warn("Date parse error: " + e, ERRORS.DateParseError);
		}
		
		return s;
	}
	
}
