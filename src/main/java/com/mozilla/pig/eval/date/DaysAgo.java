/*
 * Copyright 2012 Mozilla Foundation
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
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.mozilla.util.DateUtil;

public class DaysAgo extends EvalFunc<Integer> {

public static enum ERRORS { DateParseError };
    
    private static final double DAY_IN_MILLIS = 86400000.0d;
    
    private SimpleDateFormat sdf;
    private long currentDay;
    
    public DaysAgo(String dateFormat) {
        sdf = new SimpleDateFormat(dateFormat);
        currentDay = DateUtil.getTimeAtResolution(System.currentTimeMillis(), Calendar.DATE);
    }
    
    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        Integer daysAgo = null;
        try {
            Date d = sdf.parse((String)input.get(0));
            long delta = currentDay - d.getTime(); 
            daysAgo = (int)Math.floor((double)delta / DAY_IN_MILLIS); 
        } catch (ParseException e) {
            pigLogger.warn(this, "Date parsing error", ERRORS.DateParseError);
        }
        
        return daysAgo;
    }

}
