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

package com.mozilla.pig.filter.date;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import com.mozilla.util.DateUtil;

public class DateInRange extends FilterFunc {

    public static enum ERRORS { ParseError };
    
    private long startTime = 0L;
    private long endTime = 0L;
    private SimpleDateFormat edf;
    
    public DateInRange(String startDateStr, String endDateStr, String dateEntryFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        
        Date startDate = sdf.parse(startDateStr);
        startTime = DateUtil.getTimeAtResolution(startDate.getTime(), Calendar.DATE);
        
        Date endDate = sdf.parse(endDateStr);
        endTime = DateUtil.getEndTimeAtResolution(endDate.getTime(), Calendar.DATE);
        
        edf = new SimpleDateFormat(dateEntryFormat);
    }
    
    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            return false;
        }
        
        boolean inRange = false;
        Date d;
        try {
            d = edf.parse((String)input.get(0));
            long t = d.getTime();
            inRange = (t >= startTime && t <= endTime);
        } catch (ParseException e) {
            pigLogger.warn(this, "Parse exception: " + e.getMessage(), ERRORS.ParseError);
        } 
        
        return inRange;
    }

}
