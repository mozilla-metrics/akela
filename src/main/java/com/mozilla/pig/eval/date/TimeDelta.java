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

public class TimeDelta  extends EvalFunc<Long> {

    public static enum ERRORS { DateParseError };

    private int deltaUnit;
    private boolean parseDate = false;
    private SimpleDateFormat sdf;
    
    public TimeDelta() {
        deltaUnit = Calendar.MILLISECOND;
    }
    
    public TimeDelta(String deltaUnitSr) throws ParseException {
        this(deltaUnitSr, null);
    }
    
    public TimeDelta(String deltaUnitStr, String dateFormat) throws ParseException {
        // WEEK_OF_YEAR = 3
        // DATE = 5
        deltaUnit = Integer.parseInt(deltaUnitStr);
        if (dateFormat != null) {
            parseDate = true;
            sdf = new SimpleDateFormat(dateFormat);
        }
    }
    
    @Override
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || 
            input.get(0) == null || input.get(1) == null) {
            return null;
        }

        long delta = 0;
        if (parseDate) {
            try {
                Date d1 = sdf.parse((String)input.get(0));
                Date d2 = sdf.parse((String)input.get(1));
                delta = DateUtil.getTimeDelta(d1.getTime(), d2.getTime(), deltaUnit);
            } catch (ParseException e) {
                pigLogger.warn(this, "Date parse error", ERRORS.DateParseError);
            }
        } else {
            long t1 = ((Number)input.get(0)).longValue();
            long t2 = ((Number)input.get(1)).longValue();
            delta = DateUtil.getTimeDelta(t1, t2, deltaUnit);
        }

        return delta;
    }

}
