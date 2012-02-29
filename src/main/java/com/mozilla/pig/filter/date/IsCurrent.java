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
import static java.util.Calendar.*;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class IsCurrent extends FilterFunc {

    public static enum ERRORS { ParseError };
    
    private SimpleDateFormat sdf;
    private int field;
    private Calendar now;
    
    public IsCurrent(String field, String dateFormat) {
        if ("month".equalsIgnoreCase(field)) {
            this.field = MONTH;
        } else if ("week".equalsIgnoreCase(field)) {
            this.field = WEEK_OF_YEAR;
        } else {
            this.field = DATE;
        }
        sdf = new SimpleDateFormat(dateFormat);
        now = Calendar.getInstance();
    }
    
    @Override
    public Boolean exec(Tuple input) throws IOException {
        boolean match = false;
        try {
            Date d = sdf.parse((String)input.get(0));
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            
            switch(field) {
                case MONTH:
                    match = (now.get(MONTH) == cal.get(MONTH));
                    break;
                case WEEK_OF_YEAR:
                    match = (now.get(WEEK_OF_YEAR) == cal.get(WEEK_OF_YEAR) && now.get(YEAR) == cal.get(YEAR));
                    break;
                case DATE:
                    match = (now.get(DATE) == cal.get(DATE));
                    break;
                default:
                    break;
            }
        } catch (ParseException e) {
            pigLogger.warn(this, "Parse exception: " + e.getMessage(), ERRORS.ParseError);
        }
        
        return match;
    }

}
