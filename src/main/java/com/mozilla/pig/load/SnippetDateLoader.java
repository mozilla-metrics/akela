/**
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
package com.mozilla.pig.load;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.mozilla.util.DateIterator;
import com.mozilla.util.DateUtil;
import com.mozilla.util.StringUtil;

/**
 * SnippetDateLoader uses a specific regex to parse snippet log data
 */
public class SnippetDateLoader extends CustomRegExLoader {
    private static final Log LOG = LogFactory.getLog(SnippetDateLoader.class);
    private static final String sqlDateFormatSpec = "yyyy-MM-dd";
    private static final SimpleDateFormat sqlDateFormat = new SimpleDateFormat(sqlDateFormatSpec);
    private final SimpleDateFormat inputPathFormat;
    private final Calendar start;
    private final Calendar end;
    
    public SnippetDateLoader(String startDate, String endDate) {
        super("^(?>([^\\s]+)\\s([^\\s]*)\\s(?>-|([^-](?:[^\\[\\s]++(?:(?!\\s\\[)[\\[\\s])?)++))\\s\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s[-+]\\d{4})\\]\\s)(?>\"([A-Z]+)\\s([^\\s]*)\\sHTTP/1\\.[01]\"\\s(\\d{3})\\s(\\d+)\\s\"([^\"]+)\"\\s)(?>\"\"?([^\"]*)\"?\")(?>\\s\"([^\"]*)\")?$");
        start = Calendar.getInstance();
        end = Calendar.getInstance();
        try {
            start.setTime(sqlDateFormat.parse(startDate));
            end.setTime(sqlDateFormat.parse(endDate));
        } catch (ParseException e) {
            // TODO Throw a RuntimeException?
        }
        inputPathFormat = sqlDateFormat;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        final List<String> paths = new ArrayList<String>();
        final String finalLocation = location;
        DateUtil.iterateByDay(start.getTimeInMillis(), end.getTimeInMillis(), new DateIterator(){
            @Override
            public void see(long aTime) {
                String aDate = inputPathFormat.format(new Date(aTime));
                String path = finalLocation.replace("%DATE%", aDate);
                LOG.info("Adding an input location: '" + path + "'");
                paths.add(path);
            }
        });
        
        FileInputFormat.setInputPaths(job, StringUtil.join(",", paths.toArray(new String[]{})));
    }
}
