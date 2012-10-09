/**
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.mozilla.util.DateIterator;
import com.mozilla.util.DateUtil;
import com.mozilla.util.StringUtil;

/**
 * DateRangeLoader lets you load data from a series of locations corresponding to 
 * a set of dates.
 */
public class DateRangeLoader extends LoadFunc {

    private static final Log LOG = LogFactory.getLog(DateRangeLoader.class);
    private static final String sqlDateFormatSpec = "yyyy-MM-dd";
    private static final SimpleDateFormat sqlDateFormat = new SimpleDateFormat(sqlDateFormatSpec);
    private final SimpleDateFormat inputPathFormat;
    private final Calendar start;
    private final Calendar end;
    private LineRecordReader reader = null;
    
    public DateRangeLoader(String startDate, String endDate, String format) {
        inputPathFormat = new SimpleDateFormat(format);
        start = Calendar.getInstance();
        end = Calendar.getInstance();
        try {
            start.setTime(sqlDateFormat.parse(startDate));
            end.setTime(sqlDateFormat.parse(endDate));
        } catch (ParseException e) {
            // TODO Throw a RuntimeException?
        }
    }

    // Use the default sql-like date format
    public DateRangeLoader(String startDate, String endDate) {
        this(startDate, endDate, sqlDateFormatSpec);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
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
                LOG.info("Adding a location: '" + path + "'");
                paths.add(path);
            }
        });
        
        FileInputFormat.setInputPaths(job, StringUtil.join(",", paths.toArray(new String[]{})));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = (LineRecordReader) reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple t = null;
        while (reader.nextKeyValue()) {
            Text val = reader.getCurrentValue();
            if (val != null) {
                String line = val.toString();
                t = TupleFactory.getInstance().newTuple();
                t.append(line);
            }
        }

        return t;
    }
    
    
}
