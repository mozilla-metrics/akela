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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple;

/**
 * RegExLoader is an abstract class used to parse logs based on a regular expression.
 * 
 * There is a single abstract method, getPattern which needs to return a Pattern. Each group will be returned as a
 * different DataAtom.
 * 
 * Look to org.apache.pig.piggybank.storage.apachelog.CommonLogLoader for example usage.
 * 
 * 2011-02-03 xstevens: I changed the looping structure in getNext() which also fixes an infinite loop that was present
 * in piggybank 0.7.
 */
public abstract class RegExLoader extends LoadFunc {

    private static final Log LOG = LogFactory.getLog(RegExLoader.class);

    private LineRecordReader reader = null;

    public abstract Pattern getPattern();

    @Override
    public Tuple getNext() throws IOException {
        Tuple t = null;
        boolean tryNext = true;
        while (tryNext && reader.nextKeyValue()) {
            Text val = reader.getCurrentValue();
            if (val != null) {
                String line = val.toString();
                if (line.length() > 0 && line.charAt(line.length() - 1) == '\r') {
                    line = line.substring(0, line.length() - 1);
                }
                Matcher m = getPattern().matcher(line);
                if (m.find()) {
                    tryNext = false;
                    t = TupleFactory.getInstance().newTuple();
                    for (int i = 1; i <= m.groupCount(); i++) {
                        t.append(new DataByteArray(m.group(i)));
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Failed to match line: " + val.toString());
                    }
                }
            }
        }

        return t;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = (LineRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

}
