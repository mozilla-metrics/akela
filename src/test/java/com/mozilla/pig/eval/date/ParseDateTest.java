/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class ParseDateTest {

    private static final String TIME_FORMAT = "yyyyMMdd HH:mm:ss:S";
    
    private ParseDate parseDate = new ParseDate(TIME_FORMAT);
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        Long millis = parseDate.exec(null);
        assertNull(millis);
    }

    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        Long millis = parseDate.exec(input);
        assertNull(millis);
    }

    @Test
    public void testExec3() throws IOException {
        Tuple input = tupleFactory.newTuple();
        
        Calendar cal = Calendar.getInstance();
        long inputTimeMillis = cal.getTimeInMillis();
        SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
 
        input.append(sdf.format(cal.getTime()));
        
        Long outputTimeMillis = parseDate.exec(input);
        assertNotNull(outputTimeMillis);
        assertEquals(inputTimeMillis, (long)outputTimeMillis);
    }
    
}
