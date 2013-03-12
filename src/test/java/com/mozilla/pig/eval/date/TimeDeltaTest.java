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
package com.mozilla.pig.eval.date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TimeDeltaTest {

    private static final String TIME_FORMAT = "yyyyMMdd";
    
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException, ParseException {
        TimeDelta daysAgo = new TimeDelta("5", TIME_FORMAT);
        Long deltaDays = daysAgo.exec(null);
        assertNull(deltaDays);
    }

    @Test
    public void testExec2() throws IOException, ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, sdf.format(cal.getTime()));
        input.set(1, sdf.format(Calendar.getInstance().getTime()));
        
        TimeDelta daysAgo = new TimeDelta("5", TIME_FORMAT);
        Long deltaDays = daysAgo.exec(input);
        assertEquals(1, (long)deltaDays);
        
        cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -30);
        input.set(0, sdf.format(cal.getTime()));
        deltaDays = daysAgo.exec(input);
        assertEquals(30, (long)deltaDays);

        Date start = sdf.parse("20130101");
        Date end = sdf.parse("20130111");
        input.set(0, sdf.format(start));
        input.set(1, sdf.format(end));
        deltaDays = daysAgo.exec(input);
        assertEquals(10, (long)deltaDays);

        // Trigger the DST-change effect:
        // Note that DST began on 20130310 in many parts of
        // North America.
        start = sdf.parse("20130305");
        end = sdf.parse("20130315");
        input.set(0, sdf.format(start));
        input.set(1, sdf.format(end));
        deltaDays = daysAgo.exec(input);
        assertEquals(10, (long)deltaDays);
    }
    
}
