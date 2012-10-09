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
package com.mozilla.util;

import static java.util.Calendar.DATE;
import static java.util.Calendar.JULY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.WEEK_OF_YEAR;
import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.Test;

public class DateUtilTest {

    @Test
    public void testTimeAtResolution() {
        Calendar cal = Calendar.getInstance();
        cal.set(2012, JULY, 13, 22, 30);

        long resTime = DateUtil.getTimeAtResolution(cal.getTimeInMillis(), DATE);

        Calendar expected = Calendar.getInstance();
        expected.set(2012, JULY, 13, 0, 0, 0);
        expected.set(MILLISECOND, 0);

        assertEquals(expected.getTimeInMillis(), resTime);
    }

    @Test
    public void testEndTimeAtResolution() {
        Calendar cal = Calendar.getInstance();
        cal.set(2012, JULY, 13, 22, 30);

        long resTime = DateUtil.getEndTimeAtResolution(cal.getTimeInMillis(), DATE);

        Calendar expected = Calendar.getInstance();
        expected.set(2012, JULY, 13, 23, 59, 59);
        expected.set(MILLISECOND, 999);

        assertEquals(expected.getTimeInMillis(), resTime);
    }

    @Test
    public void testTimeDelta() {
        Calendar start = Calendar.getInstance();
        start.set(2012, JULY, 13);
        Calendar end = Calendar.getInstance();
        end.set(2012, JULY, 5);

        long delta = DateUtil.getTimeDelta(start.getTimeInMillis(), end.getTimeInMillis(), WEEK_OF_YEAR);
        assertEquals(-1L, delta);

        end.set(2011, JULY, 5);
        delta = DateUtil.getTimeDelta(start.getTimeInMillis(), end.getTimeInMillis(), WEEK_OF_YEAR);
        assertEquals(-53L, delta);
    }

    @Test
    public void testIterateByDay() {
        Calendar start = Calendar.getInstance();
        start.set(2012, JULY, 1);
        Calendar end = Calendar.getInstance();
        end.set(2012, JULY, 5);

        final List<String> seenDates = new ArrayList<String>();
        DateUtil.iterateByDay(start.getTimeInMillis(), end.getTimeInMillis(), new DateIterator(){
           SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
           @Override
        public void see(long time) {
               seenDates.add(sdf.format(new Date(time)));
           }
        });

        String[] expectedDates = new String[]{
                "2012-07-01",
                "2012-07-02",
                "2012-07-03",
                "2012-07-04",
                "2012-07-05",
        };

        assertEquals(expectedDates.length, seenDates.size());
        for (int i = 0; i < expectedDates.length; i++) {
            assertEquals(expectedDates[i], seenDates.get(i));
        }
    }

    @Test
    public void testIterateByHour() {
        Calendar start = Calendar.getInstance();
        start.set(2012, JULY, 1, 0, 0);
        Calendar end = Calendar.getInstance();
        end.set(2012, JULY, 1, 5, 1);

        final List<String> seenDates = new ArrayList<String>();
        DateUtil.iterateByTime(start.getTimeInMillis(), end.getTimeInMillis(), Calendar.HOUR, 1, new DateIterator(){
           SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
           @Override
        public void see(long time) {
               seenDates.add(sdf.format(new Date(time)));
           }
        });

        String[] expectedDates = new String[]{
                "2012-07-01 00:00",
                "2012-07-01 01:00",
                "2012-07-01 02:00",
                "2012-07-01 03:00",
                "2012-07-01 04:00",
                "2012-07-01 05:00",
        };

        assertEquals(expectedDates.length, seenDates.size());
        for (int i = 0; i < expectedDates.length; i++) {
            assertEquals(expectedDates[i], seenDates.get(i));
        }
    }
}
