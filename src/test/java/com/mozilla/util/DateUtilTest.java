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

import static org.junit.Assert.*;

import java.util.Calendar;
import static java.util.Calendar.*;
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
}
