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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class ConvertDateFormatTest {
    
    private ConvertDateFormat convDateFormat = new ConvertDateFormat("yyyyMMdd", "yyyy-MM-dd");
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        String outputDateStr = convDateFormat.exec(null);
        assertNull(outputDateStr);
    }

    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        String outputDateStr = convDateFormat.exec(input);
        assertNull(outputDateStr);
    }

    @Test
    public void testExec3() throws IOException {
        Tuple input = tupleFactory.newTuple();
        
        Calendar cal = Calendar.getInstance();
        cal.set(2011, Calendar.JANUARY, 23);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String inputDateStr = sdf.format(cal.getTime());
        
        input.append(inputDateStr);
        
        String outputDateStr = convDateFormat.exec(input);
        assertNotNull(outputDateStr);
        assertEquals(outputDateStr, "2011-01-23");
    }
    
}
