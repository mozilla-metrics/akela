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
package com.mozilla.pig.eval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class BucketTest {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testConstructor() throws IOException {
        boolean exception = false;
        try {
            Bucket bucket = new Bucket();
        } catch (IllegalArgumentException e) {
            exception = true;
        }
        
        assertTrue(exception);
    }
    
    @Test
    public void testExec2() throws IOException {
        Bucket bucket = new Bucket("1", "5", "8", "13");
        
        Tuple input = tupleFactory.newTuple(1);
        input.set(0, 0);
        assertEquals(1L, (int)bucket.exec(input));
        
        input.set(0, 2);
        assertEquals(5L, (int)bucket.exec(input));
        
        input.set(0, 5);
        assertEquals(5L, (int)bucket.exec(input));
        
        input.set(0, 16);
        assertEquals(13L, (int)bucket.exec(input));
    }
}
