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
package com.mozilla.pig.filter.tuple;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class ContainsTest {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        Tuple items = tupleFactory.newTuple();
        items.append(-1);
        items.append(0);
        items.append(1);
        
        input.set(0, items);
        input.set(1, 0);
        
        Contains c = new Contains();
        boolean found = c.exec(input);
        assertTrue(found);
    }
    
}
