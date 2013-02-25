/*
 * Copyright 2013 Mozilla Foundation
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
package com.mozilla.pig.eval.json;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class JsonTupleMapTest {

    private JsonTupleMap jsonMap = new JsonTupleMap();
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExecNestedTuple() throws IOException {
        Tuple input = tupleFactory.newTuple();
        input.append("{\"stacks\":[[[4,3],[2,1]], [[1,2],[3,4]]]}");
        Map<String,Object> myMap = jsonMap.exec(input);
        Tuple stacks = (Tuple)myMap.get("stacks");
        
        System.out.println(stacks);
        
        Tuple reference = getTestTuple();
        
        assertEquals(reference.toString(), stacks.toString());
        
        assertEquals(reference.size(), stacks.size());
        for (int i = 0; i < reference.size(); i++) {
            Tuple r = (Tuple)reference.get(i);
            Tuple s = (Tuple)stacks.get(i);
            assertEquals(r.size(), s.size());
            for (int j = 0; j < r.size(); j++) {
                System.out.println("Checking if " + r.get(j) + " == " + s.get(j));
                assertEquals(r.get(j), s.get(j));
            }
        }
    }

    // Construct a tuple that represents this json:
    //   {"stacks":[[[4,3],[2,1]], [[1,2],[3,4]]]}
    public Tuple getTestTuple() {
        TupleFactory tupleFactory = TupleFactory.getInstance();
        Tuple tAll = tupleFactory.newTuple();
        Tuple t1 = tupleFactory.newTuple();
        Tuple t1a = tupleFactory.newTuple();
        t1a.append(4);
        t1a.append(3);
        Tuple t1b = tupleFactory.newTuple();
        t1b.append(2);
        t1b.append(1);
        t1.append(t1a);
        t1.append(t1b);
        Tuple t2 = tupleFactory.newTuple();
        Tuple t2a = tupleFactory.newTuple();
        t2a.append(1);
        t2a.append(2);
        Tuple t2b = tupleFactory.newTuple();
        t2b.append(3);
        t2b.append(4);
        t2.append(t2a);
        t2.append(t2b);
        tAll.append(t1);
        tAll.append(t2);
        
        return tAll;
    }
}
