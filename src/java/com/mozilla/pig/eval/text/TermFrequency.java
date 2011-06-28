/*
 * Copyright 2011 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.pig.eval.text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class TermFrequency extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    public TermFrequency() {
    }
    
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        DataBag db = (DataBag)input.get(0);
        Map<String,Integer> termFreq = new HashMap<String,Integer>();
        long docSize = db.size();
        for (Tuple t : db) {
            String word = (String)t.get(0);
            int curCount = 0;
            if (termFreq.containsKey(word)) {
                curCount = termFreq.get(word);
            }
            termFreq.put(word, ++curCount);
        }

        DataBag output = bagFactory.newDefaultBag();
        for (Map.Entry<String, Integer> entry: termFreq.entrySet()) {
            Tuple t = tupleFactory.newTuple(2);
            t.set(0, entry.getKey());
            t.set(1, (float)entry.getValue()/(float)docSize);
            output.add(t);
        }
        
        return output;
    }
}
