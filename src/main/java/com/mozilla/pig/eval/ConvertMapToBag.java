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
package com.mozilla.pig.eval;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ConvertMapToBag extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        Map<Object,Object> m = (Map<Object,Object>)input.get(0);
        
        DataBag output = bagFactory.newDefaultBag();
        if (m != null) {
            for (Map.Entry<Object, Object> entry : m.entrySet()) {
                Tuple t = tupleFactory.newTuple(2);
                t.set(0, entry.getKey());
                t.set(1, entry.getValue());
                output.add(t);
            }
        }
        
        return output;
    }

}
