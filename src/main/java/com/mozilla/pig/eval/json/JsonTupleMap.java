/**
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.Tuple;

public class JsonTupleMap extends JsonMap {
    
    /**
     * Converts List objects to Tuple to keep Pig happy
     * 
     * @param l
     * @return
     */
    @SuppressWarnings("unchecked")
    private Tuple makeSafeList(List<Object> l) {
        Tuple safeValues = tupleFactory.newTuple();
        for (Object o : l) {
            safeValues.append(makeSafeObj(o));
        }
        return safeValues;
    }

    @SuppressWarnings("unchecked")
    private Object makeSafeObj(Object o) {
        if (o instanceof List) {
            return makeSafeList((List<Object>) o);
        } else if (o instanceof Map) {
            return makeSafe((Map<String, Object>) o);
        } else {
            return o;
        }
    }

    /**
     * Convert map and its values to types that Pig can handle
     * 
     * @param m
     * @return
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> makeSafe(Map<String, Object> m) {
        Map<String, Object> safeValues = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : m.entrySet()) {
            safeValues.put(entry.getKey(), makeSafeObj(entry.getValue()));
        }
        return safeValues;
    }
}
