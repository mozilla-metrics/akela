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

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Bucket extends EvalFunc<Integer> {

    private int[] buckets;
    
    public Bucket(String... inputs) {
        if (inputs.length == 0) {
            throw new IllegalArgumentException("You must specify at least one bucket");
        }
        
        this.buckets = new int[inputs.length];
        int i = 0;
        for (String bucket : inputs) {
            buckets[i] = Integer.parseInt(bucket);
            i++;
        }
    }
    
    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        
        int x = ((Number)input.get(0)).intValue();
        int ret = buckets[0];
        for (int i=1; i < buckets.length; i++) {
            if (x > buckets[i-1]) {
                ret = buckets[i];
            } else {
                break;
            }
        }

        return ret;
    }

}
