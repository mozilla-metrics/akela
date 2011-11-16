/**
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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.pig.filter.map;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class RegexContainsKey extends FilterFunc {

    @SuppressWarnings("unchecked")
    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            return false;
        }
        
        Map<String,Object> map = (Map<String,Object>)input.get(0);
        String keyPatternStr = (String)input.get(1);
        Pattern p = Pattern.compile(keyPatternStr);
        boolean found = false;
        for (String k : map.keySet()) {
            if (p.matcher(k).find()) {
                found = true;
                break;
            }
        }
        return found;
    }

}

