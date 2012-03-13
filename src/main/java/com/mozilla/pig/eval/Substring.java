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
package com.mozilla.pig.eval;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;

/**
 * Basically the same as the builtin SUBSTRING except you can optinally leave off the third
 * argument and it will return beginIndex to the end of the string.
 */
public class Substring extends EvalFunc<String> {

    public static enum ERRORS { NULL_SOURCE, OUT_OF_BOUNDS };
    
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            warn("invalid number of arguments to Substring", PigWarning.UDF_WARNING_1);
            return null;
        }
        try {
            String source = (String)input.get(0);
            if (source == null) {
                warn("Source was null", ERRORS.NULL_SOURCE);
                return null;
            }
            Integer beginindex = (Integer)input.get(1);
            // third arg is optional
            if (input.size() == 3) {
                Integer endindex = (Integer)input.get(2);
                return source.substring(beginindex, Math.min(source.length(), endindex));
            } else {
                return source.substring(beginindex);
            }
        } catch (StringIndexOutOfBoundsException e) {
            warn(e.toString(), ERRORS.OUT_OF_BOUNDS);
            return null;
        }
    }

}
