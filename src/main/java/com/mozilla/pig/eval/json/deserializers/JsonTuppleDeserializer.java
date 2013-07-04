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
package com.mozilla.pig.eval.json.deserializers;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;

final public class JsonTuppleDeserializer extends RegularJsonDeserializer {

    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    protected Object deserializeArray(JsonParser jp, DeserializationContext ctx) throws IOException, JsonParseException, JsonProcessingException {
        Tuple tupple = tupleFactory.newTuple();

        while (jp.nextToken() != JsonToken.END_ARRAY) {
            Object value = deserializeJson(jp, ctx);
            if (value instanceof Tuple) {
                tupple.append((Tuple) value);
            } else {
                tupple.append(deserializeJson(jp, ctx));
            }
        }

        return tupple;
    }
}
