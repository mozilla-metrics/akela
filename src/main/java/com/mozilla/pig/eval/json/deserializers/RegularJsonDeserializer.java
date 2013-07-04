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
package com.mozilla.pig.eval.json.deserializers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;

public class RegularJsonDeserializer extends JsonDeserializer<Object> {

    protected Object deserializeArray(JsonParser jp, DeserializationContext ctx) throws IOException, JsonParseException, JsonProcessingException {
        ArrayList<Object> array = new ArrayList<Object>();
        while (jp.nextToken() != JsonToken.END_ARRAY) {
            array.add(deserializeJson(jp, ctx));
        }
        return array;
    }

    protected Object deserialzeMap(JsonParser jp, DeserializationContext ctx) throws IOException, JsonParseException, JsonProcessingException {
        HashMap<String, Object> map = new HashMap<String, Object>();
        while (jp.nextToken() != JsonToken.END_OBJECT) {
            map.put(jp.getCurrentName(), deserializeJson(jp, ctx));
        }
        return map;
    }

    protected Object deserializeJson(JsonParser jp, DeserializationContext ctx) throws IOException, JsonProcessingException {
        Object result = null;
        JsonToken token = jp.getCurrentToken();
        if (token == JsonToken.FIELD_NAME) {
            // ignore.
        } else if (token == JsonToken.VALUE_NUMBER_INT) {
            result = jp.getIntValue();
        } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
            result = jp.getNumberValue();
        } else if (token == JsonToken.VALUE_STRING) {
            result = jp.getText();
        } else if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
            result = jp.getBooleanValue();
        } else if (token == JsonToken.START_OBJECT) {
            result = deserialzeMap(jp, ctx);
        } else if (token == JsonToken.START_ARRAY) {
            result = deserializeArray(jp, ctx);
        } else if (token == JsonToken.VALUE_NULL) {
            result = null;
        } else {
            throw new JsonMappingException("No mapping for " + jp.getCurrentName() + "=" + token.name());
        }
        return result;
    }

    public Object deserialize(JsonParser jp, DeserializationContext ctx) throws IOException, JsonProcessingException {
        return deserializeJson(jp, ctx);
    }
}
