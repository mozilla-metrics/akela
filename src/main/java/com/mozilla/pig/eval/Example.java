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
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.Algebraic;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
 * The Example function takes a bag of results and returns a single element
 * of that bag. It is intended for use when grouping a set of results and
 * you want a single example item in the final result set.
 */
public final class Example extends EvalFunc<Tuple> implements Algebraic
{
    static private Tuple realexec(Tuple input) throws IOException
    {
        DataBag bag = (DataBag) input.get(0);
        Iterator<Tuple> it = bag.iterator();
        while (it.hasNext()) {
            Tuple t = it.next();
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    @Override
    public Tuple exec(Tuple input) throws IOException
    {
        return realexec(input);
    }

    static public class Sub extends EvalFunc<Tuple>
    {
        @Override
        public Tuple exec(Tuple input) throws IOException
        {
            return realexec(input);
        }
    }

    @Override
    public Schema outputSchema(Schema input)
    {
        try {
            if (input.size() != 1) {
                return null;
            }

            Schema.FieldSchema fs = input.getField(0);
            if (fs.type != DataType.BAG) {
                return null;
            }

            return fs.schema;
        }
        catch (Exception e) {
            this.log.error("Caught exception in " + this.getClass().getSimpleName() + ".outputSchema", e);
            return null;
        }
    }

    // We can use the same class in all cases since the input schema is the
    // same.
    public String getInitial() { return Sub.class.getName(); }
    public String getIntermed() { return Sub.class.getName(); }
    public String getFinal() { return Sub.class.getName(); }
}