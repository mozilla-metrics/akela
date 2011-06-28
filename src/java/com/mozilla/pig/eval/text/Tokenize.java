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
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Tokenize extends EvalFunc<DataBag> {

    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private static final String NOFIELD = "";
    private final Analyzer analyzer;
    
    public Tokenize() {
        analyzer = new org.apache.lucene.analysis.en.EnglishAnalyzer(Version.LUCENE_31);
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        DataBag output = bagFactory.newDefaultBag();
        TokenStream stream = analyzer.tokenStream(NOFIELD, new StringReader((String) input.get(0)));
        CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            if (termAttr.length() > 0) {
                Tuple t = tupleFactory.newTuple(termAttr.toString());
                output.add(t);
                termAttr.setEmpty();
            }
        }

        return output;
    }

}
