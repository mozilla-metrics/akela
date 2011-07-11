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
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.mozilla.hadoop.fs.Dictionary;

public class NGramTokenize extends EvalFunc<DataBag> {

    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private static final String NOFIELD = "";
    private Analyzer analyzer;
    private Set<String> stopwords;
    
    private void loadDictionary(String dictionaryPath) throws IOException {
        if (dictionaryPath != null) {
            stopwords = Dictionary.loadDictionary(new Path(dictionaryPath));
            log.info("Loaded dictionary with size: " + stopwords.size());
        }
    }
    
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        if (analyzer == null) {
            String langCode = "en";
            if (input.size() > 1) {
                loadDictionary((String)input.get(1));
            }
            boolean stem = false;
            if (input.size() > 2) {
                stem = Boolean.parseBoolean((String)input.get(2));
            }
            boolean outputUnigrams = false;
            if (input.size() > 3) {
                outputUnigrams = Boolean.parseBoolean((String)input.get(3));
            }
            int maxNGram = 3;
            if (input.size() > 4) {
                maxNGram = Integer.parseInt((String)input.get(4));
            }
            if (input.size() > 5) {
                langCode = (String)input.get(5);
            }
            
            if (stopwords != null && stopwords.size() != 0) {
                analyzer = new com.mozilla.lucene.analysis.en.NGramEnglishAnalyzer(Version.LUCENE_31, stopwords, stem, outputUnigrams, maxNGram);
            } else {
                analyzer = new com.mozilla.lucene.analysis.en.NGramEnglishAnalyzer(Version.LUCENE_31, StandardAnalyzer.STOP_WORDS_SET, stem, outputUnigrams, maxNGram);
            }
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