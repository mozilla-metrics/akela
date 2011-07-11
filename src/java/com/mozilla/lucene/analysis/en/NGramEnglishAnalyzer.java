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
package com.mozilla.lucene.analysis.en;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.KeywordMarkerFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.mozilla.hadoop.fs.Dictionary;

public class NGramEnglishAnalyzer extends StopwordAnalyzerBase {

    private final Set<?> stemExclusionSet;
    private boolean stem = false;
    private boolean outputUnigrams = true;
    private int maxNGram = ShingleAllStopFilter.DEFAULT_MAX_SHINGLE_SIZE;

    public NGramEnglishAnalyzer(Version version) {
        this(version, StandardAnalyzer.STOP_WORDS_SET, false);
    }

    public NGramEnglishAnalyzer(Version version, boolean stem) {
        this(version, StandardAnalyzer.STOP_WORDS_SET, stem);
    }
    
    public NGramEnglishAnalyzer(Version version, Set<?> stopwords, boolean stem) {
        this(version, stopwords, stem, true);
    }
    
    public NGramEnglishAnalyzer(Version version, Set<?> stopwords, boolean stem, boolean outputUnigrams) {
        this(version, stopwords, stem, outputUnigrams, ShingleAllStopFilter.DEFAULT_MAX_SHINGLE_SIZE, CharArraySet.EMPTY_SET);
    }

    public NGramEnglishAnalyzer(Version version, Set<?> stopwords, boolean stem, boolean outputUnigrams, int maxNGram) {
        this(version, stopwords, stem, outputUnigrams, maxNGram, CharArraySet.EMPTY_SET);
    }
    
    public NGramEnglishAnalyzer(Version matchVersion, Set<?> stopwords, boolean stem, boolean outputUnigrams, int maxNGram, Set<?> stemExclusionSet) {
        super(matchVersion, stopwords);
        this.stem = stem;
        this.outputUnigrams = outputUnigrams;
        this.maxNGram = maxNGram;
        this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionSet));
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        final Tokenizer source = new StandardTokenizer(matchVersion, reader);
        TokenStream result = new StandardFilter(matchVersion, source);       
        if (matchVersion.onOrAfter(Version.LUCENE_31)) {
            result = new EnglishPossessiveFilter(result);
        }
        result = new LowerCaseFilter(matchVersion, result);
        ShingleAllStopFilter sf = new ShingleAllStopFilter(result, maxNGram, stopwords);
        sf.setOutputUnigrams(outputUnigrams);
        if (!outputUnigrams) {
            sf.setOutputUnigramsIfNoShingles(false);
        }
        result = sf;
        
        if (stem) {
            if (!stemExclusionSet.isEmpty()) {
                result = new KeywordMarkerFilter(result, stemExclusionSet);
            }
            result = new PorterStemFilter(result);
        }
        
        return new TokenStreamComponents(source, result);
    }

    public static void main(String[] args) throws IOException {
        Set<String> stopwords = Dictionary.loadDictionary(new Path("file:///Users/xstevens/workspace/akela/stopwords-en.txt"));
        NGramEnglishAnalyzer analyzer = new com.mozilla.lucene.analysis.en.NGramEnglishAnalyzer(Version.LUCENE_31, stopwords, false, true);
        TokenStream stream = analyzer.tokenStream("", new StringReader("When I was growing up this was so much fun."));
        CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            if (termAttr.length() > 0) {
                System.out.println(termAttr.toString());
                termAttr.setEmpty();
            }
        }
    }
}
