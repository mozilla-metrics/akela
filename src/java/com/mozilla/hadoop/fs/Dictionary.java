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
package com.mozilla.hadoop.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class Dictionary {
    
    private static final Logger LOG = Logger.getLogger(SequenceFileDirectoryReader.class);

    public static Set<String> loadDictionary(Path dictionaryPath) throws IOException {
        Set<String> dictionary = null;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(dictionaryPath.toUri(), new Configuration());
            dictionary = loadDictionary(fs, dictionaryPath);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        
        return dictionary;
    }
    
    public static Set<String> loadDictionary(FileSystem fs, Path dictionaryPath) throws IOException {
        Set<String> dictionary = null;
        if (dictionaryPath != null) {
            dictionary = new HashSet<String>();
            for (FileStatus status : fs.listStatus(dictionaryPath)) {
                if (!status.isDir()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                        String word = null;
                        while ((word = reader.readLine()) != null) {
                            dictionary.add(word.trim());
                        }
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                    }
                }
            }
            
            LOG.info("Loaded dictionary with size: " + dictionary.size());
        }
        
        return dictionary;
    }
    
    public static Map<String,Integer> loadFeatureIndex(Path dictionaryPath) throws IOException {
        Map<String,Integer> featureIndex = null;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(dictionaryPath.toUri(), new Configuration());
            featureIndex = loadFeatureIndex(fs, dictionaryPath);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        
        return featureIndex;
    }
    
    public static Map<String,Integer> loadFeatureIndex(FileSystem fs, Path dictionaryPath) throws IOException {
        Map<String,Integer> featureIndex = null;
        if (dictionaryPath != null) {
            featureIndex = new HashMap<String,Integer>();
            int idx = 0;
            for (FileStatus status : fs.listStatus(dictionaryPath)) {
                if (!status.isDir()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                        String word = null;
                        while ((word = reader.readLine()) != null) {
                            featureIndex.put(word.trim(), idx++);
                        }
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                    }
                }
            }
            
            LOG.info("Loaded dictionary with size: " + featureIndex.size());
        }
        
        return featureIndex;
    }
    
    public static Map<Integer,String> loadInvertedFeatureIndex(Path dictionaryPath) throws IOException {
        Map<Integer,String> featureIndex = null;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(dictionaryPath.toUri(), new Configuration());
            featureIndex = loadInvertedFeatureIndex(fs, dictionaryPath);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        
        return featureIndex;
    }
    
    public static Map<Integer,String> loadInvertedFeatureIndex(FileSystem fs, Path dictionaryPath) throws IOException {
        Map<Integer,String> featureIndex = null;
        if (dictionaryPath != null) {
            featureIndex = new HashMap<Integer,String>();
            int idx = 0;
            for (FileStatus status : fs.listStatus(dictionaryPath)) {
                if (!status.isDir()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                        String word = null;
                        while ((word = reader.readLine()) != null) {
                            featureIndex.put(idx++, word.trim());
                        }
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                    }
                }
            }
            
            LOG.info("Loaded dictionary with size: " + featureIndex.size());
        }
        
        return featureIndex;
    }
    
}
