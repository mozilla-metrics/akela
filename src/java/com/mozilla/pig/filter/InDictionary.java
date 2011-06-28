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
package com.mozilla.pig.filter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class InDictionary extends FilterFunc {

        private String dictionaryPath;
        private Set<String> dictionary;
        
        public InDictionary(String dictionaryPath) {
            this.dictionaryPath = dictionaryPath;
        }
        
        private void loadDictionary() throws IOException {
            if (dictionaryPath != null) {
                dictionary = new HashSet<String>();
                
                FileSystem hdfs = null;
                Path p = new Path(dictionaryPath);
                hdfs = FileSystem.get(p.toUri(), new Configuration());
                for (FileStatus status : hdfs.listStatus(p)) {
                    if (!status.isDir()) {
                        BufferedReader reader = null;
                        try {
                            reader = new BufferedReader(new InputStreamReader(hdfs.open(status.getPath())));
                            String line = null;
                            while ((line = reader.readLine()) != null) {
                                dictionary.add(line.trim());
                            }
                        } finally {
                            if (reader != null) {
                                reader.close();
                            }
                        }
                    }
                }
                
                log.info("Loaded dictionary with size: " + dictionary.size());
            }
        }
        
        @Override
        public Boolean exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0) {
                return null;
            }

            if (input.size() > 1) {
                dictionaryPath = (String)input.get(1);
            }
            
            if (dictionary == null) {
                loadDictionary();
            }
            
            return dictionary.contains((String)input.get(0));
        }
        
}
