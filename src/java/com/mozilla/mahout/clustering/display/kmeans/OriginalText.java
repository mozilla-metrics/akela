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
package com.mozilla.mahout.clustering.display.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

import com.mozilla.hadoop.fs.SequenceFileDirectoryReader;

public class OriginalText {

    private static final Logger LOG = Logger.getLogger(OriginalText.class);
    
    private final Path clusteredPointsPath;
    
    public OriginalText(Path clusteredPointsPath) throws IOException {
        this.clusteredPointsPath = clusteredPointsPath;
    }
    
    public Map<Integer,Set<String>> getDocIds(double sampleRate) {
        Random rand = new Random();
        Map<Integer,Set<String>> docIdMap = new HashMap<Integer,Set<String>>();
        SequenceFileDirectoryReader pointsReader = null;
        try {
            IntWritable k = new IntWritable();
            WeightedVectorWritable wvw = new WeightedVectorWritable();
            pointsReader = new SequenceFileDirectoryReader(clusteredPointsPath);
            while (pointsReader.next(k, wvw)) {
                int clusterId = k.get();                
                Vector v = wvw.getVector();
                if (v instanceof NamedVector) {
                    if (rand.nextDouble() < sampleRate) {
                        NamedVector nv = (NamedVector)v;
                        nv.getName();
                        Set<String> curDocIds = docIdMap.get(clusterId);
                        if (curDocIds == null) {
                            curDocIds = new HashSet<String>();
                        }
                        curDocIds.add(nv.getName());
                        docIdMap.put(clusterId, curDocIds);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("IOException caught while reading clustered points", e);
        } finally {
            if (pointsReader != null) {
                pointsReader.close();
            }
        }
        
        return docIdMap;
    }
    
    private void writeOriginalText(Set<String> docIds, String originalDataPath, BufferedWriter writer) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(originalDataPath), "UTF-8"));
            String line = null;
            Pattern tabPattern = Pattern.compile("\t");
            while ((line = reader.readLine()) != null) {
                String[] splits = tabPattern.split(line);
                if (splits.length != 8) {
                    continue;
                }
                if (docIds.contains(splits[0])) {
                    writer.write("\t" + splits[0] + " - " + splits[7]);
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading original text file", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.error("Error closing original text file", e);
                }
            }
        }
    }
    
    public void writeOriginalTextByCluster(Map<Integer,Set<String>> docIdMap, String originalDataPath, String outputPath) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));
            for (Map.Entry<Integer, Set<String>> entry : docIdMap.entrySet()) {
                int clusterId = entry.getKey();
                writer.write("Cluster ID: " + clusterId);
                writer.newLine();
                writeOriginalText(entry.getValue(), originalDataPath, writer);
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("UTF-8 is unsupported?", e);
        } catch (FileNotFoundException e) {
            LOG.error("Could not create writer", e);
        } catch (IOException e) {
            LOG.error("IOException while writing");
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOG.error("Error closing writer", e);
                }
            }
        }
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: OriginalText <clusterPoints> <originalDataPath> <outputPath>");
        }
        OriginalText ot = new OriginalText(new Path(args[0]));
        Map<Integer,Set<String>> docIdMap = ot.getDocIds(0.1);
        ot.writeOriginalTextByCluster(docIdMap, args[1], args[2]);
    }
}
