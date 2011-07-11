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
package com.mozilla.mahout.clustering.display.lda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.common.IntPairWritable;

import com.mozilla.hadoop.fs.Dictionary;
import com.mozilla.hadoop.fs.SequenceFileDirectoryReader;
import com.mozilla.util.Pair;

public class DisplayLDATopics {

    private static final Logger LOG = Logger.getLogger(DisplayLDATopics.class);

    // Adds the word if the queue is below capacity, or the score is high enough
    private static void enqueue(Queue<Pair<Double,String>> q, String word, double score, int numWordsToPrint) {
        if (q.size() >= numWordsToPrint && score > q.peek().getFirst()) {
            q.poll();
        }
        if (q.size() < numWordsToPrint) {
            q.add(new Pair<Double,String>(score, word));
        }
    }

    public static void printTopics(Map<Integer,PriorityQueue<Pair<Double,String>>> topicQueues) {
        System.out.println("LDA numTopics=" + topicQueues.size());
        for (Map.Entry<Integer, PriorityQueue<Pair<Double,String>>> entry : topicQueues.entrySet()) {
            System.out.println();
            
            int topic = entry.getKey();
            System.out.println("===== Topic " + topic + " =====");
            List<Pair<Double,String>> topKWords = new ArrayList<Pair<Double,String>>(entry.getValue());
            Collections.sort(topKWords, Collections.reverseOrder());
            int rank = 1;
            for (Pair<Double,String> p : topKWords) {
                String feature = p.getSecond();
                double score = p.getFirst();
                System.out.println(rank++ + " - " + feature + " [p(" + feature + "|topic_" + topic +") = " + score + "]");
            }
            
            System.out.println();
        }
    }
    
    public static Map<Integer,PriorityQueue<Pair<Double,String>>> getTopWordsByTopics(String stateDirPath, Map<Integer,String> featureIndex, int numWordsToPrint) {
        Map<Integer,Double> expSums = new HashMap<Integer, Double>();
        Map<Integer,PriorityQueue<Pair<Double,String>>> queues = new HashMap<Integer,PriorityQueue<Pair<Double,String>>>();
        SequenceFileDirectoryReader reader = null;
        try {
            IntPairWritable k = new IntPairWritable();
            DoubleWritable v = new DoubleWritable();
            reader = new SequenceFileDirectoryReader(new Path(stateDirPath));
            while (reader.next(k, v)) {
                int topic = k.getFirst();
                int featureId = k.getSecond();
                if (featureId >= 0 && topic >= 0) {
                    double score = v.get();
                    Double curSum = expSums.get(topic);
                    if (curSum == null) {
                        curSum = 0.0;
                    }
                    expSums.put(topic, curSum + Math.exp(score));
                    String feature = featureIndex.get(featureId);
                    
                    PriorityQueue<Pair<Double,String>> q = queues.get(topic);
                    if (q == null) {
                        q = new PriorityQueue<Pair<Double,String>>(numWordsToPrint);
                    }
                    enqueue(q, feature, score, numWordsToPrint);
                    queues.put(topic, q);
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading LDA state dir", e);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        
        for (Map.Entry<Integer, PriorityQueue<Pair<Double,String>>> entry : queues.entrySet()) {
            int topic = entry.getKey();
            for (Pair<Double,String> p : entry.getValue()) {
                double score = p.getFirst();
                p.setFirst(Math.exp(score) / expSums.get(topic));
            }
        }
        
        return queues;
    }

    public static void writeOriginalText(List<Pair<Double,String>> topKWords, PriorityQueue<Pair<Double,String>> docIdScores, String originalDataPath, BufferedWriter writer) {
        // Add just the docIds to a set for faster checks
        Map<String,Double> docIds = new HashMap<String,Double>();
        for (Pair<Double,String> p : docIdScores) {
            docIds.put(p.getSecond(), p.getFirst());
        }
        
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
                
                if (docIds.containsKey(splits[0])) {
                    String text = splits[7];
                    for (Pair<Double,String> p : topKWords) {
                        text = Pattern.compile(p.getSecond(), Pattern.CASE_INSENSITIVE).matcher(text).replaceAll("<strong>" + p.getSecond() + "</strong>");
                    }
                    writer.write("&nbsp;&nbsp;<span>" + docIds.get(splits[0]) + " - " + text + "</span><br/>");
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
    
    public static void writeOutputByTopic(Map<Integer,PriorityQueue<Pair<Double,String>>> topWords,
                                          Map<Integer, PriorityQueue<Pair<Double,String>>> topicDocIdMap, 
                                          String originalDataPath, String outputPath) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));
            for (Map.Entry<Integer, PriorityQueue<Pair<Double,String>>> entry : topicDocIdMap.entrySet()) {
                int topic = entry.getKey();
                writer.write("<h3>===== Topic " + topic + " =====</h3>");
                writer.newLine();
                
                List<Pair<Double,String>> topKWords = new ArrayList<Pair<Double,String>>(topWords.get(topic));
                Collections.sort(topKWords, Collections.reverseOrder());

                for (Pair<Double,String> p : topKWords) {
                    String feature = p.getSecond();
                    double score = p.getFirst();
                    writer.write("<span><strong>" + feature + "</strong> [p(" + feature + "|topic_" + topic +") = " + score + "]</span><br/>");
                    writer.newLine();
                }
                
                DisplayLDATopics.writeOriginalText(topKWords, entry.getValue(), originalDataPath, writer);
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
        if (args.length != 6) {
            System.out.println("Usage: DisplayLDATopics <stateDir> <docTopics> <featureIndex> <originalDataPath> <numWords> <outputPath>");
            System.exit(1);
        }
        
        String stateDirPath = args[0];
        String docTopicsPath = args[1];
        String featureIndexPath = args[2];
        String origDataPath = args[3];
        int numWordsPerTopic = Integer.parseInt(args[4]);
        String outputPath = args[5];
        
        Map<Integer,String> featureIndex = Dictionary.loadInvertedFeatureIndex(new Path(featureIndexPath));
        Map<Integer,PriorityQueue<Pair<Double,String>>> topWords = DisplayLDATopics.getTopWordsByTopics(stateDirPath, featureIndex, numWordsPerTopic);
        Map<Integer, PriorityQueue<Pair<Double,String>>> topicDocIdMap = OriginalText.getDocIds(new Path(docTopicsPath), numWordsPerTopic*2);
        DisplayLDATopics.writeOutputByTopic(topWords, topicDocIdMap, origDataPath, outputPath);
    }

}
