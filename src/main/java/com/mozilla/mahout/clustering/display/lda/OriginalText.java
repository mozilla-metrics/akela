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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import com.mozilla.hadoop.fs.SequenceFileDirectoryReader;
import com.mozilla.util.Pair;

public class OriginalText {

    private static final Logger LOG = Logger.getLogger(OriginalText.class);

    // Adds the word if the queue is below capacity, or the score is high enough
    private static void enqueue(Queue<Pair<Double,String>> q, String word, double score, int numDocs) {
        if (q.size() >= numDocs && score > q.peek().getFirst()) {
            q.poll();
        }
        if (q.size() < numDocs) {
            q.add(new Pair<Double,String>(score, word));
        }
    }
    
    public static Map<Integer, PriorityQueue<Pair<Double,String>>> getDocIds(Path docTopicsPath, int numDocs) {
        Map<Integer, PriorityQueue<Pair<Double,String>>> docIdMap = new HashMap<Integer, PriorityQueue<Pair<Double,String>>>();
        Map<Integer, Double> maxDocScores = new HashMap<Integer,Double>();
        SequenceFileDirectoryReader pointsReader = null;
        try {
            Text k = new Text();
            VectorWritable vw = new VectorWritable();
            pointsReader = new SequenceFileDirectoryReader(docTopicsPath);
            while (pointsReader.next(k, vw)) {
                String docId = k.toString();
                Vector normGamma = vw.get();
                Iterator<Element> iter = normGamma.iterateNonZero();
                double maxTopicScore = 0.0;
                int idx = 0;
                int topic = 0;
                while(iter.hasNext()) {
                    Element e = iter.next();
                    double score = e.get();
                    if (score > maxTopicScore) {
                        maxTopicScore = score;
                        topic = idx;
                    }
                    
                    idx++;  
                }
                
                PriorityQueue<Pair<Double,String>> docIdsForTopic = docIdMap.get(topic);
                if (docIdsForTopic == null) {
                    docIdsForTopic = new PriorityQueue<Pair<Double,String>>(numDocs);
                }
                
                Double maxDocScoreForTopic = maxDocScores.get(topic);
                if (maxDocScoreForTopic == null) {
                    maxDocScoreForTopic = 0.0;
                }
                if (maxTopicScore > maxDocScoreForTopic) {
                    maxDocScores.put(topic, maxTopicScore);
                }
                
                enqueue(docIdsForTopic, docId, maxTopicScore, numDocs);
                docIdMap.put(topic, docIdsForTopic);
            }
        } catch (IOException e) {
            LOG.error("IOException caught while reading clustered points", e);
        } finally {
            if (pointsReader != null) {
                pointsReader.close();
            }
        }

        for (Map.Entry<Integer, Double> entry : maxDocScores.entrySet()) {
            System.out.println("For topic: " + entry.getKey() + " max score: " + entry.getValue());
        }
        
        return docIdMap;
    }
    
    public static void writeOriginalText(PriorityQueue<Pair<Double,String>> docIdScores, String originalDataPath, BufferedWriter writer) {
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
                    writer.write("\t" + splits[0] + " - " + docIds.get(splits[0]) + " - " + splits[7]);
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

    public static void writeOriginalTextByTopic(Map<Integer, PriorityQueue<Pair<Double,String>>> topicDocIdMap, String originalDataPath, String outputPath) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));
            for (Map.Entry<Integer, PriorityQueue<Pair<Double,String>>> entry : topicDocIdMap.entrySet()) {
                int clusterId = entry.getKey();
                writer.write("===== Topic " + clusterId + " =====");
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
            System.out.println("Usage: OriginalText <docTopics> <originalDataPath> <outputPath>");
            System.exit(1);
        }
        
        Map<Integer, PriorityQueue<Pair<Double,String>>> topicDocIdMap = OriginalText.getDocIds(new Path(args[0]), 50);
        OriginalText.writeOriginalTextByTopic(topicDocIdMap, args[1], args[2]);
    }
}
