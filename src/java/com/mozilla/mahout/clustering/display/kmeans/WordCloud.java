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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.Vector.Element;
import org.mcavallo.opencloud.Cloud;
import org.mcavallo.opencloud.Cloud.Case;
import org.mcavallo.opencloud.formatters.HTMLFormatter;
import org.mcavallo.opencloud.Tag;

import com.mozilla.hadoop.fs.Dictionary;
import com.mozilla.hadoop.fs.SequenceFileDirectoryReader;

public class WordCloud {

    private static final Logger LOG = Logger.getLogger(WordCloud.class);
    
    private final Path clusteredPointsPath;
    private Map<Integer,String> invertedFeatureIndex;
    
    public WordCloud(Path clusteredPointsPath, Path dictionaryPath) throws IOException {
        this.clusteredPointsPath = clusteredPointsPath;
        this.invertedFeatureIndex = Dictionary.loadInvertedFeatureIndex(dictionaryPath);
    }
    
    public Map<Integer,Cloud> getClouds(Cloud template) {
        Map<Integer,Cloud> cloudMap = new HashMap<Integer,Cloud>();
        SequenceFileDirectoryReader pointsReader = null;
        try {
            IntWritable k = new IntWritable();
            WeightedVectorWritable wvw = new WeightedVectorWritable();
            pointsReader = new SequenceFileDirectoryReader(clusteredPointsPath);
            while (pointsReader.next(k, wvw)) {
                int clusterId = k.get();
                Cloud c = cloudMap.get(clusterId);
                if (c == null) {
                    c = new Cloud(template);
                }
                Iterator<Element> viter = wvw.getVector().iterateNonZero();
                while (viter.hasNext()) {
                    Element e = viter.next();
                    String feature = invertedFeatureIndex.get(e.index());
                    c.addTag(new Tag(feature, e.get()));
                }
                
                cloudMap.put(clusterId, c);
            }
        } catch (IOException e) {
            LOG.error("IOException caught while reading clustered points", e);
        } finally {
            if (pointsReader != null) {
                pointsReader.close();
            }
        }
        
        return cloudMap;
    }
    
    public void printCloudsHTML(Map<Integer,Cloud> cloudMap) {
        HTMLFormatter formatter = new HTMLFormatter();
        
        System.out.println("<h2>KMeans k=" + cloudMap.size() + "</h2>");
        for (Map.Entry<Integer, Cloud> entry : cloudMap.entrySet()) {
            Integer clusterId = entry.getKey();
            formatter.setHtmlTemplateTop("<h3>Cluster ID: &nbsp;" + clusterId + "</h3>");
            System.out.println(formatter.html(entry.getValue()));
        }
    }
    
    public void writeCloudsHTML(Map<Integer,Cloud> cloudMap, String outputPath) {
        HTMLFormatter formatter = new HTMLFormatter();
        
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));
            writer.write("<h2>KMeans k=" + cloudMap.size() + "</h2>\n");
            for (Map.Entry<Integer, Cloud> entry : cloudMap.entrySet()) {
                Integer clusterId = entry.getKey();
                formatter.setHtmlTemplateTop("<h3>Cluster ID: &nbsp;" + clusterId + "</h3>");
                writer.write(formatter.html(entry.getValue()));
                writer.newLine();
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("UTF-8 is unsupported?", e);
        } catch (FileNotFoundException e) {
            LOG.error("Could not create clouds HTML writer", e);
        } catch (IOException e) {
            LOG.error("IOException while writing clouds HTML");
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOG.error("Error closing clouds HTML writer", e);
                }
            }
        }
        
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: WordCloud <clusterPoints> <feature-index> <htmlOutputPath>");
        }
        WordCloud wc = new WordCloud(new Path(args[0]), new Path(args[1]));
        
        Cloud template = new Cloud();
        template.setMaxTagsToDisplay(50);
        template.setTagCase(Case.LOWER);
        template.setMinWeight(10.0);
        template.setMaxWeight(96.0);
        
        Map<Integer,Cloud> cloudMap = wc.getClouds(template);
        wc.writeCloudsHTML(cloudMap, args[2]);
    }
}
