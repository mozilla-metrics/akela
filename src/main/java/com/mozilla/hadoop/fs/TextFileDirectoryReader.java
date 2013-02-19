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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TextFileDirectoryReader  {
    
    private static final Logger LOG = Logger.getLogger(TextFileDirectoryReader.class);
    
    private Configuration conf = new Configuration();
    private FileSystem fs;
    private List<Path> paths;
    private Iterator<Path> pathIter;
    private Path curPath;
    private BufferedReader curReader;
    
    public TextFileDirectoryReader(Path inputPath) throws IOException {
        fs = FileSystem.get(inputPath.toUri(), conf);
        paths = new ArrayList<Path>();
        for(FileStatus status : fs.listStatus(inputPath)) {
            Path p = status.getPath();
            if (!status.isDir() && !p.getName().startsWith("_")) {
                paths.add(p);
            }
        }
        
        pathIter = paths.iterator();
    }
    
    private boolean nextReader() throws IOException {        
        if (curReader != null) {
            curReader.close();
        }
        
        if (!pathIter.hasNext()) {
            return false;
        }
        
        curPath = pathIter.next();
        curReader = new BufferedReader(new InputStreamReader(fs.open(curPath)));
        
        return true;
    }
    
    public void close() {
        if (curReader != null) {
            try {
                curReader.close();
            } catch (IOException e) {
                LOG.error("Error closing reader", e);
            }
        }
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                LOG.error("Error closing filesystem", e);
            }
        }
    }
    
    public String next() throws IOException {
        if (curReader == null) {
            if (!nextReader()) {
                return null;
            }
        }

        String line = curReader.readLine();
        if (line == null) {
            boolean success = nextReader();
            if (success) {
                line = curReader.readLine();
            }
        }
        
        return line;
    }
    
    public static void main(String[] args) {
        String inputPath = args[0];
        TextFileDirectoryReader tfdr = null;
        try {
            tfdr = new TextFileDirectoryReader(new Path(inputPath));
            String line = null;
            while ((line = tfdr.next()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            LOG.error("IO error while reading directory", e);
        } finally {
            if (tfdr != null) {
                tfdr.close();
            }
        }
    }
}