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
package com.mozilla.hadoop.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CSVImport implements Tool {

    private static final Logger LOG = Logger.getLogger(CSVImport.class);
    
    private static final String NAME = "CSVImport";
    
    private Configuration conf;
    
    public static class CSVImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

        public enum ReportStats { UNEXPECTED_LENGTH };
      
        private Pattern commaPattern;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            commaPattern = Pattern.compile(",");
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {       
            String[] splits = commaPattern.split(value.toString());
            if (splits.length != 4) {
                context.getCounter(ReportStats.UNEXPECTED_LENGTH);
                return;
            }
            
            byte[] rowkey = Bytes.toBytes(splits[0]);
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes(splits[1]), Bytes.toBytes(splits[2]), Bytes.toBytes(splits[3]));
            context.write(new ImmutableBytesWritable(rowkey), put);
        }
        
    }    
    /**
     * @param args
     * @return
     * @throws IOException
     * @throws ParseException 
     */
    public Job initJob(String[] args) throws IOException, ParseException {

        Path inputPath = new Path(args[0]);
        String table = args[1];
        
        Job job = new Job(getConf());
        job.setJobName(NAME);
        job.setJarByClass(CSVImport.class);
        job.setMapperClass(CSVImportMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        
        FileInputFormat.addInputPath(job, inputPath);
        
        return job;
    }

    /**
     * @return
     */
    private static int printUsage() {
        System.out.println("Usage: " + NAME + " [generic-options] <input-path> <output-table>");
        System.out.println();
        GenericOptionsParser.printGenericCommandUsage(System.out);
        
        return -1;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            return printUsage();
        }
        
        Job job = initJob(args);
        
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    public Configuration getConf() {
        return this.conf;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     */
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CSVImport(), args);
        System.exit(res);
    }

}
