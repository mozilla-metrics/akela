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
 *
 *
 * EX:
 * STORE info_channel INTO '$output_prefix' USING com.mozilla.pig.storage.SeqFileMultiStorage( '$output_prefix','2','org.apache.hadoop.io.Text','org.apache.pig.data.Tuple');
 */



package com.mozilla.pig.storage;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.BackendException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import static org.apache.pig.data.DataType.*;

public class SeqFileMultiStorage extends StoreFunc {
    private Path outputPath; // User specified output Path
    private int splitFieldIndex = -1; // Index of the key field
    private Compression comp; // Compression type of output data.
    private final Class keyClass;
    private final Class valueClass;
    
    @SuppressWarnings("rawtypes")
    protected RecordWriter writer;
    
    enum Compression {
        none, bz2, bz, gz;
    };



    public SeqFileMultiStorage(String parentPathStr,String splitFieldIndex,
                               String keyClass,String valueClass)
        throws ClassNotFoundException  {
        
        this.outputPath = new Path(parentPathStr);
        this.splitFieldIndex = Integer.parseInt(splitFieldIndex);
        this.keyClass = Class.forName(keyClass);
        this.valueClass = Class.forName(valueClass);
    }


    @SuppressWarnings("unchecked")
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new MultiStorageSequenceOutputFormat();
    }

    @Override
    public void setStoreLocation(String location, Job job)
        throws IOException {
        job.setOutputKeyClass(this.keyClass);
        job.setOutputKeyClass(this.keyClass);
        Configuration conf = job.getConfiguration();
        if ("true".equals(conf.get("output.compression.enabled"))) {
            FileOutputFormat.setCompressOutput(job, true);
            String codec = conf.get("output.compression.codec");
            FileOutputFormat.setOutputCompressorClass(job,
                                                      PigContext.resolveClassName(codec).asSubclass(CompressionCodec.class));
        }
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }
    
    @Override
    public void putNext(Tuple tuple) throws IOException {
        if (tuple.size() <= splitFieldIndex) {
            throw new IOException("split field index:" + this.splitFieldIndex
                                  + " >= tuple size:" + tuple.size());
        }
        Object field = null;
        try {
            field = tuple.get(splitFieldIndex);
        } catch (ExecException exec) {
            throw new IOException(exec);
        }
        try {
            writer.write(field, tuple);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public static  class MultiStorageSequenceOutputFormat extends
                                                       SequenceFileOutputFormat<String,Tuple> {
        
      @Override
      public RecordWriter<String,Tuple> 
            getRecordWriter(TaskAttemptContext context
                            ) throws IOException, InterruptedException {
            final TaskAttemptContext ctx = context;

            return new RecordWriter<String, Tuple>() {

                private Map<String, SequenceFile.Writer> storeMap = 
                    new HashMap<String, SequenceFile.Writer>();

                @Override
                public void write(String key, Tuple val) throws IOException {
                    getStore(key).append(inferWritable(val.get(0)), inferWritable(
                                                                                    val.get(1)));
                }

                 @Override
                 public void close(TaskAttemptContext context) throws IOException { 
                    for (SequenceFile.Writer out : storeMap.values()) {
                        out.close();
                    }
                }
      
                private SequenceFile.Writer getStore(String fieldValue) throws IOException {
                    SequenceFile.Writer writer = storeMap.get(fieldValue);
                    if (writer == null) {
                        writer = createSequenceWriter(fieldValue,ctx.getOutputKeyClass(),ctx.getOutputValueClass());
                        storeMap.put(fieldValue, writer);
                    }
                    return writer;
                }
          
                private SequenceFile.Writer createSequenceWriter(String fieldValue,Class<?> keyClass,
                                                                 Class<?> valueClass) throws IOException {
                    Configuration conf = ctx.getConfiguration();
                    TaskID taskId = ctx.getTaskAttemptID().getTaskID();
          
                    // Check whether compression is enabled, if so get the extension and add them to the path
                    boolean isCompressed = getCompressOutput(ctx);
                    CompressionCodec codec = null;
                    String extension = "";
                    CompressionType compressionType = CompressionType.NONE;
                    if (isCompressed) {
                        Class<? extends CompressionCodec> codecClass = 
                            getOutputCompressorClass(ctx, GzipCodec.class);
                       compressionType = getOutputCompressionType(ctx);
                        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
                        extension = codec.getDefaultExtension();
                    }
                    String taskType = (conf.getBoolean("mapred.task.is.map", true)) ? "m" : "r";
                    int partition = conf.getInt("mapred.task.partition", -1);
                    NumberFormat nf = NumberFormat.getInstance();
                    nf.setGroupingUsed(false);
                    nf.setMinimumIntegerDigits(4);
                    Path path = new Path(fieldValue,  "part-" + taskType + "-" + nf.format(partition));
                    Path workOutputPath = ((FileOutputCommitter)getOutputCommitter(ctx)).getWorkPath();
                    Path file = new Path(workOutputPath, path);
                    FileSystem fs = file.getFileSystem(conf);                
                    FSDataOutputStream fileOut = fs.create(file, false);
                    return SequenceFile.createWriter(fs, conf, file,
                                                     keyClass,valueClass,compressionType,
                                                     codec,ctx);
                }
          
            };
        }

      protected Object inferWritable(Object o) throws BackendException {
          switch (DataType.findType(o)) {
          case BYTEARRAY: {
              return new BytesWritable(((DataByteArray) o).get());
          }
          case CHARARRAY: {
              return new Text(o.toString());
          }
          case INTEGER: {
              return new IntWritable((Integer) o);
          }
          case LONG: {
              return new LongWritable((Long) o);
          }
          case FLOAT: {
              return new FloatWritable((Float) o);
          }
          case DOUBLE: {
              return new DoubleWritable((Double) o);
          }
          case BOOLEAN: {
              return new BooleanWritable((Boolean) o);
          }
          case BYTE: {
              return new ByteWritable((Byte) o);
          }
          }
          throw new BackendException("Unable to translate " + o.getClass() +
                                     " to a Writable datatype");
      }
  
    }


}


    
