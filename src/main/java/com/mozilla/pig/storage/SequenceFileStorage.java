package com.mozilla.pig.storage;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

public class SequenceFileStorage extends StoreFunc {

    @SuppressWarnings("rawtypes")
    protected RecordWriter writer = null;
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    public SequenceFileStorage() {
        super();
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new SequenceFileOutputFormat<Text, Text>();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple tuple) throws IOException {
        outputKey.set((String)tuple.get(0));
        outputValue.set((String)tuple.get(1));
        try {
            writer.write(outputKey, outputValue);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(location));
    }
    
}
