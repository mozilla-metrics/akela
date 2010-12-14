/**
 * Copyright 2010 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.mozilla.hadoop;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Backup implements Tool {

	private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Backup.class);
	
	private static final String NAME = "Backup";
	
	private Configuration conf;
	
	public static class BackupMapper extends Mapper<LongWritable, Text, Text, Text> {

		private FileSystem inputFs;
		private FileSystem outputFs;
		private String outputRootPath;
		private Pattern filePattern;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				String backupInputPath = conf.get("backup.input.path");
				if (!backupInputPath.endsWith(Path.SEPARATOR)) {
					backupInputPath += Path.SEPARATOR;
				}
				filePattern = Pattern.compile(backupInputPath + "(.+)");
				inputFs = FileSystem.get(new Path(backupInputPath).toUri(), context.getConfiguration());
				
				outputRootPath = conf.get("backup.output.path");
				if (!outputRootPath.endsWith(Path.SEPARATOR)) {
					outputRootPath += Path.SEPARATOR;
				}
				outputFs = FileSystem.get(new Path(outputRootPath).toUri(), conf);
			} catch (IOException e) {
				throw new RuntimeException("Could not get FileSystem", e);
			}
			
			
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void cleanup(Context context) {
			if (inputFs != null) {
				try {
					inputFs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			System.out.println("Received: " + value.toString());
			
			DataInputStream dis = null;
			DataOutputStream dos = null;
			try {		
				String commonPath = null;
				Matcher m = filePattern.matcher(value.toString());
				if (m.find()) {
					if (m.groupCount() == 1) {
						commonPath = m.group(1);
					}
				} else {
					throw new RuntimeException("Regex match on common path failed");
				}
			
				Path fullOutputPath = new Path(outputRootPath + commonPath);
				outputFs.mkdirs(fullOutputPath.getParent());

				dis = inputFs.open(new Path(value.toString()));
				
//				if (commonPath.endsWith(".gz")) {
//					dos = new GZIPOutputStream(new FileOutputStream(outputFile));
//				} else {
					dos = outputFs.create(fullOutputPath, true);
//				}
				byte[] buffer = new byte[32768];
				int bytesRead = 0;
				while ((bytesRead = dis.read(buffer)) > 0) {
					dos.write(buffer, 0, bytesRead);
				}
			} finally {
				if (dis != null) {
					dis.close();
				}
				if (dos != null) {
					dos.close();
				}
			}
			
			context.write(new Text(key.toString()), new Text(""));
		}
		
	}	

	public List<Path> getPaths(FileSystem fs, Path inputPath) throws IOException {
		List<Path> retPaths = new ArrayList<Path>();
		for (FileStatus status : fs.listStatus(inputPath)) {
			if (status.isDir()) {
				retPaths.addAll(getPaths(fs, status.getPath()));
			} else {
				retPaths.add(status.getPath());
			}
		}
		
		return retPaths;
	}
	
	/**
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws ParseException 
	 */
	public Job initJob(String[] args) throws IOException, ParseException {

		Path inputPath = new Path(args[0]);
		Path fakeOutputPath = new Path("fakeoutput");
		String outputPath = args[1];
		
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.set("backup.input.path", inputPath.toString());
		conf.set("backup.output.path", outputPath);
		
		FileSystem inputFs = FileSystem.get(inputPath.toUri(), new Configuration());
		FileSystem outputFs = FileSystem.get(getConf());
		List<Path> paths = getPaths(inputFs, inputPath);
		int suggestedMapRedTasks = conf.getInt("mapred.map.tasks", 4);
		Path[] inputSources = new Path[suggestedMapRedTasks];
		for (int i=0; i < inputSources.length; i++) {
			inputSources[i] = new Path("backup-inputsource" + i + ".txt");
		}
		List<BufferedWriter> writers = new ArrayList<BufferedWriter>();
		int idx = 0;
		try {
			for (Path source : inputSources) {
				writers.add(new BufferedWriter(new OutputStreamWriter(outputFs.create(source))));
			}
			for (Path p : paths) {
				writers.get(idx).write(p.toString());
				writers.get(idx).newLine();
				
				idx++;
				if (idx >= inputSources.length) {
					idx = 0;
				}
			}
		} finally {
			for (BufferedWriter writer : writers) {
				try {
					writer.close();
				} catch (IOException e) {
					LOG.error("Error closing writer for input source file", e);
				}
			}
			
			if (inputFs != null) {
				try {
					inputFs.close();
				} catch (IOException e) {
					LOG.error("Error closing input filesystem", e);
				}
			}
			
			if (outputFs != null) {
				try {
					outputFs.close();
				} catch (IOException e) {
					LOG.error("Error closing output filesystem", e);
				}
			}
		}
		
		Job job = new Job(getConf());
		job.setJobName(NAME);
		job.setJarByClass(Backup.class);
	
		job.setMapperClass(BackupMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
	
		job.setInputFormatClass(TextInputFormat.class);
		
		for (Path source : inputSources) {
			System.out.println("Adding input path: " + source.toString());
			FileInputFormat.addInputPath(job, source);
		}
	
		FileOutputFormat.setOutputPath(job, fakeOutputPath);
		
		return job;
	}

	/**
	 * @return
	 */
	private static int printUsage() {
		System.out.println("Usage: " + NAME + " [generic-options] <input-path> <output-path>");
		System.out.println();
		GenericOptionsParser.printGenericCommandUsage(System.out);
		
		return -1;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			return printUsage();
		}
		
		int rc = -1;
		Job job = initJob(args);
		job.waitForCompletion(true);
		if (job.isSuccessful()) {
			rc = 0;
		}
		
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(job.getConfiguration());
			hdfs.delete(FileOutputFormat.getOutputPath(job), true);
			hdfs.delete(new Path("backup-inputsource*.txt"), false);
		} finally {
			if (hdfs != null) {
				hdfs.close();
			}
		}

		return rc;
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
		int res = ToolRunner.run(new Configuration(), new Backup(), args);
		System.exit(res);
	}

}
