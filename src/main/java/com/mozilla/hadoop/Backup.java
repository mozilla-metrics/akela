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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * Backup is a distcp alternative.  It was created specifically for copying HBase table directories while
 * the source cluster may still be running.  Backup tends to be more immune to errors than distcp when running
 * in this type of scenario as it favors swallowing exceptions and incrementing counters as opposed to failing.
 * 
 * The MapReduce output is used to list files that have failed so that list could be used for investigation purposes
 * and potentially distcp at a later time.
 * 
 * @author Xavier Stevens
 *
 */
public class Backup implements Tool {

    private static final Logger LOG = Logger.getLogger(Backup.class);
	
	private static final String NAME = "Backup";
	
	private Configuration conf;
	
	public static class BackupMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		public enum ReportStats { DIRECTORY_GET_PATHS_FAILED, BYTES_EXPECTED, BYTES_COPIED, BYTES_OUTPUTFS, COPY_FILE_FNF_FAILURE, COPY_FILE_FAILED, NOT_MODIFIED };
		
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
			checkAndClose(inputFs);
		}
		  
		/**
		 * Copy the file at inputPath to the destination cluster using the same directory structure
		 * @param inputPath
		 * @param context
		 * @throws InterruptedException
		 * @throws IOException
		 */
		private void copyFile(Path inputPath, Context context) throws InterruptedException, IOException {
			FSDataInputStream dis = null;
			FSDataOutputStream dos = null;
			
			String commonPath = null;
			Matcher m = filePattern.matcher(inputPath.toString());
			if (m.find()) {
				if (m.groupCount() == 1) {
					commonPath = m.group(1);
				}
			} else {
				throw new RuntimeException("Regex match on common path failed");
			}
			
			Path fullOutputPath = new Path(outputRootPath + commonPath);
			try {		
				outputFs.mkdirs(fullOutputPath.getParent());
				long inputFileSize = inputFs.getFileStatus(inputPath).getLen();
				if (outputFs.exists(fullOutputPath) && inputFs.getFileStatus(inputPath).getLen() == outputFs.getFileStatus(fullOutputPath).getLen()) {
					context.getCounter(ReportStats.NOT_MODIFIED).increment(1L);
				} else {
					context.getCounter(ReportStats.BYTES_EXPECTED).increment(inputFileSize);
					
					dis = inputFs.open(inputPath);
					dos = outputFs.create(fullOutputPath, true);

					LOG.info("Writing " + inputPath.toString() + " to " + fullOutputPath);
					String statusStr = "%s to %s : copying [ %s / %s ]";
					long totalBytesWritten = 0L;
					Object[] statusFormatArgs = new Object[] { inputPath.toString(), fullOutputPath, StringUtils.humanReadableInt(totalBytesWritten), StringUtils.humanReadableInt(inputFileSize) };
					byte[] buffer = new byte[65536];
					int bytesRead = 0;
					int writeCount = 0;
					while ((bytesRead = dis.read(buffer)) >= 0) {
						dos.write(buffer, 0, bytesRead);
						totalBytesWritten += bytesRead;
						// Give progress and status updates (roughly once per MB)
						if (writeCount % 20 == 0) {
							context.progress();
							writeCount = 0;
							// output copy status
							statusFormatArgs[2] = StringUtils.humanReadableInt(totalBytesWritten);
							context.setStatus(String.format(statusStr, statusFormatArgs));
						}
						writeCount++;
					}
					context.getCounter(ReportStats.BYTES_COPIED).increment(totalBytesWritten);
				}
			} catch (FileNotFoundException e) {
				LOG.error("Source file is missing", e);
				context.getCounter(ReportStats.COPY_FILE_FNF_FAILURE).increment(1L);
			} catch (Exception e) {
				LOG.error("Error copying file", e);
				context.getCounter(ReportStats.COPY_FILE_FAILED).increment(1L);
				context.write(new Text(inputPath.toString()), NullWritable.get());
			} finally {
				checkAndClose(dis);
				checkAndClose(dos);
			}
			
			context.getCounter(ReportStats.BYTES_OUTPUTFS).increment(outputFs.getFileStatus(fullOutputPath).getLen());
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {		
			Path inputPath = new Path(value.toString());
			List<Path> inputPaths = null;
			try {
				inputPaths = Backup.getAllPaths(inputFs, inputPath);
				if (inputPaths != null && inputPaths.size() > 0) {
					for (Path p : inputPaths) {
						copyFile(p, context);
					}
				}
			} catch (Exception e) {
				LOG.error("Directory getPaths failed", e);
				context.getCounter(ReportStats.DIRECTORY_GET_PATHS_FAILED).increment(1L);
				context.write(new Text(value.toString()), NullWritable.get());
				return;
			}
		}
		
	}	

	/**
	 * Check the handle and close it
	 * @param c
	 */
	private static void checkAndClose(java.io.Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
				LOG.error("Error closing stream", e);
			}
		}
	}
	
	/**
	 * Load a list of paths from a file
	 * @param fs
	 * @param inputPath
	 * @return
	 * @throws IOException
	 */
	public static List<Path> loadPaths(FileSystem fs, Path inputPath) throws IOException {
		List<Path> retPaths = new ArrayList<Path>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				retPaths.add(new Path(line));
			}
		} catch (IOException e) {
			LOG.error("Exception in loadPaths for inputPath: " + inputPath.toString());
		} finally {
			checkAndClose(reader);
		}
		
		return retPaths;
	}
	
	/**
	 * Walk recursively to get all file paths up to a max depth
	 * @param fs
	 * @param inputPath
	 * @param depth
	 * @param maxDepth
	 * @return
	 * @throws IOException
	 */
	public static List<Path> getPaths(FileSystem fs, Path inputPath, int depth, int maxDepth) throws IOException {
		List<Path> retPaths = new ArrayList<Path>();
		for (FileStatus status : fs.listStatus(inputPath)) {
			if (status.isDir() && (maxDepth == -1 || depth < maxDepth)) {
				retPaths.addAll(getPaths(fs, status.getPath(), depth + 1, maxDepth));
			} else {
				retPaths.add(status.getPath());
			}
		}
		
		return retPaths;
	}
	
	/**
	 * Walk recursively to get all file paths
	 * @param fs
	 * @param inputPath
	 * @param endTimeMillis
	 * @return
	 * @throws IOException
	 */
	public static List<Path> getAllPaths(FileSystem fs, Path inputPath) throws IOException {
		return getPaths(fs, inputPath, 0, -1);
	}
	
	/**
	 * Get the input source files to be used as input for the backup mappers
	 * @param inputFs
	 * @param inputPath
	 * @param outputFs
	 * @return
	 * @throws IOException
	 */
	public Path[] createInputSources(List<Path> paths, FileSystem outputFs) throws IOException {
		int suggestedMapRedTasks = conf.getInt("mapred.map.tasks", 1);
		Path[] inputSources = new Path[suggestedMapRedTasks];
		for (int i=0; i < inputSources.length; i++) {
			inputSources[i] = new Path(NAME + "-inputsource" + i + ".txt");
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
				checkAndClose(writer);
			}
		}
		
		return inputSources;
	}
	
	/**
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws ParseException 
	 */
	public Job initJob(String[] args) throws IOException, ParseException {

		Path inputPath = null;
		Path loadPath = null;
		String outputPath = null;
		boolean useSpecifiedPaths = false;
		for (int idx=0; idx < args.length; idx++) {
			if ("-f".equals(args[idx])) {
				useSpecifiedPaths = true;
				loadPath = new Path(args[++idx]);
			} else if (idx == args.length -1) {
				outputPath = args[idx];
			} else {
				inputPath = new Path(args[idx]);
			}
		}

		Path mrOutputPath = new Path(NAME + "-results");
		
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.set("backup.input.path", inputPath.toString());
		conf.set("backup.output.path", outputPath);
		
		FileSystem inputFs = null;
		FileSystem outputFs = null;
		Path[] inputSources = null;
		try {
			inputFs = FileSystem.get(inputPath.toUri(), new Configuration());
			outputFs = FileSystem.get(getConf());
			if (useSpecifiedPaths) {
				inputSources = createInputSources(loadPaths(outputFs, loadPath), outputFs);
			} else {
				inputSources = createInputSources(getPaths(inputFs, inputPath, 0, 2), outputFs);
			}
		} finally {
			checkAndClose(inputFs);
			checkAndClose(outputFs);
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
	
		FileOutputFormat.setOutputPath(job, mrOutputPath);
		
		return job;
	}

	/**
	 * @return
	 */
	private static int printUsage() {
		System.out.println("Usage: " + NAME + " [generic-options] [-f <file-list-path>] <input-path> <output-path>");
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
		
		int rc = -1;
		Job job = initJob(args);
		job.waitForCompletion(true);
		if (job.isSuccessful()) {
			rc = 0;
			
			FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(job.getConfiguration());
				hdfs.delete(new Path(NAME + "-inputsource*.txt"), false);
			} finally {
				checkAndClose(hdfs);
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
