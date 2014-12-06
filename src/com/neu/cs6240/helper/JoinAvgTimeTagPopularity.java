package com.neu.cs6240.helper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class JoinAvgTimeTagPopularity {

	private static final int TAG = 0;
	private static final int MINS = 1;
	private static final int POPULARITY = 1;
	private static final String SPLITTER = ",";

	public static class AvgTimeToAnsPerHashTagMapper extends
			Mapper<Object, Text, Text, Text> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text line, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] parsedData = this.csvParser.parseLine(line.toString());
			Text value = null;
			Text key = null;

			key = new Text(parsedData[TAG]);
			value = new Text("0" + SPLITTER + parsedData[MINS]);
			context.write(key, value);
		}
	}

	public static class HashTagPopularityMapper extends
			Mapper<Object, Text, Text, Text> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text line, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] parsedData = this.csvParser.parseLine(line.toString());
			Text value = null;
			Text key = null;

			key = new Text(parsedData[TAG]);
			value = new Text("1" + SPLITTER + parsedData[POPULARITY]);
			context.write(key, value);
		}
	}

	public static class AvgTimeToAnsPerHashTagReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder output = new StringBuilder();

			String avgTime = "";
			String popularity = "";

			for (Text value : values) {
				if (value.toString().startsWith("0")) {
					avgTime = value.toString().split(SPLITTER)[1];
				} else {
					popularity = value.toString().split(SPLITTER)[1];
				}
			}

			output.append(avgTime).append(",").append(popularity);
			context.write(key, new Text(output.toString()));
		}
	}

	public static class JoinAvgTimeTagPopularityPartitioner extends
			Partitioner<Text, Text> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique hash Tags
		 */
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: JoinAvgTimeTagPopularity <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "JoinAvgTimeTagPopularity");
		job.setJarByClass(JoinAvgTimeTagPopularity.class);

		job.setMapperClass(AvgTimeToAnsPerHashTagMapper.class);
		job.setMapperClass(HashTagPopularityMapper.class);
		job.setReducerClass(AvgTimeToAnsPerHashTagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(JoinAvgTimeTagPopularityPartitioner.class);

		Path inputAvgTimePath = new Path(otherArgs[0]);
		Path inputPopularityPath = new Path(otherArgs[1]);
		Path outputPath = new Path(otherArgs[2]);

		job.setInputFormatClass(TextInputFormat.class);

		// Setting multiple input class
		MultipleInputs.addInputPath(job, inputAvgTimePath,
				TextInputFormat.class, AvgTimeToAnsPerHashTagMapper.class);
		MultipleInputs.addInputPath(job, inputPopularityPath,
				TextInputFormat.class, HashTagPopularityMapper.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
