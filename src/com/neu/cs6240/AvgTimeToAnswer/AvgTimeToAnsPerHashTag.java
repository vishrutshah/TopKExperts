/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.neu.cs6240.AvgTimeToAnswer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class AvgTimeToAnsPerHashTag {

	private static final int TAGS = 1;
	private static final int MINS = 2;
	private static final String HASH_SEPERATOR = "=";

	public static class AvgTimeToAnsPerHashTagMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text line, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] parsedData = this.csvParser.parseLine(line.toString());
			IntWritable value = null;
			Text key = null;

			if (!isValid(parsedData)) {
				return;
			}

			String[] hashTags = parsedData[TAGS]
					.replaceAll("><", HASH_SEPERATOR).replaceAll("<", "")
					.replaceAll(">", "").split(HASH_SEPERATOR);

			for(String hashTag : hashTags){
				key = new Text(hashTag);
				// Just in case some minutes values cannot be parsed
				try{
					int minutes = Integer.parseInt(parsedData[MINS]);
					value = new IntWritable(minutes);
				}catch(NumberFormatException ex){
					// Ignore the record 
					return;
				}
				
				context.write(key, value);
			}			
		}

		/**
		 * remove invalid entries
		 * 
		 * @param parsedData
		 * @return
		 */
		private boolean isValid(String[] parsedData) {
			// We must have TAGS & MINS
			if (parsedData[TAGS].isEmpty() || parsedData[MINS].isEmpty()) {
				return false;
			}
			return true;
		}
	}

	public static class AvgTimeToAnsPerHashTagReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			int totalMinutes = 0;
			int totalEntries = 0;
			
			for (IntWritable value : values) {
					totalEntries++;
					totalMinutes += value.get();
				}
									
			context.write(key, new IntWritable(totalMinutes/totalEntries));
			}
		}
	
	public static class AvgTimeToAnsPerHashTagPartitioner extends
			Partitioner<Text, IntWritable> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique hash Tags
		 */
		@Override
		public int getPartition(Text key, IntWritable value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AvgTimeToAnsPerHashTag <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "AvgTimeToAnsPerHashTag");
		job.setJarByClass(AvgTimeToAnsPerHashTag.class);
		job.setMapperClass(AvgTimeToAnsPerHashTagMapper.class);
		job.setReducerClass(AvgTimeToAnsPerHashTagReducer.class);
		job.setCombinerClass(AvgTimeToAnsPerHashTagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(AvgTimeToAnsPerHashTagPartitioner.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}