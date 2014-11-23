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

package com.neu.cs6240.TopKExperts;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class UserAnswerCountPerHashTag {

	public static class UserAnswerCountPerHashTagMapper extends
			Mapper<Object, Text, UserAnswerCountPerHashTagKey, IntWritable> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');
		private static final String HASH_SEPERATOR = "="; 
				
		public void map(Object offset, Text line, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] parsedData = null;
			try{
				parsedData = this.csvParser.parseLine(line.toString()); 
			}catch(Exception e){
				// In case of bad data record ignore them
				return;
			}
			
			UserAnswerCountPerHashTagKey key = null;
			
			if (!isValid(parsedData)) {
				return;
			}

			String[] hashTags = parsedData[1].replaceAll("><", HASH_SEPERATOR).replaceAll("<", "").replaceAll(">", "").split(HASH_SEPERATOR);
			
			for(String hashTag : hashTags){
				key = new UserAnswerCountPerHashTagKey(hashTag, parsedData[0]);
				context.write(key, new IntWritable(1));
			}			
		}
		
		/**
		 * remove invalid entries
		 * 
		 * @param parsedData
		 * @return
		 */
		private boolean isValid(String[] parsedData) {
			// We must have User ID and HashTags
			if (parsedData[0].isEmpty()
					|| parsedData[1].isEmpty()) {
				return false;
			}
			return true;
		}
		
	}

	public static class UserAnswerCountPerHashTagReducer extends
			Reducer<UserAnswerCountPerHashTagKey, IntWritable, Text, NullWritable> {

		public void reduce(UserAnswerCountPerHashTagKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			String currentUserId = null;
			int total = 0;
			
			for(IntWritable value : values){
				if(currentUserId == null){
					currentUserId = key.getUserId().toString();
					total = value.get();
				}else if(! currentUserId.equalsIgnoreCase(key.getUserId().toString())){
					StringBuilder output = new StringBuilder();
					output.append(currentUserId).append(",").append(total).append(",").append(key.getHashTag().toString());							
					context.write(new Text(output.toString()), NullWritable.get());
					currentUserId = key.getUserId().toString();
					total = value.get();
				}else{
					total += value.get();
				}
			}
			
			// Writing for last user for this hash tag
			StringBuilder output = new StringBuilder();
			output.append(currentUserId).append(",").append(total).append(",").append(key.getHashTag().toString());
			context.write(new Text(output.toString()), NullWritable.get());
		}
	}

	public static class UserAnswerCountPerHashTagPartitioner extends
			Partitioner<UserAnswerCountPerHashTagKey, IntWritable> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique HashTags
		 */
		@Override
		public int getPartition(UserAnswerCountPerHashTagKey key, IntWritable value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getHashTag().hashCode() * 127) % numPartitions;
		}
	}

	public static class UserAnswerCountPerHashTagGroupComparator extends WritableComparator {
		protected UserAnswerCountPerHashTagGroupComparator() {
			super(UserAnswerCountPerHashTagKey.class, true);
		}

		/**
		 * Make sure that there'll be single reduce call per hash tag
		 */
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			UserAnswerCountPerHashTagKey key1 = (UserAnswerCountPerHashTagKey) w1;
			UserAnswerCountPerHashTagKey key2 = (UserAnswerCountPerHashTagKey) w2;
			return key1.getHashTag().compareTo(key2.getHashTag());
		}
	}

	public static void main(String[] args) throws Exception {		
		System.exit(UserAnswerCountPerHashTag.initUserAnswerCountPerHashTag(args) ? 0 : 1);	
	}
	
	public static boolean initUserAnswerCountPerHashTag(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UserAnswerCountPerHashTag <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UserAnswerCountPerHashTag");
		job.setJarByClass(UserAnswerCountPerHashTag.class);
		job.setMapperClass(UserAnswerCountPerHashTagMapper.class);
		job.setReducerClass(UserAnswerCountPerHashTagReducer.class);
		job.setOutputKeyClass(UserAnswerCountPerHashTagKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(UserAnswerCountPerHashTagPartitioner.class);
		job.setGroupingComparatorClass(UserAnswerCountPerHashTagGroupComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		return job.waitForCompletion(true);
	}
}