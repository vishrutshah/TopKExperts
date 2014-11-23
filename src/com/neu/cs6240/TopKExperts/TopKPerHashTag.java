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

public class TopKPerHashTag {
	// Define Top K
	private static final int TOP_K = 5;
	// Column definition
	private static final int USER_ID = 0;
	private static final int ANS_COUNT = 1;
	private static final int HASH_TAG = 2;
	
	public static class TopKPerHashTagMapper extends
			Mapper<Object, Text, TopKPerHashTagKey, Text> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');
						
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
			
			TopKPerHashTagKey key = null;
			int ansCountPerUserId;
			
			if (!isValid(parsedData)) {
				return;
			}
			
			// In case of invalid input continue
			try{
				ansCountPerUserId = Integer.parseInt(parsedData[ANS_COUNT]);
			}catch(NumberFormatException ex){
				return;
			}

			key = new TopKPerHashTagKey(parsedData[HASH_TAG], ansCountPerUserId);
			context.write(key, new Text(parsedData[USER_ID]));
		}
		
		/**
		 * remove invalid entries
		 * 
		 * @param parsedData
		 * @return
		 */
		private boolean isValid(String[] parsedData) {
			// We must have User ID, count per HashTag and HashTag
			if (parsedData[USER_ID].isEmpty()
					|| parsedData[ANS_COUNT].isEmpty() || parsedData[HASH_TAG].isEmpty()) {
				return false;
			}
			return true;
		}
		
	}

	public static class TopKPerHashTagReducer extends
			Reducer<TopKPerHashTagKey, Text, Text, Text> {

		public void reduce(TopKPerHashTagKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			StringBuilder kExperts = new StringBuilder();
			int findMore = TOP_K;
			
			for(Text value : values){
				if(findMore <= 0){
					break;
				}
				
				if(kExperts.length() == 0){
					kExperts.append(value);
					// Only for debug purpose
					// kExperts.append(value).append("(" + key.getAnsCountPerUserId().get() +")");
				}else{
					kExperts.append(",").append(value);
					// Only for debug purpose
					// kExperts.append(",").append(value).append("(" + key.getAnsCountPerUserId().get() +")");
				}
				findMore--;
			}
			
			// Output Top K Experts for this hash Tag			
			context.write(key.getHashTag(), new Text(kExperts.toString()));
		}
	}

	public static class TopKPerHashTagPartitioner extends
			Partitioner<TopKPerHashTagKey, Text> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique HashTag
		 */
		@Override
		public int getPartition(TopKPerHashTagKey key, Text value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getHashTag().hashCode() * 127) % numPartitions;
		}
	}

	public static class TopKPerHashTagGroupComparator extends WritableComparator {
		protected TopKPerHashTagGroupComparator() {
			super(TopKPerHashTagKey.class, true);
		}

		/**
		 * Make sure that there'll be single reduce call per hash tag
		 */
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TopKPerHashTagKey key1 = (TopKPerHashTagKey) w1;
			TopKPerHashTagKey key2 = (TopKPerHashTagKey) w2;
			return key1.getHashTag().compareTo(key2.getHashTag());
		}
	}

	public static void main(String[] args) throws Exception {
		System.exit(TopKPerHashTag.initTopKPerHashTag(args) ? 0 : 1);
	}
	
	public static boolean initTopKPerHashTag(String[] args) throws Exception {		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopKPerHashTag <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "TopKPerHashTag");
		job.setJarByClass(TopKPerHashTag.class);
		job.setMapperClass(TopKPerHashTagMapper.class);
		job.setReducerClass(TopKPerHashTagReducer.class);
		job.setOutputKeyClass(TopKPerHashTagKey.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(TopKPerHashTagPartitioner.class);
		job.setGroupingComparatorClass(TopKPerHashTagGroupComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));	
		
		return job.waitForCompletion(true);
	}
}