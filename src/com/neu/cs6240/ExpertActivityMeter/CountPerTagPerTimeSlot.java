package com.neu.cs6240.ExpertActivityMeter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.neu.cs6240.ExpertActivityMeter.TagsPerPost.TagsPerPostGroupComparator;
import com.neu.cs6240.ExpertActivityMeter.TagsPerPost.TagsPerPostMapper;
import com.neu.cs6240.ExpertActivityMeter.TagsPerPost.TagsPerPostPartitioner;
import com.neu.cs6240.ExpertActivityMeter.TagsPerPost.TagsPerPostReducer;

import au.com.bytecode.opencsv.CSVParser;

public class CountPerTagPerTimeSlot {
	private static final int TAGS = 0;
	private static final int TIMESLOT = 1;
	private static final String HASH_SEPERATOR = "=";

	public static class CountPerTagPerTimeSlotMapper extends
	Mapper<Object, Text, CountPerTagPerTimeSlotKey, NullWritable> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text line, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] parsedData = this.csvParser.parseLine(line.toString());
			CountPerTagPerTimeSlotKey key = null;

			String[] hashTags = parsedData[TAGS]
					.replaceAll("><", HASH_SEPERATOR).replaceAll("<", "")
					.replaceAll(">", "").split(HASH_SEPERATOR);

			for(String hashTag : hashTags){
				key = new CountPerTagPerTimeSlotKey(hashTag, parsedData[TIMESLOT]);
				context.write(key, NullWritable.get());
			}
		}
	}
	
	public static class CountPerTagPerTimeSlotPartitioner extends
	Partitioner<CountPerTagPerTimeSlotKey, NullWritable> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique HashTags
		 */
		@Override
		public int getPartition(CountPerTagPerTimeSlotKey key, NullWritable value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.tag.hashCode() * 127) % numPartitions;
		}
	}
	
	public static class CountPerTagPerTimeSlotGroupComparator extends WritableComparator {
		protected CountPerTagPerTimeSlotGroupComparator() {
			super(CountPerTagPerTimeSlotKey.class, true);
		}
		/**
		 * Make sure that there'll be single reduce call per HashTag
		 */
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CountPerTagPerTimeSlotKey key1 = (CountPerTagPerTimeSlotKey) w1;
			CountPerTagPerTimeSlotKey key2 = (CountPerTagPerTimeSlotKey) w2;
			return key1.tag.compareTo(key2.tag);
		}
	}
	
	public static class CountPerTagPerTimeSlotReducer extends
	Reducer<CountPerTagPerTimeSlotKey, NullWritable, Text, NullWritable> {

		public void reduce(CountPerTagPerTimeSlotKey key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			int counter = 0;
			String currentTimeSlot = key.timeSlot.toString();
			StringBuilder output = new StringBuilder();
			output.append(key.tag.toString())
			.append(",");
			for (NullWritable value : values) {
				if (currentTimeSlot.equalsIgnoreCase(key.timeSlot.toString())) {
					counter++;
				} else {
					output.append(currentTimeSlot)
					.append(":")
					.append(Integer.toString(counter))
					.append(",");
					counter = 1;
					currentTimeSlot = key.timeSlot.toString();
				}
			}
			
			output.append(currentTimeSlot)
			.append(":")
			.append(Integer.toString(counter));
			
			context.write(new Text(output.toString()),
					NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountPerTagPerTimeSlot <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CountPerTagPerTimeSlot");
		job.setJarByClass(CountPerTagPerTimeSlot.class);
		job.setMapperClass(CountPerTagPerTimeSlotMapper.class);
		job.setPartitionerClass(CountPerTagPerTimeSlotPartitioner.class);
		job.setGroupingComparatorClass(CountPerTagPerTimeSlotGroupComparator.class);
		job.setReducerClass(CountPerTagPerTimeSlotReducer.class);
		job.setOutputKeyClass(CountPerTagPerTimeSlotKey.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
