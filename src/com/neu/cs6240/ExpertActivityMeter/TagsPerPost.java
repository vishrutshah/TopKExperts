package com.neu.cs6240.ExpertActivityMeter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.MurmurHash;

import com.neu.cs6240.TopKExperts.TopKPerHashTag;
import com.neu.cs6240.TopKExperts.UserAnswerCountPerHashTag;

import au.com.bytecode.opencsv.CSVParser;

public class TagsPerPost {

	private static final int POST_ID = 0;
	private static final int POST_TYPE_ID = 1;
	private static final int PARENT_ID = 16;
	private static final int CREATION_DATE = 3;
	private static final int OWNER_USER_ID = 6;
	private static final int TAGS = 10;
	private static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	private static final SimpleDateFormat formatter = new SimpleDateFormat(
			DATE_PATTERN);

	public static class TagsPerPostMapper extends
	Mapper<Object, Text, TagsPerPostKey, TagsPerPostValue> {


		BloomFilter expertsBloomFilter;
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');
		//initialize time slot classifier
		private TimeSlotClassifier timeClassifier = new TimeSlotClassifier(4);

		@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
			try {
				expertsBloomFilter = new BloomFilter(1000, 2, MurmurHash.MURMUR_HASH);
				Path[] files = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());

				if (files == null || files.length == 0) {
					throw new RuntimeException(
							"User information is not set in DistributedCache");
				}

				// Read all files in the DistributedCache
				for (Path p : files) {
					BufferedReader rdr = new BufferedReader(
							new InputStreamReader(
									new FileInputStream(
											new File(p.toString()))));

					String line;
					// For each record in the expert file
					while ((line = rdr.readLine()) != null) {
						String experts = line.split("\\s+")[1];
						String[] expertUIds = this.csvParser.parseLine(experts);
						for(String expertID : expertUIds) {
							expertsBloomFilter.add(new Key(expertID.getBytes()));
						}
					}
				}

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

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

			if (!isValid(parsedData)) {
				return;
			}

			TagsPerPostValue value = null;
			TagsPerPostKey key = null;

			if (parsedData[POST_TYPE_ID].equals("1")) {
				// Question
				key = new TagsPerPostKey(parsedData[POST_ID], "Q");
				value = new TagsPerPostValue(parsedData[TAGS],"");
				context.write(key, value);
			} else 
				//if (expertsBloomFilter.membershipTest(new Key(parsedData[OWNER_USER_ID].getBytes()))) 
			{
				// Answer
				// User Id must exists otherwise ignore
				key = new TagsPerPostKey(parsedData[PARENT_ID], "A");
				try {
					value = new TagsPerPostValue("",
							"TimeSlot" + Integer.toString(timeClassifier.getTimeSlot(parsedData[CREATION_DATE])));
				} catch (ParseException e) {
					//failed to parse the time.
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
			// We must have POST_ID & POST_TYPE_ID & CREATION_DATE
			if (parsedData[POST_ID].isEmpty()
					|| parsedData[POST_TYPE_ID].isEmpty()
					|| parsedData[CREATION_DATE].isEmpty()) {
				return false;
			}

			// POST_TYPE_ID must be either 1 / 2
			if (!parsedData[POST_TYPE_ID].equals("1")
					&& !parsedData[POST_TYPE_ID].equals("2")) {
				return false;
			}

			// POST_TYPE_ID = 1 => TAGS
			if (parsedData[POST_TYPE_ID].equals("1")
					&& (parsedData[TAGS].isEmpty())) {
				return false;
			}

			// POST_TYPE_ID = 2 => we must have PARENT_ID & OWNER_USER_ID
			if (parsedData[POST_TYPE_ID].equals("2")
					&& (parsedData[PARENT_ID].isEmpty() || parsedData[OWNER_USER_ID].isEmpty())) {
				return false;
			}

			return true;
		}
	}

	public static class TagsPerPostPartitioner extends
	Partitioner<TagsPerPostKey, TagsPerPostValue> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique post Ids
		 */
		@Override
		public int getPartition(TagsPerPostKey key, TagsPerPostValue value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getPostId().hashCode() * 127) % numPartitions;
		}
	}

	public static class TagsPerPostGroupComparator extends WritableComparator {
		protected TagsPerPostGroupComparator() {
			super(TagsPerPostKey.class, true);
		}

		/**
		 * Make sure that there'll be single reduce call per Post Id
		 */
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TagsPerPostKey key1 = (TagsPerPostKey) w1;
			TagsPerPostKey key2 = (TagsPerPostKey) w2;
			return key1.getPostId().compareTo(key2.getPostId());
		}
	}

	public static class TagsPerPostReducer extends
	Reducer<TagsPerPostKey, TagsPerPostValue, Text, NullWritable> {

		public void reduce(TagsPerPostKey key, Iterable<TagsPerPostValue> values,
				Context context) throws IOException, InterruptedException {

			ArrayList<TagsPerPostValue> questions = new ArrayList<TagsPerPostValue>();

			for (TagsPerPostValue value : values) {
				// Get all questions those will be secondary sorted followed by
				// answers
				if (key.getFlag().toString().equals("Q")) {
					questions.add(new TagsPerPostValue(
							value.getHashTags().toString(), 
							value.getTimeSlot().toString()));
				} else {
					Iterator<TagsPerPostValue> questionIterator = questions
							.iterator();
					while (questionIterator.hasNext()) {
						TagsPerPostValue question = questionIterator.next();
						StringBuilder output = new StringBuilder();

						output.append(question.getHashTags().toString())
						.append(",")
						.append(value.getTimeSlot().toString());
						context.write(new Text(output.toString()),
								NullWritable.get());
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: TagsPerPost <in1> <in2> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "TagsPerPost");
		job.setJarByClass(TagsPerPost.class);
		job.setMapperClass(TagsPerPostMapper.class);
		job.setPartitionerClass(TagsPerPostPartitioner.class);
		job.setGroupingComparatorClass(TagsPerPostGroupComparator.class);
		job.setReducerClass(TagsPerPostReducer.class);
		job.setOutputKeyClass(TagsPerPostKey.class);
		job.setOutputValueClass(TagsPerPostValue.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Configure the DistributedCache
		DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(),
				job.getConfiguration());

		boolean isSucess = false; 
		
		isSucess = job.waitForCompletion(true);
		
		if(isSucess){
			// On successful completion of TagsPerPost start CountPerTagPerTimeSlot MR
			System.out.println("MR - TagsPerPost complete. Starting CountPerTagPerTimeSlot...");
			String[] argsForMR2 = new String[2];
			argsForMR2[0] = otherArgs[2];
			argsForMR2[1] = otherArgs[2] + "CountPerTagPerTimeSlotOutput/";
			isSucess = CountPerTagPerTimeSlot.initCountPerTagPerTimeSlot(argsForMR2);
			if(isSucess){
				// Successfully complete TopKPerHashTag MR
				System.out.println("All MR - Successful.");
			}else{
				// Failed UserAnswerCountPerHashTag MR
				System.out.println("MR - CountPerTagPerTimeSlot failed.");
			}
		}else{
			System.out.println("MR - TagsPerPost failed.");
		}
		
		System.exit(isSucess ? 0 : 1);
	}
}
