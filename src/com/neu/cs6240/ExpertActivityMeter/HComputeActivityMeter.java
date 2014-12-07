package com.neu.cs6240.ExpertActivityMeter;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.MurmurHash;

import au.com.bytecode.opencsv.CSVParser;


public class HComputeActivityMeter 
{
	private static final int TAGS = 0;
	private static final String HASH_SEPERATOR = "=";

	// Custom Schema Element Names
	public static final String TABLE_NAME 	= "Post_Table";
	public static final String COL_FAMILY 	= "Post_Family";
	public static final String QNAFLAG 		= "QnAFlag";
	public static final String HASHES       = "Hashes";
	public static final String CREATIONDATE = "Creationdate";
	public static final String PARENTID 	= "PArentID";
	public static final String UID 			= "OwnerUserId";

	// Schema Element Name definitions to Bytes
	public static final byte[] BYTES_COL_FAMILY 	= Bytes.toBytes(COL_FAMILY);
	public static final byte[] BYTES_QNAFLAG 		= Bytes.toBytes(QNAFLAG);
	public static final byte[] BYTES_HASHES  		= Bytes.toBytes(HASHES);
	public static final byte[] BYTES_CREATIONDATE 	= Bytes.toBytes(CREATIONDATE);
	public static final byte[] BYTES_PARENTID 		= Bytes.toBytes(PARENTID);
	public static final byte[] BYTES_UID 		    = Bytes.toBytes(UID);

	static class HComputeActivityMeterMapper extends TableMapper<CountPerTagPerTimeSlotKey, NullWritable>
	{	 
		//initialize time slot classifier
		private TimeSlotClassifier timeClassifier = new TimeSlotClassifier(4);
		BloomFilter expertsBloomFilter;
		private CSVParser csvParser = new CSVParser(',', '"');
		
		@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
			try {
				expertsBloomFilter = new BloomFilter(1000, 2, MurmurHash.MURMUR_HASH);
				
				Configuration conf = context.getConfiguration();
				Path topKExpertsFilePath = new Path(conf.get("topKExpertsFilePath"));
				FileSystem fs = topKExpertsFilePath.getFileSystem(conf);

				BufferedReader rdr = new BufferedReader(
						new InputStreamReader(fs.open(topKExpertsFilePath)));

				String line;
				// For each record in the expert file
				while ((line = rdr.readLine()) != null) {
					String experts = line.split("\\s+")[1];
					String[] expertUIds = this.csvParser.parseLine(experts);
					for(String expertID : expertUIds) {
						expertsBloomFilter.add(new Key(expertID.getBytes()));
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		

		public void map(ImmutableBytesWritable ibwRow, Result value, Context context) throws IOException, InterruptedException 
		{
			String postID = new String(value.getRow());
			String qnaFlag = new String(value.getValue(BYTES_COL_FAMILY, BYTES_QNAFLAG));
			String hashes = "";
			String creationDate = new String(value.getValue(BYTES_COL_FAMILY, BYTES_CREATIONDATE));
			String parentID = new String(value.getValue(BYTES_COL_FAMILY, BYTES_PARENTID));
			String uID = new String(value.getValue(BYTES_COL_FAMILY, BYTES_UID));
			
			HTable htable = null;
			try {
				htable = new HTable(context.getConfiguration(), TABLE_NAME);
				Get aGetVal = new Get(Bytes.toBytes(parentID));

				Result result = htable.get(aGetVal);
				hashes = new String(result.getValue(BYTES_COL_FAMILY, BYTES_HASHES));
			}
			finally{
				if (htable != null){
					htable.close();
				}
			}
			// Iterate through the Hashes and Emit with the Creation date
			if (!hashes.isEmpty()){
				String[] hashTags = hashes
						.replaceAll("><", HASH_SEPERATOR).replaceAll("<", "")
						.replaceAll(">", "").split(HASH_SEPERATOR);
				CountPerTagPerTimeSlotKey key = null;

				if (!uID.isEmpty())
				{
					if (expertsBloomFilter.membershipTest(new Key(uID.getBytes())))
					{
						for(String hashTag : hashTags){
							try {
								key = new CountPerTagPerTimeSlotKey(hashTag, "TimeSlot" 
										+ Integer.toString(timeClassifier.getTimeSlot(creationDate)));
							} 
							catch (ParseException e) {
								return;
							}
							context.write(key, NullWritable.get());
						}
					}
				}
			}
		}
	}

	public static class HComputeActivityMeterPartitioner extends
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

	public static class HComputeActivityMeterGroupComparator extends WritableComparator {
		protected HComputeActivityMeterGroupComparator() {
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

	public static class HComputeActivityMeterReducer extends
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

			context.write(new Text(output.toString()), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration config = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: HCompute <out>");
			System.exit(2);
		}
		Path topKExpertsFilePath = new Path(args[0]);
		config.set("topKExpertsFilePath", topKExpertsFilePath.toString());
		
		Job job = new Job(config, "HCompute");
		job.setJarByClass(HComputeActivityMeter.class); 
		job.setOutputKeyClass(CountPerTagPerTimeSlotKey.class);
		job.setOutputValueClass(NullWritable.class);
		

		// Construct a list of filters
		FilterList lFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

		//  See if Tags is empty, signifying an Answer post 
		SingleColumnValueFilter tagFieldNull_filter = new SingleColumnValueFilter(BYTES_COL_FAMILY,
				BYTES_QNAFLAG,
				CompareOp.EQUAL,
				Bytes.toBytes("2"));

		// Add filters to list of filters
		lFilters.addFilter(tagFieldNull_filter);

		// Create a Scan operation across all rows
		Scan hBasescaner = new Scan();

		// Set the number of rows for caching that will be passed to scanners
		hBasescaner.setCaching(500);        
		hBasescaner.setCacheBlocks(false); 

		// Add the list of Filters to the Hbase Scanner
		hBasescaner.setFilter(lFilters);

		// Appropriately set up the TableMap job
		TableMapReduceUtil.initTableMapperJob(TABLE_NAME, 
				hBasescaner,
				HComputeActivityMeterMapper.class, 
				CountPerTagPerTimeSlotKey.class,
				NullWritable.class,
				job);

		job.setReducerClass(HComputeActivityMeterReducer.class);    
		job.setGroupingComparatorClass(HComputeActivityMeterGroupComparator.class);
		job.setPartitionerClass(HComputeActivityMeterPartitioner.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (job.waitForCompletion(true)) 
			System.exit(0); 
		else 
			System.exit(1);
	}
}
