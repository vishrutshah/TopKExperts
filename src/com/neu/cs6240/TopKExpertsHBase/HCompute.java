package com.neu.cs6240.TopKExpertsHBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.neu.cs6240.TopKExperts.JoinQAKey;
import com.neu.cs6240.TopKExperts.JoinQA.JoinQAGroupComparator;

public class HCompute {

	/**
	 * @param args
	 */
	private static final String TABLE_NAME = "Post1";
	public static final String TABLE_FAMILY = "PostData";
	private static final String COLUMN_POST_ID = "COLUMN_POST_ID";
	private static final String COLUMN_POST_TYPE_ID = "COLUMN_POST_TYPE_ID";
	private static final String COLUMN_ACCEPTED_ANS_ID = "COLUMN_ACCEPTED_ANS_ID";
	private static final String COLUMN_TAGS = "COLUMN_TAGS";
	private static final String COLUMN_OWNER_USER_ID = "COLUMN_OWNER_USER_ID";

	public static class HComputeMapper extends TableMapper<HComputeKey, IntWritable> {
		private HTable table;
		private static final String HASH_SEPERATOR = "=";

		protected void setup(Context context) throws IOException {
			Configuration mapconf = new Configuration();
			table = new HTable(mapconf, TABLE_NAME);

		}

		public void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			Text k;
			Text val;

			byte[] acceptedAnsId = value.getValue(Bytes.toBytes(TABLE_FAMILY),
					Bytes.toBytes(COLUMN_ACCEPTED_ANS_ID));
			String s = new String(acceptedAnsId);
			if (s != null && !s.isEmpty()) {
				Result row = null;
				row = table.get(new Get(acceptedAnsId));

				byte[] hashTags = value.getValue(Bytes.toBytes(TABLE_FAMILY),
						Bytes.toBytes(COLUMN_TAGS));
				String hashString = new String(hashTags);
				String[] tagArray = hashString.replaceAll("><", HASH_SEPERATOR)
						.replaceAll("<", "").replaceAll(">", "")
						.split(HASH_SEPERATOR);

				if (!row.isEmpty()) {
					String userId = new String(row.getValue(
							Bytes.toBytes(TABLE_FAMILY),
							Bytes.toBytes(COLUMN_OWNER_USER_ID)));
					if (userId != null && !userId.isEmpty()) {
						val = new Text(userId);
						for (String hashTag : tagArray) {
							HComputeKey obj = new HComputeKey(val.toString(), hashTag);
							context.write(obj, new IntWritable(1));
						}
					}
					
				}
			}

		}
	}

	public static class HComputeReducer extends
			Reducer<HComputeKey, IntWritable, Text, NullWritable> {

		public void reduce(HComputeKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			String currentUserId = null;
			int total = 0;

			for (IntWritable value : values) {
				if (currentUserId == null) {
					currentUserId = key.getUserId().toString();
					total = value.get();
				} else if (!currentUserId.equalsIgnoreCase(key.getUserId().toString())) {
					StringBuilder output = new StringBuilder();
					output.append(currentUserId).append(",")
							.append(key.getHashTag()).append(",")
							.append(total);
					context.write(new Text(output.toString()),
							NullWritable.get());
					currentUserId = key.getUserId().toString();
					total = value.get();
				} else {
					total += value.get();
				}
			}

			// Writing for last user for this hash tag
			StringBuilder output = new StringBuilder();
			output.append(currentUserId).append(",")
				.append(key.getHashTag()).append(",")
				.append(total);
			context.write(new Text(output.toString()), NullWritable.get());
		}
	}

	public static class HComputePartitioner extends
			Partitioner<HComputeKey, IntWritable> {

		@Override
		public int getPartition(HComputeKey key, IntWritable value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getHashTag().hashCode() * 127)
					% numPartitions;
		}
	}

	public static class HComputeGroupComparator extends WritableComparator {
		protected HComputeGroupComparator() {
			super(HComputeKey.class, true);
		}

		/**
		 * Make sure that there'll be single reduce call per Post Id
		 */
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			HComputeKey key1 = (HComputeKey) w1;
			HComputeKey key2 = (HComputeKey) w2;
			return key1.getHashTag().compareTo(key2.getHashTag());
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: HCompute <out>");
			System.exit(2);
		}

		FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);

		SingleColumnValueFilter QUESTION_ONLY = new SingleColumnValueFilter(
				Bytes.toBytes(TABLE_FAMILY),
				Bytes.toBytes(COLUMN_POST_TYPE_ID), CompareOp.EQUAL,
				Bytes.toBytes("1"));
		
		SingleColumnValueFilter NO_NULL_USER_ID = new SingleColumnValueFilter(
				Bytes.toBytes(TABLE_FAMILY),
				Bytes.toBytes(COLUMN_OWNER_USER_ID), CompareOp.NOT_EQUAL,
				Bytes.toBytes(""));

		fl.addFilter(QUESTION_ONLY);
		fl.addFilter(NO_NULL_USER_ID);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.setFilter(fl);

		Job job = new Job(conf, "HCompute Delay");
		job.setJarByClass(HCompute.class);
		job.setMapperClass(HComputeMapper.class);
		job.setPartitionerClass(HComputePartitioner.class);
		job.setReducerClass(HComputeReducer.class);
		job.setGroupingComparatorClass(HComputeGroupComparator.class);
		job.setOutputKeyClass(HComputeKey.class);
		job.setOutputValueClass(IntWritable.class);

		TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan,
				HComputeMapper.class, HComputeKey.class, IntWritable.class, job);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		System.exit(exitCode);
	}

}
