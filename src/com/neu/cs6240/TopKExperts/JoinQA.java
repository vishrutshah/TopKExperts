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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
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

import au.com.bytecode.opencsv.CSVParser;

public class JoinQA {

	private static final int POST_ID = 0;
	private static final int POST_TYPE_ID = 1;
	private static final int ACCEPTED_ANS_ID = 2;
	private static final int CREATION_DATE = 3;
	private static final int OWNER_USER_ID = 6;
	private static final int TAGS = 10;
	private static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	private static final SimpleDateFormat formatter = new SimpleDateFormat(
			DATE_PATTERN);

	public static class JoinQAMapper extends
			Mapper<Object, Text, JoinQAKey, JoinQAValue> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text line, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] parsedData = this.csvParser.parseLine(line.toString());
			JoinQAValue value = null;
			JoinQAKey key = null;

			if (!isValid(parsedData)) {
				return;
			}

			if (parsedData[POST_TYPE_ID].equals("1")) {
				// Question
				key = new JoinQAKey(parsedData[ACCEPTED_ANS_ID], "Q");
				value = new JoinQAValue("", parsedData[TAGS],
						parsedData[POST_ID], parsedData[CREATION_DATE]);
			} else {
				// Answer
				// User Id must exists otherwise ignore
				key = new JoinQAKey(parsedData[POST_ID], "A");
				value = new JoinQAValue(parsedData[OWNER_USER_ID], "", "",
						parsedData[CREATION_DATE]);

			}
			context.write(key, value);
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

			// POST_TYPE_ID = 1 => we must have ACCEPTED_ANS_ID & TAGS
			if (parsedData[POST_TYPE_ID].equals("1")
					&& (parsedData[ACCEPTED_ANS_ID].isEmpty() || parsedData[TAGS]
							.isEmpty())) {
				return false;
			}

			// POST_TYPE_ID = 2 => we must have POST_ID & OWNER_USER_ID
			if (parsedData[POST_TYPE_ID].equals("2")
					&& (parsedData[OWNER_USER_ID].isEmpty())) {
				return false;
			}

			return true;
		}
	}

	public static class JoinQAReducer extends
			Reducer<JoinQAKey, JoinQAValue, Text, NullWritable> {

		public void reduce(JoinQAKey key, Iterable<JoinQAValue> values,
				Context context) throws IOException, InterruptedException {

			ArrayList<JoinQAValue> questions = new ArrayList<JoinQAValue>();

			for (JoinQAValue value : values) {
				// Get all questions those will be secondary sorted followed by
				// answers
				if (key.getFlag().toString().equals("Q")) {
					questions.add(new JoinQAValue(value.getUserId().toString(),
							value.getHashTags().toString(), value
									.getQuestionId().toString(), value
									.getCreationDate().toString()));
				} else {
					Iterator<JoinQAValue> questionIterator = questions
							.iterator();
					while (questionIterator.hasNext()) {
						JoinQAValue question = questionIterator.next();
						StringBuilder output = new StringBuilder();

						int timeToAnsInMinutes = getDiffMinutes(question
								.getCreationDate().toString(), value
								.getCreationDate().toString());

						output.append(value.getUserId().toString()).append(",")
								.append(question.getHashTags().toString())
								.append(",")
								.append(""+timeToAnsInMinutes)
								.append(",")
								.append(question.getQuestionId().toString());
						context.write(new Text(output.toString()),
								NullWritable.get());
					}
				}
			}
		}

		private int getDiffMinutes(String questionDate, String ansDate) {
			int minutes = -1;
			try {
				Date qDate = formatter.parse(questionDate);
				Date aDate = formatter.parse(ansDate);

				long milliSeconds = aDate.getTime() - qDate.getTime();
				minutes = (int) (milliSeconds /(60 * 1000));

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return minutes;
		}
	}

	public static class JoinQAPartitioner extends
			Partitioner<JoinQAKey, JoinQAValue> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique post Ids
		 */
		@Override
		public int getPartition(JoinQAKey key, JoinQAValue value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getPostId().hashCode() * 127) % numPartitions;
		}
	}

	public static class JoinQAGroupComparator extends WritableComparator {
		protected JoinQAGroupComparator() {
			super(JoinQAKey.class, true);
		}

		/**
		 * Make sure that there'll be single reduce call per Post Id
		 */
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			JoinQAKey key1 = (JoinQAKey) w1;
			JoinQAKey key2 = (JoinQAKey) w2;
			return key1.getPostId().compareTo(key2.getPostId());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: JoinQA <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "JoinQA");
		job.setJarByClass(JoinQA.class);
		job.setMapperClass(JoinQAMapper.class);
		job.setReducerClass(JoinQAReducer.class);
		job.setOutputKeyClass(JoinQAKey.class);
		job.setOutputValueClass(JoinQAValue.class);
		job.setPartitionerClass(JoinQAPartitioner.class);
		job.setGroupingComparatorClass(JoinQAGroupComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}