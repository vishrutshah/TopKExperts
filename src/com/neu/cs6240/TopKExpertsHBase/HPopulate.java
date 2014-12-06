package com.neu.cs6240.TopKExpertsHBase;



import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import au.com.bytecode.opencsv.CSVParser;

public class HPopulate {

	/**
	 * @param args
	 */
	private static final String TABLE_NAME = "Post";
	public static final String TABLE_FAMILY = "PostData";
	private static final int POST_ID = 0;
	private static final int POST_TYPE_ID = 1;
	private static final int ACCEPTED_ANS_ID = 2;
	private static final int CREATION_DATE = 3;
	private static final int SCORE = 4;
	private static final int VIEW_COUNT = 5;
	private static final int OWNER_USER_ID = 6;
	private static final int LAST_EDITOR_DISPLAY_NAME = 7;
	private static final int LAST_EDIT_DATE = 8;
	private static final int LAST_ACTIVITY_DATE = 9;
	private static final int TAGS = 10;
	private static final int ANSWER_COUNT = 11;
	private static final int COMMENT_COUNT = 12;
	private static final int FAVORITE_COUNT = 13;
	private static final int COMMUNITY_OWNED_DATE = 14;
	private static final int TITLE = 15;
	private static final int PARENT_ID = 16;
	
	
	private static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	private static final SimpleDateFormat formatter = new SimpleDateFormat(
			DATE_PATTERN);
	private static final String COLUMN_POST_ID = "COLUMN_POST_ID";
	private static final String COLUMN_POST_TYPE_ID = "COLUMN_POST_TYPE_ID";
	private static final String COLUMN_ACCEPTED_ANS_ID = "COLUMN_ACCEPTED_ANS_ID";
	private static final String COLUMN_CREATION_DATE = "COLUMN_CREATION_DATE";
	private static final String COLUMN_SCORE = "COLUMN_SCORE";
	private static final String COLUMN_VIEW_COUNT = "COLUMN_VIEW_COUNT";
	private static final String COLUMN_OWNER_USER_ID = "COLUMN_OWNER_USER_ID";
	private static final String COLUMN_LAST_EDITOR_DISPLAY_NAME = "COLUMN_LAST_EDITOR_DISPLAY_NAME";
	private static final String COLUMN_LAST_EDIT_DATE = "COLUMN_LAST_EDIT_DATE";
	private static final String COLUMN_LAST_ACTIVITY_DATE = "COLUMN_LAST_ACTIVITY_DATE";
	private static final String COLUMN_TAGS = "COLUMN_TAGS";
	private static final String COLUMN_ANSWER_COUNT = "COLUMN_ANSWER_COUNT";
	private static final String COLUMN_COMMENT_COUNT = "COLUMN_COMMENT_COUNT";
	private static final String COLUMN_FAVORITE_COUNT = "COLUMN_FAVORITE_COUNT";
	private static final String COLUMN_COMMUNITY_OWNED_DATE = "COLUMN_COMMUNITY_OWNED_DATE";
	private static final String COLUMN_TITLE = "COLUMN_TITLE";
	private static final String COLUMN_PARENT_ID = "COLUMN_PARENT_ID";
	
	
	
	
	
	public static class HPopulateMapper extends Mapper<Object, Text, ImmutableBytesWritable, Writable>{
		private CSVParser csvParser = null;
		private HTable h_table = null;
		
		
		protected void setup(Context context) throws IOException{
			this.csvParser = new CSVParser(',','"');
			this.h_table = new HTable(context.getConfiguration(), TABLE_NAME);
			this.h_table.setAutoFlush(false);
			this.h_table.setWriteBufferSize(1024*50);	//setting the buffer flush size as 50MB
		}
		
		@Override
		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException{
			String line[] = this.csvParser.parseLine(value.toString());
			if(isValid(line)){
				Put put = new Put(getRowKey(line));	//Unique Row key as Flight year and system timestamp
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_POST_ID),Bytes.toBytes(line[POST_ID]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_POST_TYPE_ID),Bytes.toBytes(line[POST_TYPE_ID]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_ACCEPTED_ANS_ID),Bytes.toBytes(line[ACCEPTED_ANS_ID]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_CREATION_DATE),Bytes.toBytes(line[CREATION_DATE]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_SCORE),Bytes.toBytes(line[SCORE]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_VIEW_COUNT),Bytes.toBytes(line[VIEW_COUNT]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_OWNER_USER_ID),Bytes.toBytes(line[OWNER_USER_ID]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_LAST_EDITOR_DISPLAY_NAME),Bytes.toBytes(line[LAST_EDITOR_DISPLAY_NAME]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_LAST_EDIT_DATE),Bytes.toBytes(line[LAST_EDIT_DATE]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_LAST_ACTIVITY_DATE),Bytes.toBytes(line[LAST_ACTIVITY_DATE]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_TAGS),Bytes.toBytes(line[TAGS]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_ANSWER_COUNT),Bytes.toBytes(line[ANSWER_COUNT]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_COMMENT_COUNT),Bytes.toBytes(line[COMMENT_COUNT]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_FAVORITE_COUNT),Bytes.toBytes(line[FAVORITE_COUNT]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_COMMUNITY_OWNED_DATE),Bytes.toBytes(line[COMMUNITY_OWNED_DATE]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_TITLE),Bytes.toBytes(line[TITLE]));
				put.add(Bytes.toBytes(TABLE_FAMILY),Bytes.toBytes(COLUMN_PARENT_ID),Bytes.toBytes(line[PARENT_ID]));
				
				h_table.put(put);
			}
			
		}
		
		public byte[] getRowKey(String[] line){
			String key;
			if(line[POST_TYPE_ID].equals("1")){
				key = line[ACCEPTED_ANS_ID];
			}
			else{
				key = line[POST_ID];
			}
			byte[] b = Bytes.toBytes(key);
			return b;	
		}
		
		public boolean isValid(String[] line){
			boolean b = false;
			if((line[POST_TYPE_ID].equals("1") && !line[ACCEPTED_ANS_ID].equals("")) ||
					(line[POST_TYPE_ID].equals("1") && !line[ACCEPTED_ANS_ID].equals(null)) ||
					(line[POST_TYPE_ID].equals("2") && !line[POST_ID].equals("")) ||
					(line[POST_TYPE_ID].equals("2") && !line[POST_ID].equals(null))
					){
				b=true;
			}
			return b;	
		}
		
		
		protected void cleanup(Context context) throws IOException{
			this.h_table.close();
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		createTable();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: TopKExperts <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Create and Populate HBase table");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(HPopulateMapper.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		System.exit(exitCode);
	}
	
	public static void createTable() throws IOException{
		HBaseConfiguration config = new HBaseConfiguration(new Configuration());
		HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
		htd.addFamily(new HColumnDescriptor(TABLE_FAMILY));
		HBaseAdmin hba = new HBaseAdmin(config);
		//If table already exists delete the table and create again.
		if(hba.tableExists(TABLE_NAME)){
			hba.disableTable(TABLE_NAME);
			hba.deleteTable(TABLE_NAME);
		}
		hba.createTable(htd);
		hba.close();
	}
}
