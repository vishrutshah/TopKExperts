package com.neu.cs6240.kmeans;

import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import au.com.bytecode.opencsv.CSVParser;

public class TagAverageResponseTime {	
	public static String CENTROID_FILE_NAME = "/centroid/centroid.txt";
	public static String OUTPUT_FILE_NAME = "/part-00000";
	public static String DATA_FILE_NAME = "/part";
	public static String SPLITTER = ",";
	public static List<Point> centers = new ArrayList<Point>();

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Point, Point> {
		
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');
		
		@Override
		public void configure(JobConf job) {
			try {
				// Get centroid from Distributed cache
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					centers.clear();					
					BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
					try {
						while ((line = br.readLine()) != null) {
							String[] columns = line.split(SPLITTER);
							Point point; 
							point = new Point(columns[0], columns[1]);
							centers.add(point);
						}
					} finally {
						br.close();
					}
				}
			} catch (IOException e) {
				System.err.println("IO Exception - Distributed cache: " + e.getMessage());
			}
		}

		/*
		 * Map function will find the minimum center of the point and emit it to
		 * the reducer
		 */
		@Override
		public void map(LongWritable key, Text line,
				OutputCollector<Point, Point> output,
				Reporter reporter) throws IOException {
			
			// Parse the input line
			String[] parsedData = null;
			try {
				parsedData = this.csvParser.parseLine(line.toString());
			} catch (Exception e) {
				// In case of bad data record ignore them
				return;
			}
			
			if(parsedData.length != 3){
				return;
			}
			
			Point p1 = new Point(parsedData[1], parsedData[2]);
			p1.setHashTag(parsedData[0]);
			
			double min1 = Double.MAX_VALUE;
			double min2 = Double.MAX_VALUE;						 
			Point nearestCenter = centers.get(0);
			
			// Find the minimum center from a point
			for (Point p : centers) {
				min1 = p.euclidian(p1);
				min2 = p.euclidian(nearestCenter);				
				if (Math.abs(min1) < Math.abs(min2)) {
					nearestCenter = p;
					min2 = min1;
				}
			}
			
			// Emit the nearest center and the point
			output.collect(nearestCenter,p1);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Point, Point, Text, Text> {
		@Override
		public void reduce(Point key, Iterator<Point> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			double newX = 0.0;
			double newY = 0.0;
			int total = 0;
			Text hashTag = null;
			
			while(values.hasNext()){
				Point value = values.next();
				double x = value.getAvgResponseTimeValue();
				double y = value.getPopularityValue();
				hashTag = value.getHashTag();
				newX = newX + x;
				newY = newY + y;
				++total;
			}
			
			newX = (newX / total);
			newY = (newY / total);
			StringBuilder op = new StringBuilder();
			op.append(String.valueOf(newX)).append(",").append(String.valueOf(newY));
			
			// Emit new center and point
			output.collect(hashTag, new Text(op.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		initKMeans(args);
	}

	public static void initKMeans(String[] args) throws Exception {
		
		String inputFile = "";
		String outputFile = "";
		
		if (args.length != 2) {
			System.err.println("Usage: TagAverageResponseTime <in> <out>");
			System.exit(2);
		}
		
		inputFile = args[0];
		outputFile = args[1];
		
		String input = inputFile;
		String output = outputFile + System.nanoTime();
		String new_input = output;

		// Number of iteration performed
		int iteration = 0;
		boolean isdone = false;
		
		while (! isdone || iteration < 2) {
			JobConf conf = new JobConf(TagAverageResponseTime.class);
			conf.set("mapred.textoutputformat.separator", ",");
			if (iteration == 0) {
				// upload the file to hdfs. Overwrite any existing copy.
				Path hdfsPath = new Path(input + CENTROID_FILE_NAME);				
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			} else {
				// upload the file to hdfs. Overwrite any existing copy.				
				Path hdfsPath = new Path(new_input + OUTPUT_FILE_NAME);
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}

			conf.setJobName("KMeans - TagAverageResponseTime");
			conf.setMapOutputKeyClass(Point.class);
			conf.setMapOutputValueClass(Point.class);
			
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Map.class);
			
			conf.setReducerClass(Reduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf,
					new Path(input + DATA_FILE_NAME));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			JobClient.runJob(conf);

			Path ofile = new Path(output + OUTPUT_FILE_NAME);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
			
			List<Point> centers_next = new ArrayList<Point>();
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split(",");
				double c = Double.parseDouble(sp[0]);
				Point p = new Point(sp[0], sp[1]);
				centers_next.add(p);
				line = br.readLine();
			}
			br.close();

			String prev;
			if (iteration == 0) {
				prev = input + CENTROID_FILE_NAME;
			} else {
				prev = new_input + OUTPUT_FILE_NAME;
			}
			Path prevfile = new Path(prev);
			FileSystem fs1 = FileSystem.get(new Configuration());
			BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(prevfile)));
			List<Point> centers_prev = new ArrayList<Point>();
			String l = br1.readLine();
			while (l != null) {
				String[] sp1 = l.split(SPLITTER);
				Point p = new Point(sp1[0], sp1[1]);				
				centers_prev.add(p);
				l = br1.readLine();
			}
			br1.close();

			// Sort the old centroid and new centroid and check for convergence
			// condition
			Collections.sort(centers_next);
			Collections.sort(centers_prev);

			Iterator<Point> it = centers_prev.iterator();
			for (Point d : centers_next) {
				Point temp = it.next();
				if (Math.abs(temp.euclidian(d)) <= 0.1) {
					isdone = true;
				} else {
					isdone = false;
					break;
				}
			}
			++iteration;
			new_input = output;
			output = outputFile + System.nanoTime();
		}
	}
}
