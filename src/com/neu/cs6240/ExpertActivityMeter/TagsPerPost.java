package com.neu.cs6240.ExpertActivityMeter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import au.com.bytecode.opencsv.CSVParser;

import com.neu.cs6240.TopKExperts.TopKPerHashTagKey;

public class TagsPerPost {
	public static class TagsPerPostMapper extends
	Mapper<Object, Text, TagsPerPostKey, Text> {


		BloomFilter expertsBloomFilter;
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
			try {
				 expertsBloomFilter = new BloomFilter();
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
					// For each record in the user file
					while ((line = rdr.readLine()) != null) {
						String experts = line.split("    ")[1];
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
			String[] parsedData = this.csvParser.parseLine(line.toString());

		}
	}
}
