package com.neu.cs6240.ExpertActivityMeter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TagsPerPostValue implements Writable {
		// List of the hash tags for the post
		Text hashTags;
		// TimeSlot the post belongs to
		Text timeSlot;

	    /**
	     * constructor
	     */
	    public TagsPerPostValue() {
	    	this.hashTags = new Text();
	    	this.timeSlot = new Text();
	    }
	    /**
	     * constructor
	     * @param hashTags
	     * @param timeSlot
	     */
		public TagsPerPostValue(String hashTags, String timeSlot) {
	    	this.hashTags = new Text(hashTags);
	    	this.timeSlot = new Text(timeSlot);
	    }
		
		/**
		 * @return the hashTags
		 */
		public Text getHashTags() {
			return hashTags;
		}

		/**
		 * @return the timeSlot
		 */
		public Text getTimeSlot() {
			return timeSlot;
		}
		
	    /**
	     * overrider the write method to support write operation
	     */
	    public void write(DataOutput out) throws IOException {
	    	this.hashTags.write(out);
	    	this.timeSlot.write(out);
	    }

	    /**
	     * overrider readField method to support reading fields 
	     */
	    public void readFields(DataInput in) throws IOException {

	            if (this.hashTags == null)
	            	this.hashTags = new Text();
	            
	            if (this.timeSlot == null)
	            	this.timeSlot = new Text();

	            this.hashTags.readFields(in);
	            this.timeSlot.readFields(in);
	    }
}
