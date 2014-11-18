package com.neu.cs6240.TopKExperts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * JoinQAValue implements the Writable
 */
public class JoinQAValue implements Writable {
	// List of the hash tags for the post
	Text hashTags;
	// User who has correctly answered the question
	Text userId;

    /**
     * constructor
     */
    public JoinQAValue() {
    	this.hashTags = new Text();
    	this.userId = new Text();
    }
    /**
     * constructor
     * @param userId
     * @param hashTags
     */
	public JoinQAValue(String userId, String hashTags) {
    	this.hashTags = new Text(hashTags);
    	this.userId = new Text(userId);
    }
	
	/**
	 * @return the hashTags
	 */
	public Text getHashTags() {
		return hashTags;
	}

	/**
	 * @return the userId
	 */
	public Text getUserId() {
		return userId;
	}
  
    /**
     * overrider the write method to support write operation
     */
    public void write(DataOutput out) throws IOException {
    	this.hashTags.write(out);
    	this.userId.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {

            if (this.hashTags == null)
            	this.hashTags = new Text();
            
            if (this.userId == null)
            	this.userId = new Text();

            this.hashTags.readFields(in);
            this.userId.readFields(in);
    }
}
