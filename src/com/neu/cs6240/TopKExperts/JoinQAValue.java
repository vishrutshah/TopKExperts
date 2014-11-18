package com.neu.cs6240.TopKExperts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * JoinQAHelper implements the WritableComparable
 * This class contains useful functions used for keyComparator and 
 * Grouping comparator
 */
public class JoinQAValue implements Writable {
	Text hashTags;
	Text userId;

    /**
     * constructor
     */
    public JoinQAValue() {
    	this.hashTags = new Text();
    	this.userId = new Text();
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

	public JoinQAValue(String userId, String hashTags) {
    	this.hashTags = new Text(hashTags);
    	this.userId = new Text(userId);
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
