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
	// Following parameter are useful for package
	// com.neu.cs6240.AvgTimeToAnswer
	// Question Id if this post is a question otherwise empty
	Text questionId;
	// Creation Date for this post whether its a question / answer
	Text creationDate;
	

    /**
     * constructor
     */
    public JoinQAValue() {
    	this.hashTags = new Text();
    	this.userId = new Text();
    	this.questionId = new Text();
    	this.creationDate = new Text();
    }
    /**
     * constructor
     * @param userId
     * @param hashTags
     * @param questionId
     * @param creationDate
     */
	public JoinQAValue(String userId, String hashTags, String questionId, String creationDate) {
    	this.hashTags = new Text(hashTags);
    	this.userId = new Text(userId);
    	this.questionId = new Text(questionId);
    	this.creationDate = new Text(creationDate);
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
	 * @return the questionId
	 */
	public Text getQuestionId() {
		return questionId;
	}
	/**
	 * @return the creationDate
	 */
	public Text getCreationDate() {
		return creationDate;
	}
	
    /**
     * overrider the write method to support write operation
     */
    public void write(DataOutput out) throws IOException {
    	this.hashTags.write(out);
    	this.userId.write(out);
    	this.questionId.write(out);
    	this.creationDate.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {

            if (this.hashTags == null)
            	this.hashTags = new Text();
            
            if (this.userId == null)
            	this.userId = new Text();
            
            if (this.questionId == null)
            	this.questionId = new Text();
            
            if (this.creationDate == null)
            	this.creationDate = new Text();

            this.hashTags.readFields(in);
            this.userId.readFields(in);
            this.questionId.readFields(in);
            this.creationDate.readFields(in);
    }
}
