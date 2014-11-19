package com.neu.cs6240.TopKExperts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * UserAnswerCountPerHashTagKey implements the WritableComparable
 * This class contains useful functions used for keyComparator 
 */
public class UserAnswerCountPerHashTagKey implements WritableComparable {
	// Hash Tag
	Text hashTag;
	// User Id who has given correct answer on this hash tag
	Text userId;

    /**
     * constructor
     */
    public UserAnswerCountPerHashTagKey() {
    	this.hashTag = new Text();
    	this.userId = new Text();    	
    }
    
    /**
     * constructor
     * @param hashTag
     * @param userId
     */
	public UserAnswerCountPerHashTagKey(String hashTag, String userId) {
		this.hashTag = new Text(hashTag);
		this.userId = new Text(userId);
    }
    
	/**
	 * @return the hashTag
	 */
	public Text getHashTag() {
		return hashTag;
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
    	this.hashTag.write(out);
    	this.userId.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {
            if (this.hashTag == null)
            	this.hashTag = new Text();

            if (this.userId == null)
            	this.userId = new Text();
                        
            this.hashTag.readFields(in);
            this.userId.readFields(in);
    }
    
    /**
     * First sort by hashTag and then by userId
     */
	@Override
	public int compareTo(Object object) {
		UserAnswerCountPerHashTagKey ip2 = (UserAnswerCountPerHashTagKey) object;
		int cmp = getHashTag().compareTo(ip2.getHashTag());
        if (cmp != 0) {
        	return cmp;
        }        
        // Sort by user Id
        int userId2 = Integer.parseInt(ip2.getUserId().toString());
		int userId = Integer.parseInt(getUserId().toString());		
        return (userId == userId2) ? 0 : (userId < userId2 ? -1 : 1);
	}

}
