package com.neu.cs6240.TopKExpertsHBase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * JoinQAKey implements the WritableComparable
 * This class contains useful functions used for keyComparator 
 */
public class HComputeKey implements WritableComparable {
	// Post Id
	Text userId;
	// flag representing whether this is a "Question[Q] / Answer[A]"
	Text hashTag;

    /**
     * constructor
     */
    public HComputeKey() {
    	this.userId = new Text();
    	this.hashTag = new Text();    	
    }
    
    /**
     * constructor
     * @param postId
     * @param flag
     */
	public HComputeKey(String userId, String hashTag) {
		this.userId = new Text(userId);
		this.hashTag = new Text(hashTag);
    }
    
	/**
	 * @return the postId
	 */
	public Text getUserId() {
		return userId;
	}

	/**
	 * @return the flag
	 */
	public Text getHashTag() {
		return hashTag;
	}
  
    /**
     * overrider the write method to support write operation
     */
    public void write(DataOutput out) throws IOException {
    	this.userId.write(out);
    	this.hashTag.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {
            if (this.userId == null)
            	this.userId = new Text();

            if (this.hashTag == null)
            	this.hashTag = new Text();
                        
            this.userId.readFields(in);
            this.hashTag.readFields(in);
    }
    
    /**
     * First sort by Post Id and then by flag
     */
	@Override
	public int compareTo(Object object) {
		HComputeKey ip2 = (HComputeKey) object;
		int cmp = this.getHashTag().compareTo(ip2.getHashTag());
		
		if(cmp != 0){
			return cmp;
		}
		
		int user2 = Integer.parseInt(ip2.getUserId().toString());
		int user1 = Integer.parseInt(getUserId().toString());		
        cmp = (user1 == user2) ? 0 : (user1 < user2 ? -1 : 1);
        return cmp;
	}

}
