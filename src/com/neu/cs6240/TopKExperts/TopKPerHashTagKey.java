package com.neu.cs6240.TopKExperts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * TopKPerHashTagKey implements the WritableComparable
 * This class contains useful functions used for keyComparator 
 */
public class TopKPerHashTagKey implements WritableComparable {
	// Hash Tag
	Text hashTag;
	// total correct answers given by per user for this hash tag
	IntWritable ansCountPerUserId;

    /**
     * constructor
     */
    public TopKPerHashTagKey() {
    	this.hashTag = new Text();
    	this.ansCountPerUserId = new IntWritable();    	
    }
    
    /**
     * constructor
     * @param hashTag
     * @param ansCountPerUserId
     */
	public TopKPerHashTagKey(String hashTag, int ansCountPerUserId) {
		this.hashTag = new Text(hashTag);
		this.ansCountPerUserId = new IntWritable(ansCountPerUserId);
    }
    
	/**
	 * @return the hashTag
	 */
	public Text getHashTag() {
		return hashTag;
	}

	/**
	 * @return the ansCountPerUserId
	 */
	public IntWritable getAnsCountPerUserId() {
		return ansCountPerUserId;
	}
  
    /**
     * overrider the write method to support write operation
     */
    public void write(DataOutput out) throws IOException {
    	this.hashTag.write(out);
    	this.ansCountPerUserId.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {
            if (this.hashTag == null)
            	this.hashTag = new Text();

            if (this.ansCountPerUserId == null)
            	this.ansCountPerUserId = new IntWritable();
                        
            this.hashTag.readFields(in);
            this.ansCountPerUserId.readFields(in);
    }
    
    /**
     * First sort by hashTag and then by ansCountPerUserId
     */
	@Override
	public int compareTo(Object object) {
		TopKPerHashTagKey ip2 = (TopKPerHashTagKey) object;
		int cmp = getHashTag().compareTo(ip2.getHashTag());
        if (cmp != 0) {
        	return cmp;
        }        
        // Sort by ansCountPerUserId in non-increasing order
        int userId2 = Integer.parseInt(ip2.getAnsCountPerUserId().toString());
		int userId = Integer.parseInt(getAnsCountPerUserId().toString());		
        return -1 * ((userId == userId2) ? 0 : (userId < userId2 ? -1 : 1));
	}
}
