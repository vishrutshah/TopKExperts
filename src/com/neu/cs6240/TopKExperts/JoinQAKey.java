package com.neu.cs6240.TopKExperts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * JoinQAHelper implements the WritableComparable
 * This class contains useful functions used for keyComparator and 
 * Grouping comparator
 */
public class JoinQAKey implements WritableComparable {
	Text postId;
	Text flag;

    /**
     * constructor
     */
    public JoinQAKey() {
    	this.flag = new Text();
    	this.postId = new Text();    	
    }
  
   

	public JoinQAKey(String postId, String flag) {
		this.flag = new Text(flag);
		this.postId = new Text(postId);
    }
    
	/**
	 * @return the postId
	 */
	public String getPostId() {
		return postId.toString();
	}



	/**
	 * @return the flag
	 */
	public String getFlag() {
		return flag.toString();
	}
  
    /**
     * overrider the write method to support write operation
     */
    public void write(DataOutput out) throws IOException {
    	this.flag.write(out);
    	this.postId.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {
            if (this.flag == null)
            	this.flag = new Text();

            if (this.postId == null)
            	this.postId = new Text();
                        
            this.flag.readFields(in);
            this.postId.readFields(in);
    }



	@Override
	public int compareTo(Object object) {
		JoinQAKey ip2 = (JoinQAKey) object;
        int cmp = ip2.getPostId().compareTo(getPostId());
        if (cmp != 0) {
        	return cmp;
        }
        
        return ip2.getFlag().compareTo(getFlag());
	}

}
