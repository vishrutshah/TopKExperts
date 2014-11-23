package com.neu.cs6240.ExpertActivityMeter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.neu.cs6240.TopKExperts.JoinQAKey;
import com.neu.cs6240.TopKExperts.TopKPerHashTagKey;

public class TagsPerPostKey implements WritableComparable {

		// Post Id
		Text postId;
		// flag representing whether this is a "Question[Q] / Answer[A]"
		Text flag;

	    /**
	     * constructor
	     */
	    public TagsPerPostKey() {
	    	this.flag = new Text();
	    	this.postId = new Text();    	
	    }
	    
	    /**
	     * constructor
	     * @param postId
	     * @param flag
	     */
		public TagsPerPostKey(String postId, String flag) {
			this.flag = new Text(flag);
			this.postId = new Text(postId);
	    }
	    
		/**
		 * @return the postId
		 */
		public Text getPostId() {
			return postId;
		}

		/**
		 * @return the flag
		 */
		public Text getFlag() {
			return flag;
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
	    
	    /**
	     * First sort by Post Id and then by flag
	     */
		@Override
		public int compareTo(Object object) {
			JoinQAKey ip2 = (JoinQAKey) object;
			int postId2 = Integer.parseInt(ip2.getPostId().toString());
			int postId = Integer.parseInt(getPostId().toString());		
	        int cmp = (postId == postId2) ? 0 : (postId < postId2 ? -1 : 1);
	        if (cmp != 0) {
	        	return cmp;
	        }        
	        // - because Q must come before A
	        return - getFlag().compareTo(ip2.getFlag());
		}
}
