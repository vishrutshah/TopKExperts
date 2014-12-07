package com.neu.cs6240.ExpertActivityMeter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CountPerTagPerTimeSlotKey implements WritableComparable<CountPerTagPerTimeSlotKey> {
	
	Text tag;
	Text timeSlot;
	
	/**
     * constructor
     */
    public CountPerTagPerTimeSlotKey() {
    	this.tag = new Text();
    	this.timeSlot = new Text();    	
    }
    
    /**
     * constructor
     * @param tag
     * @param timeSlot
     */
	public CountPerTagPerTimeSlotKey(String tag, String timeSlot) {
		this.tag = new Text(tag);
		this.timeSlot = new Text(timeSlot);
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.tag == null)
        	this.tag = new Text();

        if (this.timeSlot == null)
        	this.timeSlot = new Text();
                    
        this.tag.readFields(in);
        this.timeSlot.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.tag.write(out);
    	this.timeSlot.write(out);
	}

	/**
	 * Key Comparator to sort by HashTag first and then by TimeSlot
	 */
	@Override
	public int compareTo(CountPerTagPerTimeSlotKey other) {
		int cmp = tag.compareTo(other.tag);
		if(cmp == 0){
			char a = other.timeSlot.toString().charAt(other.timeSlot.toString().length()-1);			
			int timeSlotOther = Character.getNumericValue(a);
			
			char b = this.timeSlot.toString().charAt(this.timeSlot.toString().length()-1);			
			int timeSlot = Character.getNumericValue(b);			
				
	        return ((timeSlot == timeSlotOther) ? 0 : (timeSlot < timeSlotOther ? -1 : 1));
		}
		return cmp;
	}

}
