package com.neu.cs6240.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KMeansTimePopularityKey implements WritableComparable{
	IntWritable mapNumber;
	Text center;
	
	public KMeansTimePopularityKey(){
		mapNumber = new IntWritable(0);
		center = new Text();
	}
	
	public IntWritable getMapNumber() {
		return mapNumber;
	}

	public void setMapNumber(IntWritable mapNumber) {
		this.mapNumber = mapNumber;
	}

	public Text getCenter() {
		return center;
	}

	public void setCenter(Text center) {
		this.center = center;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.mapNumber == null)
        	this.mapNumber = new IntWritable();

        if (this.center == null)
        	this.center = new Text();
                    
        this.mapNumber.readFields(in);
        this.center.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.mapNumber.write(out);
    	this.center.write(out);		
	}
	@Override
	public int compareTo(Object arg0) {
		KMeansTimePopularityKey ip2 = (KMeansTimePopularityKey)arg0;
		int cmp = this.mapNumber.compareTo(ip2.getMapNumber());
		
		return (cmp == 0) ? (this.center.compareTo(ip2.getCenter())) : cmp;
	}
	
	@Override
	public String toString() {
		return center.toString() + "," + mapNumber.toString();
	}

}
