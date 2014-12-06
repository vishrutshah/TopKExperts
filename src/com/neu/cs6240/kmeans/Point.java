package com.neu.cs6240.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable{
	
	public static final DecimalFormat twoDForm = new DecimalFormat("#.##");
	
	// average response to a correct answer for hash tag
	Text avgResponseTime = null;
	// popularity index for this hash tag
	Text popularity = null;
	// Hash Tag if its not centroid point
	Text hashTag = null;

	public Point(){
		
	}
	
	public Point(Text line){
		String[] centerData = line.toString().split(",");
		if(centerData.length == 2){
			this.avgResponseTime = new Text(centerData[0]);
			this.popularity = new Text(centerData[1]);
		}
	}
	
	public Point(String avgResponseTime, String popularity) throws NumberFormatException{
		this.avgResponseTime = new Text(avgResponseTime); //Double.parseDouble(avgResponseTime);
		this.popularity = new Text(popularity); //Double.parseDouble(popularity);
	}
	
	public Point(double avgResponseTime, double popularity){
		this.avgResponseTime = new Text(String.valueOf(avgResponseTime));
		this.popularity = new Text(String.valueOf(popularity));
	}
	
	public void setHashTag(String hashTag){
		 this.hashTag = new Text(hashTag);
	}
	
	public Text getHashTag(){
		return this.hashTag;
	}
	
	public Text getAvgResponseTime(){
		return this.avgResponseTime;
	}
	
	public double getAvgResponseTimeValue(){
		Double d = Double.parseDouble(this.avgResponseTime.toString());
		return Double.valueOf(twoDForm.format(d.doubleValue()));
	}
	
	public Text getPopularity(){
		return this.popularity;
	}
	
	public double getPopularityValue(){
		Double d = Double.parseDouble(this.popularity.toString());
		return Double.valueOf(twoDForm.format(d.doubleValue()));
	}
	
	public double euclidian(Point point2){
		double distance  = 0.0;
		
		distance = Math.sqrt(
				Math.pow(this.getAvgResponseTimeValue() - point2.getAvgResponseTimeValue(), 2.0) + 
				Math.pow(this.getPopularityValue() - point2.getPopularityValue(), 2.0)
				);
		
		return distance;
	}
	
    /**
     * overrider the write method to support write operation
     */
    public void write(DataOutput out) throws IOException {
    	this.avgResponseTime.write(out);
    	this.popularity.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {
            if (this.avgResponseTime == null)
            	this.avgResponseTime = new Text();

            if (this.popularity == null)
            	this.popularity = new Text();
                        
            this.avgResponseTime.readFields(in);
            this.popularity.readFields(in);
    }
 
	@Override
	public int compareTo(Object object) {
		
		int ip = this.hashCode();
		int ip2 = object.hashCode();
		
		int cmp = (ip == ip2) ? 0 : (ip < ip2 ? -1 : 1);
		
		return cmp;

	}
	
	@Override
	public String toString() {
		return getAvgResponseTimeValue() + "," + getPopularityValue();
	}
}
