package com.neu.cs6240.ExpertActivityMeter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

public class TimeSlotClassifier {
	
	private static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	private static final SimpleDateFormat formatter = new SimpleDateFormat(
			DATE_PATTERN);
	private HashMap<Integer, Integer> slotMap = new HashMap<Integer, Integer>(24);
	
	public TimeSlotClassifier(int noOfSlots) {
		int slotCounter = 0;
		int currentSlot = 0;
		for (int i=0; i<24; i++ ) {
			slotMap.put(i, currentSlot);
			slotCounter++;
			if(slotCounter == 24/noOfSlots) {
				slotCounter = 0;
				currentSlot++;
			}
		}
	}
	
	public int getTimeSlot(String time) throws ParseException {
		Date d = formatter.parse(time);
		return slotMap.get(d.getHours());
	}
	
	private HashMap<Integer, Integer> getSlotMap() {
		return slotMap;
	}
	
//	public static void main(String args[]) throws ParseException {
//		TimeSlot ts = new TimeSlot(4);
//		for(Entry<Integer, Integer> e : ts.getSlotMap().entrySet()) {
//			System.out.println(e.getKey() + "," + e.getValue());
//		}
//		System.out.println(ts.getTimeSlot("2008-08-01T00:59:33.643"));
//		System.out.println(ts.getTimeSlot("2008-08-01T07:59:33.643"));
//		System.out.println(ts.getTimeSlot("2008-08-01T13:59:33.643"));
//		System.out.println(ts.getTimeSlot("2008-08-01T19:59:33.643"));
//		
//	}

	
}
