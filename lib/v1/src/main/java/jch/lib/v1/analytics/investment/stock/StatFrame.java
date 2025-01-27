package jch.lib.analytics.investment.stock;
import static java.time.temporal.ChronoUnit.*;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;

//import jch.lib.analytics.text.StringStatEntryList;
//import jch.lib.analytics.text.StringStatEntryList.SortByFromCount;


/***
 * 
 * @author hydro
 *
 *	Conventions used:
 *		get - retrieves value in memory
 *		set - stores value in memory
 *		calc - calculates and returns value
 *		prep - pre-processes dataset
 */

public class StatFrame {

	public StatFrame() {
		// TODO Auto-generated constructor stub
	}
	
	/***
	 * Adds and links statistical entries together
	 * @param newEntry
	 * @return
	 */
	public boolean addEntry(StatEntry newEntry) {
		boolean success = true;
		
		//First Entry
		if(firstEntry == null) {
			firstEntry = newEntry;
			lastEntry = newEntry;
		}
		else {
			if (firstEntry == lastEntry) {
				firstEntry.setNextEntry(newEntry);
				lastEntry.setPrevEntry(firstEntry);
			}
			else {
				lastEntry.setNextEntry(newEntry);	
				newEntry.setPrevEntry(lastEntry);
			}
			lastEntry = newEntry;
		}
		
		return success;
	}
	
	
	/***
	 * Divides current StatFrame into equal parts specified by input value
	 * divideBy, and then outputs to a StatFrameSet.  For example, if divideBy is 
	 * set to 3, then a StatFrameSet will contain 3 StatFrame's.  This method
	 * also fills in missing values
	 * 
	 *                 Base Frame
	 *  |--------------------------------------|
	 *  |*                            *        |
	 *  |         *                            |
	 *  |      *                            *  |               
	 *  |--------------------------------------|
	 * x1                                     x2
	 *  
	 *                Divide by 3
	 *      
	 *      Frame 1      Frame2       Frame 3
	 *  |------------|------------|------------|    
	 *  | *          |            |   *        |      
	 *  |          * |o           |o           |
	 *  |       *    |            |         *  |
	 *  |------------|------------|------------|
	 *             *->o----------->o
	 * If value is missing in  frame, last entry from previous frame becomes
	 * first entry of that frame.  
	 * 
	 * Frame 1 has 3 entries, frame 2 has 0 entries
	 * Frame 2 receives the last entry  
	 *
	 * @param divideBy
	 * @return
	 */
	public StatFrameSet divideByTime(long divideBy) {
		StatFrameSet statSet = new StatFrameSet();
		StatFrame newFrame = new StatFrame();
		
		//make sure frame is ordered by time
		this.orderByTime();
		
		//Get starting and end reference on timeline in seconds
		double x1 = this.getFirstEntry().calcTimeSecond();
		double x2 = this.getLastEntry().calcTimeSecond();
		
		//calculate time delta
		double td = (x2 - x1) / divideBy;
		
		//keeps track of frame iteration
		int i = 1;
		
		//iterate through linked list.  grab first link, then move to next link
		//until the end of list
		StatEntry curEntry = this.getFirstEntry();
		while(curEntry != null &&
			  curEntry != lastEntry.getNextEntry()) {
			
			//if entry falls between the two points in time, add to current frame
			if(curEntry.calcTimeSecond() >= x1 + (td * (i - 1)) &&
			   curEntry.calcTimeSecond() <  x1 + (td * i)) {
				newFrame.addEntry(curEntry);
				curEntry = curEntry.getNextEntry();
			}
			//else, adjust the time window and create new frame, and add last entry from
			//previously encountered frame as first entry.
			else {
				LocalTime lt = this.getFirstEntry().getTime();
				lt = lt.plusSeconds((long) td*i);
				//System.out.println(lt.toString() + "\t" + (td * i));
				statSet.addEntry(newFrame);
				newFrame = new StatFrame();
				newFrame.addEntry(new StatEntry(lt, 
						                        curEntry.getPrevEntry().getPrice(),  
						                        curEntry.getPrevEntry().getVolume()));
				i++;
			}
		}
		return statSet;
	}
	
	/***               _
	 * wxSum = Wi(Xi - X*)^2     
	 *        
	 *         (M-1)
	 * nzWts = -----
	 *           M       
	 * @return
	 */
	public double calcWtdStdDevVolDurPrice() {
		double output = 0;
		//double nzWts = 0;  //nonzero weights
		double wxSum = 0;
		double wSum = 0;
		double wt = 0;
		double wAvg = this.calcWtdStdDevVolDurPrice();
		//long eCnt = this.calcEntryCount();
		StatEntry curEntry = firstEntry;
		while(curEntry != null  &&
			  curEntry != lastEntry.getNextEntry()) {
			//wt = curEntry.getVolume() * curEntry.getMilSecDelta();
			wt = curEntry.getVolume() * curEntry.getNanoSecDelta();
			if(wt > 0) wt++;
			wSum = wSum + wt;
			wxSum = wxSum * (wt * Math.pow(curEntry.getPrice() - wAvg, 2));
			curEntry = curEntry.getNextEntry();
		}
		output = Math.sqrt(wxSum / ((wt - 1) / wt) * wSum);
		return output;
	}
	
	/***
	 * Weighted Average Volume Price
	 * @return
	 */
	public double calcWtdAvgVolPrice () {
		double output = 0;
		double wxSum = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null  &&
		      curEntry != lastEntry.getNextEntry()) {
			wxSum = wxSum + (curEntry.getVolume() * curEntry.getPrice());
			curEntry = curEntry.getNextEntry();
		}
		output = wxSum / this.calcSumVolume();
		return output;
	}
	
	
	/***
	 * Weighted Average Duration (Time) Price
	 * @return
	 */
	public double calcWtdAvgDurPrice () {
		double output = 0;
		double wxSum = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry!= null &&
			  curEntry != lastEntry.getNextEntry()) {
			wxSum = wxSum + (curEntry.getNanoSecDelta() * curEntry.getPrice());
			curEntry = curEntry.getNextEntry();
		}
		output = wxSum / this.calcSumNanoSec();
		return output;
	}
	
	
	/***
	 * Weighted Average Volume/Duration Price
	 * @return
	 */
	public double calcWtdAvgVolDurPrice () {
		double output = 0;
		double wxSum = 0;
		double wSum = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null  &&
			  curEntry != lastEntry.getNextEntry()) {
			wxSum = wxSum + ((double)curEntry.getNanoSecDelta() * 
					         (double)curEntry.getVolume() * 
					         curEntry.getPrice());
			wSum = wSum + ((double)curEntry.getNanoSecDelta() * 
			         	  (double)curEntry.getVolume());
			
			curEntry = curEntry.getNextEntry();
		}
		output = wxSum / wSum;;
		return output;
	}
	
	/***
	 * 
	 * @return
	 */
	public double calcStdDevPrice() {
		double output = 0;
		double avgPrice = this.calcAvgPrice();
		long entryCount = this.calcEntryCount();
		double sumDev = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null  &&
			  curEntry != lastEntry.getNextEntry()) {
			sumDev = sumDev + Math.pow(curEntry.getPrice() - avgPrice, 2);
			curEntry = curEntry.getNextEntry();
		}
		output = sumDev / (double) entryCount;
		return output;
	}
	
	/***
	 * 
	 * @return
	 */
	public double calcAvgPrice() {
		double output = 0;
		output = calcSumPrice() / 
				(double) calcEntryCount();
		return output;
	}
	
	
	/***
	 * 
	 * @return
	 */
	public double calcMaxPrice() {
		double output = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null  &&
			  curEntry != lastEntry.getNextEntry()) {
			if(curEntry == firstEntry) output = curEntry.getPrice();
			else if (output < curEntry.getPrice()) output = curEntry.getPrice();
			curEntry = curEntry.getNextEntry();
		}
		return output;
	}
	
	/***
	 * 
	 * @return
	 */
	public double calcMinPrice() {
		double output = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null  &&
			  curEntry != lastEntry.getNextEntry()) {
			if(curEntry == firstEntry) output = curEntry.getPrice();
			else if (output > curEntry.getPrice()) output = curEntry.getPrice();
			curEntry = curEntry.getNextEntry();
		}
		return output;
	}
	
	/***
	 * 
	 * @return
	 */
	public double calcSumPrice() {
		double output = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null  &&
			  curEntry != lastEntry.getNextEntry()) {
			output = output + curEntry.getPrice();
			curEntry = curEntry.getNextEntry();
		}
		return output;
	}
	
	/***
	 * 
	 * @return
	 */
	/*
	public long calcSumMilSec() {
		long output = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null) {
			output = output + curEntry.getMilSecDelta();
			curEntry = curEntry.getNextEntry();
		}
		return output;
	}
	*/
	public long calcSumNanoSec() {
		long output = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null &&
			  curEntry != lastEntry.getNextEntry()) {
			output = output + curEntry.getNanoSecDelta();
			curEntry = curEntry.getNextEntry();
		}
		return output;
	}
	
	/***
	 * 
	 * @return
	 */
	public long calcSumVolume() {
		long output = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null &&
			  curEntry != lastEntry.getNextEntry()) {
			output = output + curEntry.getVolume();
			curEntry = curEntry.getNextEntry();
		}
		return output;
	}
	
	/***
	 * 
	 * @return
	 */
	public long calcEntryCount() {
		long output = 0;
		StatEntry curEntry = firstEntry;
		while(curEntry != null &&
			  curEntry != lastEntry.getNextEntry()) {
			output++;
			curEntry = curEntry.getNextEntry();
		}
		return output;
	}
	
	/***
	 * 
	 */
	public void prepTimeDeltas() {
		StatEntry curEntry = firstEntry;
		do {
			curEntry.setNanoSecDelta(
					NANOS.between(curEntry.getTime(), 
								   curEntry.getNextEntry().getTime()));
			curEntry = curEntry.getNextEntry();
		}
		while(curEntry.nextEntry != null);

	}
	
	/***
	 * 
	 */
	public void prepPriceDeltas() {
		StatEntry curEntry = firstEntry;
		do {
			
			curEntry = curEntry.getNextEntry();
		}
		while(curEntry.nextEntry != null);

	}
	
	/***
	 * 
	 */
	public void orderByPrice() {
		ArrayList<StatEntry> entryList = this.toStatEntryArrayList();
		if(entryList != null) {
			Collections.sort(entryList, new StatEntry().new SortByPrice());
			for(long i = 0; i < entryList.size() - 1; i++) {
				entryList.get((int) i).setNextEntry(entryList.get((int) i + 1));
				entryList.get((int) i + 1).setPrevEntry(entryList.get((int) i));
				entryList.get((int) i + 1).setNextEntry(null);
			}
		}	
	}
	
	public void orderByTime() {
		ArrayList<StatEntry> entryList = this.toStatEntryArrayList();
		if(entryList != null) {
			Collections.sort(entryList, new StatEntry().new SortByTime());
			for(long i = 0; i < entryList.size() - 1; i++) {
				entryList.get((int) i).setNextEntry(entryList.get((int) i + 1));
				entryList.get((int) i + 1).setPrevEntry(entryList.get((int) i));
				entryList.get((int) i + 1).setNextEntry(null);
			}
		}	
	}
	
	public void orderByVolume() {
		ArrayList<StatEntry> entryList = this.toStatEntryArrayList();
		if(entryList != null) {
			Collections.sort(entryList, new StatEntry().new SortByVolume());
			for(long i = 0; i < entryList.size() - 1; i++) {
				entryList.get((int) i).setNextEntry(entryList.get((int) i + 1));
				entryList.get((int) i + 1).setPrevEntry(entryList.get((int) i));
				entryList.get((int) i + 1).setNextEntry(null);
			}
		}	
	}
	
	
	/***
	 * Converts and creates the doubly linked list into an ArrayList of StatEntrys
	 * @return
	 */
	public ArrayList<StatEntry> toStatEntryArrayList () {
		ArrayList<StatEntry> output = null;
		if(this.calcEntryCount() > 0) {
			output = new ArrayList<StatEntry>();
			StatEntry curEntry = this.getFirstEntry();
			while(curEntry != null &&
				  curEntry != lastEntry.getNextEntry()) {
				output.add(curEntry);
				curEntry = curEntry.getNextEntry();
			}
		}
		return output;
	}
	
	public StatEntry getFirstEntry() {
		return firstEntry;
	}
	public StatEntry getLastEntry() {
		return lastEntry;
	}
	
	public StatFrame getNextFrame() {
		return nextFrame;
	}

	public void setNextFrame(StatFrame nextFrame) {
		this.nextFrame = nextFrame;
	}

	public StatFrame getPrevFrame() {
		return prevFrame;
	}

	public void setPrevFrame(StatFrame prevFrame) {
		this.prevFrame = prevFrame;
	}

	StatFrame nextFrame = null;
	StatFrame prevFrame = null;
	
	StatEntry firstEntry = null;
	StatEntry lastEntry = null;

}
