package jch.lib.analytics.investment.stock;

import java.time.LocalTime;
import java.util.Comparator;

//import jch.lib.analytics.text.StringStatEntryList.StringStatEntry;


/***
 * A doubly linkable record meant to run stats on for stocks
 * @author hydro
 *
 */
public class StatEntry {

	public StatEntry() {
		// TODO Auto-generated constructor stub
	}
	
	public StatEntry(LocalTime time, double price, int volume) {
		this.time = time;
		this.price = price;
		this.volume = volume;
	}
	
	public StatEntry getNextEntry() {
		return nextEntry;
	}
	public void setNextEntry(StatEntry nextEntry) {
		this.nextEntry = nextEntry;
	}
	public StatEntry getPrevEntry() {
		return prevEntry;
	}
	public void setPrevEntry(StatEntry prevEntry) {
		this.prevEntry = prevEntry;
	}
	public String getStockSymbol() {
		return stockSymbol;
	}
	public void setStockSymbol(String stockSymbol) {
		this.stockSymbol = stockSymbol;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public int getVolume() {
		return volume;
	}
	public void setVolume(int volume) {
		this.volume = volume;
	}
	public LocalTime getTime() {
		return time;
	}
	public void setTime(LocalTime time) {
		this.time = time;
	}
	/*
	public long getMilSecDelta() {
		return milSecDelta;
	}
	public void setMilSecDelta(long milSecDelta) {
		this.milSecDelta = milSecDelta;
	}
	*/
	public long getNanoSecDelta() {
		return nanoSecDelta;
	}

	public void setNanoSecDelta(long nanoSecDelta) {
		this.nanoSecDelta = nanoSecDelta;
	}
	public double getWeightedVolumePrice() {
		return weightedVolumePrice;
	}
	public void setWeightedVolumePrice(double weightedVolumePrice) {
		this.weightedVolumePrice = weightedVolumePrice;
	}
	public double getWeightedDurationPrice() {
		return weightedDurationPrice;
	}
	public void setWeightedDurationPrice(double weightedDurationPrice) {
		this.weightedDurationPrice = weightedDurationPrice;
	}
	public double getPriceDelta() {
		return priceDelta;
	}
	public void setPriceDelta(double priceDelta) {
		this.priceDelta = priceDelta;
	}
	public long getVolumeDelta() {
		return volumeDelta;
	}
	public void setVolumeDelta(long volumeDelta) {
		this.volumeDelta = volumeDelta;
	}

	public double calcTimeHour() {
		return this.getTime().getHour() +
		   (this.getTime().getMinute()/ 60.0) + 
		   (this.getTime().getSecond()/ 3600.0) +
		   (this.getTime().getNano()/ (1000000000.0 * 3600.0));
	}

	public double calcTimeMinute() {
		return 
		   (this.getTime().getHour() * 60.0)  +
		   (this.getTime().getMinute()) + 
		   (this.getTime().getSecond()/ 60.0) +
		   (this.getTime().getNano()/ (1000000000.0 * 60.0));
	}

	public double calcTimeSecond() {
		return 
		   (this.getTime().getHour() * 3600.0)  +
		   (this.getTime().getMinute() * 60.0 ) + 
		   (this.getTime().getSecond()) +
		   (this.getTime().getNano()/ (1000000000.0));
	}
	
	public double calcTimeNano() {
		return 
		   (this.getTime().getHour() * 3600.0 * 1000000000.0)  +
		   (this.getTime().getMinute() * 60.0 * 1000000000.0 ) + 
		   (this.getTime().getSecond() * 1000000000.0) +
		   (this.getTime().getNano());
	}
	
	StatEntry nextEntry;
	StatEntry prevEntry;
	String stockSymbol;
	double price;
	int volume;
	LocalTime time;
	//long milSecDelta;
	long nanoSecDelta;


	double priceDelta;
	long volumeDelta;
	double weightedVolumePrice;
	double weightedDurationPrice;
	
	
	
	
	/***
	 * Sort Comparator implementation by Time for an ArrayList of StatEntrys
	 * @author hydro
	 *
	 */
	public class SortByTime implements Comparator<StatEntry> {
		public int compare(StatEntry s1, StatEntry s2) {
			int result = 0;
			result = s1.getTime().compareTo(s2.getTime());
			return result;
		}
	}
	
	/***
	 * Sort Comparator implementation by Price for an ArrayList of StatEntrys
	 * @author hydro
	 *
	 */
	public class SortByPrice implements Comparator<StatEntry> {
		public int compare(StatEntry s1, StatEntry s2) {
			int result = 0;
			if(s1.getPrice() == s2.getPrice()) {result = 0;}
			if(s1.getPrice() < s2.getPrice()) {result = -1;}
			if(s1.getPrice() > s2.getPrice()) {result = 1;}
			
			return result;
		}
	}
	
	/***
	 * Sort Comparator implementation by Volume for an ArrayList of StatEntrys
	 * @author hydro
	 *
	 */
	public class SortByVolume implements Comparator<StatEntry> {
		public int compare(StatEntry s1, StatEntry s2) {
			int result = 0;
			if(s1.getVolume() == s2.getVolume()) {result = 0;}
			if(s1.getVolume() < s2.getVolume()) {result = -1;}
			if(s1.getVolume() > s2.getVolume()) {result = 1;}
			
			return result;
		}
	}
}
