package jch.lib.analytics.text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

//import jch.lib.analytics.text.StringStatEntryList.StringStatEntry;

public class StringArrayStatEntryList {
	public ArrayList<StringArrayStatEntry> list;
	
	/***
	 * 
	 * @param key - 
	 * @param from
	 * @param to
	 * @param count
	 */
	public void addEntry(String[] key, String[] from, String to, Long count) {
		StringArrayStatEntry newEntry = new StringArrayStatEntry(key, from, to, count);
		list.add(newEntry);
	}
	
	public StringArrayStatEntryList() {
		list = new ArrayList<StringArrayStatEntry>(); 
		// TODO Auto-generated constructor stub
	}
	
	public class StringArrayStatEntry {
		
		public String[] getFrom() {
			return from;
		}
		
		public void setFrom(String[] from) {
			this.from = from;
		}
		
		public String getTo() {
			return to;
		}
		
		public void setTo(String to) {
			this.to = to;
		}
		
		public long getCount() {
			return count;
		}
		
		public void setCount(long count) {
			this.count = count;
		}
		
		public double getChance() {
			return chance;
		}
		
		public void setChance(double chance) {
			this.chance = chance;
		}
		
		public double getRoll() {
			return roll;
		}
		
		public void setRoll(double roll) {
			this.roll = roll;
		}
		
		
		public String[] getKey() {
			return key;
		}
		
		public void setKey(String[] key) {
			this.key = key;
		}
		
		StringArrayStatEntry(){
			key = null;
			from = null;
			to = null;
			count = 0;
			chance = 0;
			roll = 0;
		}
		
		StringArrayStatEntry(String key[], String[] from, String to, Long count){
			this.key = key;
			this.from = from;
			this.to = to;
			this.count = count;
		}
		
		String[] key;
		String[] from;
		String to;
		long count;
		double chance;
		double roll;
	}
	
	public class SortByFromCount implements Comparator<StringArrayStatEntry> {
		public int compare(StringArrayStatEntry s1, StringArrayStatEntry s2) {
			int result = 0;
			if(Arrays.equals(s1.getFrom(), s2.getFrom())) {
				if(s1.getCount() == s2.getCount()) {result = 0;}
				if(s1.getCount() < s2.getCount()) {result = -1;}
				if(s1.getCount() > s2.getCount()) {result = 1;}		
			}
			else {
				result = Arrays.compare(s1.getFrom(), s2.getFrom());
			}
			return result;
		}
	}

}
