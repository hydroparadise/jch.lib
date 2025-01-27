package jch.lib.analytics.text;

import java.util.ArrayList;
import java.util.Comparator;
//import java.util.TreeMap;



public class StringStatEntryList {
	public ArrayList<StringStatEntry> list;
	
	public StringStatEntryList(){
		list = new ArrayList<StringStatEntry>();
	}
	
	public void addEntry(String key, String from, String to, long count) {
		StringStatEntry newEntry = new StringStatEntry(key, from, to, count);
		list.add(newEntry);
	}
	
	public StringStatEntry get(int index) {
		return list.get(index);
	}
	
	public class StringStatEntry {
		
		public String getFrom() {
			return from;
		}
		
		public void setFrom(String from) {
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
		
		String key;
		public String getKey() {
			return key;
		}
		
		public void setKey(String key) {
			this.key = key;
		}
		
		public StringStatEntry(){
			key = null;
			from = null;
			to = null;
			count = 0;
			chance = 0;
			roll = 0;
		}
		
		StringStatEntry(String key, String from, String to, long count){
			this.key = key;
			this.from = from;
			this.to = to;
			this.count = count;
		}
		
		String from;
		String to;
		long count;
		double chance;
		double roll;
	}
	
	public class SortByFromCount implements Comparator<StringStatEntry> {
		public int compare(StringStatEntry s1, StringStatEntry s2) {
			int result = 0;
			if(s1.getFrom().compareTo(s2.getFrom()) == 0) {
				if(s1.getCount() == s2.getCount()) {result = 0;}
				if(s1.getCount() < s2.getCount()) {result = -1;}
				if(s1.getCount() > s2.getCount()) {result = 1;}
				
			}
			else {
				result = s1.getFrom().compareTo(s2.getFrom());
			}
			return result;
		}
	}

}
