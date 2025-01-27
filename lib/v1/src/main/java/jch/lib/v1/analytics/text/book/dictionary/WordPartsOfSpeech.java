package jch.lib.analytics.text.book.dictionary;

import java.util.Comparator;

//import jch.lib.analytics.text.StringStatEntryList.StringStatEntry;

public class WordPartsOfSpeech {
	
	public int getOrderNumber() {
		return orderNumber;
	}


	public void setOrderNumber(int orderNumber) {
		this.orderNumber = orderNumber;
	}


	public String getAbrev() {
		return abrev;
	}


	public void setAbrev(String abrev) {
		this.abrev = abrev;
	}


	public String getPartOfSpeech() {
		return partOfSpeech;
	}


	public void setPartOfSpeech(String partOfSpeech) {
		this.partOfSpeech = partOfSpeech;
	}


	int orderNumber;
	String abrev;
	String partOfSpeech;
	
	
	public WordPartsOfSpeech() {
		// TODO Auto-generated constructor stub
	}

	public class SortByOrderNumber implements Comparator<WordPartsOfSpeech> {
		@Override
		public int compare(WordPartsOfSpeech s1, WordPartsOfSpeech s2) {
			int result = 0;
	
				if(s1.getOrderNumber() == s2.getOrderNumber()) {result = 0;}
				if(s1.getOrderNumber() < s2.getOrderNumber()) {result = -1;}
				if(s1.getOrderNumber() > s2.getOrderNumber()) {result = 1;}

			return result;
		}
	}
	 
}
