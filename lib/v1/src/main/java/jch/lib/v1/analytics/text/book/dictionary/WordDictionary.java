package jch.lib.analytics.text.book.dictionary;

import java.util.ArrayList;
import java.util.TreeMap;

public class WordDictionary {
	public TreeMap<String, ArrayList<WordInstance>> WordMap;
	public ArrayList<WordInstance> Words;
	public WordDictionary() {
		WordMap = new TreeMap<String, ArrayList<WordInstance>>();
		Words = new ArrayList<WordInstance>();
		
		// TODO Auto-generated constructor stub
	}

}
