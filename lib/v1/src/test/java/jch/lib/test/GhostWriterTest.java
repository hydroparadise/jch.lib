package jch.lib.test;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import jch.lib.analytics.text.GhostWriter;
import jch.lib.analytics.text.StringStatEntryList;
import jch.lib.analytics.text.GhostWriter.StatLenEntry;
import jch.lib.analytics.text.StringStatEntryList.SortByFromCount;
import jch.lib.analytics.text.StringStatEntryList.StringStatEntry;
import jch.lib.analytics.text.book.SyntaxDatasetEntry;
import jch.lib.analytics.text.book.bible.KingJamesVersionBible;

public class GhostWriterTest {

	public GhostWriterTest() {
		// TODO Auto-generated constructor stub
	}
	
	
	static void ghostWriter_test2() {
		GhostWriter gw = new GhostWriter();
		gw.SeedSource = KingJamesVersionBible.clKJV;
		gw.SeedSourceGrammar = KingJamesVersionBible.alKJVGrammar;
		gw.buildPartsOfSpeechStats(30);
	}
	

	static void ghostWriter_test3() {
		//parts of speach to words
		
		//how ugly would a double tree map be?
		TreeMap<String, Long> pt = new TreeMap<String, Long>();
		TreeMap<String, TreeMap<String, Long>> pts = new TreeMap<String, TreeMap<String, Long>>();
		StringStatEntryList posToWord = new StringStatEntryList();
		
		
		long cnt = 0;
		String prevFrom = "";
		double rt = 0.0;
		for(SyntaxDatasetEntry entry : KingJamesVersionBible.alKJVGrammar) {
			//System.out.print(entry.getText() + " - ");
			//System.out.print(entry.getMsrPDP() + " ");
			
			if(pts.get(entry.getMsrPDP()) == null){
				pt.put(entry.getMsrPDP(), (long) 0);
				pts.put(entry.getMsrPDP(), new TreeMap<String, Long>());
			} 
			
			if(pts.get(entry.getMsrPDP()).get(entry.getText()) == null) {
				pts.get(entry.getMsrPDP()).put(entry.getText(), (long) 0);
			}
			
			cnt = pt.get(entry.getMsrPDP());
			pt.put(entry.getMsrPDP(), ++cnt);
			
			cnt = pts.get(entry.getMsrPDP()).get(entry.getText());
			pts.get(entry.getMsrPDP()).put(entry.getText(), ++cnt);
			//System.out.print(pts.get(entry.getMsrPDP()).get(entry.getText()) + " out of ");
			//System.out.println(pt.get(entry.getMsrPDP()));
		}
		
		StringStatEntryList posToWordStats = new StringStatEntryList();
		
		for(Entry<String, TreeMap<String, Long>> posEntry : pts.entrySet()) {
			for(Entry<String, Long> wordEntry : posEntry.getValue().entrySet()) {
				System.out.print(posEntry.getKey() + " - ");
				System.out.print(wordEntry.getKey() + " : ");
				System.out.print(wordEntry.getValue() + " out of ");
				System.out.println(pt.get(posEntry.getKey()));
				
				StringStatEntryList.StringStatEntry ssEntry = new StringStatEntryList().new StringStatEntry();
				ssEntry.setFrom(posEntry.getKey());
				ssEntry.setTo(wordEntry.getKey());
				ssEntry.setCount(wordEntry.getValue());
				ssEntry.setChance((double)wordEntry.getValue()/(double)pt.get(posEntry.getKey()));
				
				posToWordStats.list.add(ssEntry);
			}
		}
		Collections.sort(posToWordStats.list, new StringStatEntryList().new SortByFromCount());

		rt = 0.0;
		prevFrom = "";		
		for(StringStatEntry entry : posToWordStats.list) {
			if(prevFrom.compareTo(entry.getFrom()) != 0) 
				rt = 0.0;
			rt = entry.getChance() + rt;
			entry.setRoll(rt);
			prevFrom = entry.getFrom();
			
			System.out.print(entry.getKey());
			System.out.print("\t"); 
			System.out.print(entry.getFrom());
			System.out.print("\t");
			System.out.print(entry.getTo());
			System.out.print("\t");
			System.out.print(entry.getCount());
			System.out.print("\t");
			System.out.print(entry.getChance());
			System.out.print("\t");
			System.out.println(entry.getRoll());
		}
		
		System.out.println(posToWordStats.list.size());
	}

	
	
	void ghostWriter_test() {
		//KJV.sayHi();
		GhostWriter gw = new GhostWriter();
		gw.SeedSource = KingJamesVersionBible.clKJV;
		
		System.out.println(gw.SeedSource.getChunkCount());
		
		//1-3 Reealy wacky stuff
		//4-6 Kinda word like but still weird
		//7-9 We're forming real words, but no logical order
		//10+ actual resemblance of verses start to appear
		//24 = 13BG Ram Needed
		gw.buildStringStats(24);
		
		System.out.println("Stats Built");
		System.out.println(gw.CharStringStatLen.size());
		
		for(Map.Entry<Integer, StatLenEntry>  statLen : gw.CharStringStatLen.entrySet()) {
			System.out.print(statLen.getKey());
			System.out.print("\t");
			System.out.println(statLen.getValue().CharChanceList.list.size());
		}
		
		gw.buildCharPublish(500);
		
		System.out.println(gw.Publish.getChunkCount());
		gw.Publish.moveFirstChunk();
		System.out.println(gw.Publish.toString(500));
	}
	
	
}
