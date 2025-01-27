package jch.lib.analytics.text;

import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
//import java.util.Iterator;
import java.util.Map.Entry;

import jch.lib.analytics.text.StringStatEntryList.StringStatEntry;
import jch.lib.analytics.text.book.SyntaxDatasetEntry;
//import jch.lib.analytics.text.book.bible.BibleSyntaxDatasetEntry;
//import jch.lib.analytics.text.book.bible.KingJamesVersionBible;
import jch.lib.list.ChunkList;
import jch.lib.list.StringChunkLink;
import jch.lib.analytics.text.StringArrayStatEntryList.StringArrayStatEntry;
//import jch.lib.analytics.text.StringStatEntryList.SortByFromCount;


/***
 * 
 * @author harrisonc
 *
 */
public class GhostWriter {
	public ChunkList SeedSource;
	public ArrayList<SyntaxDatasetEntry> SeedSourceGrammar;
	
	public ChunkList Publish;
		
	public TreeMap<Integer, Long> AsciiSet;
	public TreeMap<String, Long> PartsOfSpeechSet;
	//public ArrayList<StatLenEntry> StringStatLen = new ArrayList<StatLenEntry>();
	
	public TreeMap<Integer, StatLenEntry> CharStringStatLen;	//Char
	public TreeMap<Integer, StatArrayLenEntry> PosStringArrayStatLen;	//word
	public StringStatEntryList PosToWordStringArrayStat;
	private int lookback = 0;
	
	public GhostWriter() {
		AsciiSet = new TreeMap<Integer, Long>();
		CharStringStatLen = new TreeMap<Integer, StatLenEntry>();
		SeedSourceGrammar = new ArrayList<SyntaxDatasetEntry>();
		PosStringArrayStatLen = new TreeMap<Integer, StatArrayLenEntry>();
		PosToWordStringArrayStat = new StringStatEntryList();
	}
	
	
	/**
	 * Builds a string of random characters based on the analysis of SeedSource
	 * @param len
	 */
	public void buildCharPublish(long len) {
		
		double roll;
		String nextKey = "";
		String prevKeys = "";
		StringBuilder build = new StringBuilder();
		int t = 0;
		
		
		//looper builds to builds string to argument
		for(long i = 0; i < len; i++) {
			roll = Math.random();
			if(lookback > build.length()) {t = build.length() + 1;}
				else {t = lookback;}			
			
			if(build.length() > 0) {
				prevKeys = build.substring(build.length() - t + 1);
			}

			//looper goes through chance list
			for(int k = 0; k < CharStringStatLen.get(t).CharChanceList.list.size(); k++) {
				
				if(CharStringStatLen.get(t).CharChanceList.get(k).getFrom().compareTo(prevKeys)==0 &&
				   CharStringStatLen.get(t).CharChanceList.get(k).getRoll() >= roll) {
					nextKey = CharStringStatLen.get(t).CharChanceList.get(k).getTo();
					k = CharStringStatLen.get(t).CharChanceList.list.size();
				}
			}
			
			build.append(nextKey);
		}
		
		Publish = ChunkList.stringToChunks(build.toString());
	}
	
	/**
	 * Builds unique mapping of ascii numerals within SeedSource
	 * 
	 */
	public void buildAsciiStats() {
		if(SeedSource != null && SeedSource.getChunkCount() > 0) {
			AsciiSet = new TreeMap<Integer, Long>();
			int tempVal = -1;
			long cnt = 0;
			
			SeedSource.moveFirstChunk();
			
			do {
				//get current character ascii value
				tempVal = (int)(SeedSource.toString(1).charAt(0)); 
				
				if(AsciiSet.get(tempVal) == null) {
					AsciiSet.put(tempVal, (long) 1);
				}
				else { 
					cnt = (long)AsciiSet.get(tempVal);
					AsciiSet.put(tempVal, ++cnt);
				}
				//reset
				tempVal = -1;
			}
			while(SeedSource.moveNextChunk());	
			
		}
	}
	
	/***
	 * Builds single layer of string stats
	 * 
	 * TODO: refactor
	 * @param lookback
	 */
	public void buildStringStats2(int lookback) {
		
		if(SeedSource != null && SeedSource.getChunkCount() > 0) {
			
			this.lookback = lookback;
			String tempVal = null;
			String prevFrom = "";
			long cnt = 0;
			double rt = 0.0;
			
			//start from 1 and work our way up to lookback value
			for(int curLen = 1; curLen - 1 < lookback; curLen++) {
				System.out.println(curLen);
				
				//ins
				TreeMap<String, Long> tmap = new TreeMap<String, Long>();
				StringStatEntryList   list = new StringStatEntryList();
				StatLenEntry          lenEntry = new StatLenEntry();
		
				SeedSource.moveFirstChunk();
				
				//scan through file with given length				
				cnt = 0;
				rt = 0.0;
				do {//will short-circuit when .moveNextChunk() hits NULL
					//get current character ascii value
					tempVal = SeedSource.toString(curLen).toUpperCase();
					
					if(tempVal.length() == curLen ) {
						//if not exist, insert here
						if(tmap.get(tempVal) == null) {
							tmap.put(tempVal, (long) 1);
						}
						//otherwise, add 1 more to its count
						else { 
							cnt = (long)tmap.get(tempVal);
							tmap.put(tempVal, ++cnt);
						}
					}
					
					//reset
					tempVal = null;
				} while(SeedSource.moveNextChunk());
				
				
				//Load treemap entries in a list
				for(Entry<String, Long> entry : tmap.entrySet()) {
					String from = null;
					String to = null;
					
					//we cant split a string where len == 1
					if(curLen == 1) {
						from = entry.getKey();
						to = "";
					}
					else if(curLen > 1) {
						from = entry.getKey().substring(0,curLen-1);
						to = entry.getKey().substring(curLen-1);
					}
					
					//add entry to list
                    //             'abcd'         'abc' 'd'	  250
					list.addEntry(entry.getKey(), from, to, entry.getValue());
				}
				
				//sort entries from least common to most common
				Collections.sort(list.list, new StringStatEntryList().new SortByFromCount());
				
				//calc odds
				rt = 0.0;
				prevFrom = "";
				for(StringStatEntry stringStatEntry : list.list) {
					
					//
					double chance = 0.0;
					if(curLen == 1) {
						//calcs odds for base
						chance = (double)stringStatEntry.getCount()/(double)SeedSource.getChunkCount();
					}
					else if(curLen > 1) {
						//grabs from len - 1
						chance = 
								(double)stringStatEntry.getCount()/
							    (double)CharStringStatLen.get(curLen - 1).CharSet.get(stringStatEntry.getFrom());
					}
					stringStatEntry.setChance(chance);
					
					//restart running total where len greater than 1 and "from" differs from prevFrom
					if(curLen > 1 && prevFrom.compareTo(stringStatEntry.getFrom()) != 0) 
						rt = 0.0;
					rt = stringStatEntry.getChance() + rt;
					stringStatEntry.setRoll(rt);
					
					//grab "from" value to be compares as prevFrom for next iteration
					prevFrom = stringStatEntry.getFrom();

				}
				

				
				lenEntry.setLen(curLen);
				lenEntry.CharSet = tmap;
				lenEntry.CharChanceList = list;
				
				CharStringStatLen.put(curLen, lenEntry);
				
				//Trim horizontally
				
				//Trim vertically
				if(curLen > 1) CharStringStatLen.remove(curLen - 1);
			}
		}
	}

	/**
	 * Builds char statistics in layers from 1 to lookback
	 *  
	 * 
	 * @param lookback
	 */
	public void buildStringStats(int lookback) {

		if(SeedSource != null && SeedSource.getChunkCount() > 0) {
			this.lookback = lookback;
			
			String tempVal = null;
			String prevFrom = "";
			long cnt = 0;
			double rt = 0.0;
			
			//CharStringStatLen = new ArrayList<StatLenEntry>();
			/*
			 * Len 1 sets up the baseline
			 * After that each iteration depends on previous integration for math
			 * 2 depends on 1
			 * 3 depends on 2
			 * etc
			 * 
			 * Len = 1 to X
			 */
			for(int curLen = 1; curLen - 1 < lookback; curLen++) {
				System.out.println(curLen);
				
				TreeMap<String, Long> tmap = new TreeMap<String, Long>();
				StringStatEntryList list = new StringStatEntryList();
				StatLenEntry lenEntry = new StatLenEntry();
				
				SeedSource.moveFirstChunk();
				
				//2 sections: length = 1 and length > 1
				//Section 1: length = 1
				if(curLen == 1) {
					cnt = 0;
					rt = 0.0;
					do {
						//get current character ascii value
						tempVal = SeedSource.toString(1);
						
						//if not exist, insert here
						if(tmap.get(tempVal) == null) {
							tmap.put(tempVal, (long) 1);
						}
						//otherwise, add 1 more to its count
						else { 
							cnt = (long)tmap.get(tempVal);
							tmap.put(tempVal, ++cnt);
						}
						//reset
						tempVal = null;
					}
					///will short when .moveNextChunk() hits NULL
					while(SeedSource.moveNextChunk());	
					
					//
					for(Entry<String, Long> entry : tmap.entrySet()) {
						list.addEntry(
								entry.getKey(), 
								"", 
								entry.getKey(), 
								entry.getValue());
					}
					
					//
					for(StringStatEntry stringStatEntry : list.list) {
						stringStatEntry.setChance(
							(double)stringStatEntry.getCount()/(double)SeedSource.getChunkCount());
						rt = stringStatEntry.getChance() + rt;
						stringStatEntry.setRoll(rt);
						
					}
				}
				
				//section 2: now we hit length > than 1
				else if (curLen > 1) {
					cnt = 0;
					rt = 0.0;
					
					do {
						tempVal = SeedSource.toString(curLen);
						
						if(tempVal.length() == curLen ) {
							if(tmap.get(tempVal) == null) {
								tmap.put(tempVal, (long) 1);
							}
							else { 
								cnt = (long)tmap.get(tempVal);
								tmap.put(tempVal, ++cnt);
							}
						}
						tempVal = null;
					}
					while(SeedSource.moveNextChunk());	
					
					for(Entry<String, Long> entry : tmap.entrySet()) {
						/*
						System.out.print(entry.getKey());
						System.out.print("\t");
						System.out.println(entry.getValue());
						*/
						
						list.addEntry(
								entry.getKey(), 
								entry.getKey().substring(0,curLen-1), 
								entry.getKey().substring(curLen-1), 
								entry.getValue());
					}
					
					Collections.sort(list.list, new StringStatEntryList().new SortByFromCount());
					
					rt = 0.0;
					prevFrom = "";
					for(StringStatEntry stringStatEntry : list.list) {
						
						if(prevFrom.compareTo(stringStatEntry.getFrom()) != 0) 
							rt = 0.0;
						
						stringStatEntry.setChance(
								(double)stringStatEntry.getCount()/
							    (double)CharStringStatLen.get(curLen - 1).CharSet.get(stringStatEntry.getFrom()));
						rt = stringStatEntry.getChance() + rt;
						stringStatEntry.setRoll(rt);
						
						prevFrom = stringStatEntry.getFrom();
						/*
						System.out.print(stringStatEntry.getKey());
						System.out.print("\t"); 
						System.out.print(stringStatEntry.getFrom());
						System.out.print("\t");
						System.out.print(stringStatEntry.getTo());
						System.out.print("\t");
						System.out.print(stringStatEntry.getChance());
						System.out.print("\t");
						System.out.println(stringStatEntry.getRoll());
						*/
					}
				}
				
				lenEntry.setLen(curLen);
				lenEntry.CharSet = tmap;
				lenEntry.CharChanceList = list;
				
				CharStringStatLen.put(curLen, lenEntry);
			}
		}
	}
	
	

	/***
	 * Calculates max repeatable set of characters for a given
	 * SeedSource.  Attempts to be light on the cpu, but who knows.
	 * @return
	 */
	public long calcMaxLengthString2() {
		long output = 0;
		
		//make sure SeedSource is populated and has content
		if(SeedSource != null && SeedSource.getChunkCount() > 0) {
			StringBuilder lenSmpl = null;
			StringBuilder lenTest = null;
			
			//increasing lengths to test
			for(long curLen = 1; curLen < SeedSource.getChunkCount(); curLen++) {
				
				//flag for repeat found
				Boolean fnd = false;
				
				//Sample loop
				for(long smplPos = 1; smplPos - curLen < SeedSource.getChunkCount(); smplPos++) {
					SeedSource.moveFirstChunk();   //jumps instead of moves (saves time/compute)
					//SeedSource.setPosition(smpl);  //moves to position
					lenSmpl = new StringBuilder(SeedSource.toString(curLen));
					
					System.out.println(curLen + ": " +lenSmpl);
					
					//Compare Loop
					for(long cmpr = smplPos + 1; cmpr - curLen < SeedSource.getChunkCount(); cmpr++  ) {
						//SeedSource.setPosition(cmpr);  //moves to position
						SeedSource.moveNextChunk();
						lenTest = new StringBuilder(SeedSource.toString(curLen));
						
						if(cmpr % 10000000 == 0) System.out.println(cmpr);
						
						if(lenSmpl.compareTo(lenTest) == 0) {
							//repeat found; short circuit loops
							smplPos = Long.MAX_VALUE;
							cmpr = Long.MAX_VALUE;
							fnd = true;
							System.out.print("boom!");
							
						}	
					}
				}//smpl
				
				if(fnd == false) {
					//didnt find repeat at curLen; report prev max length
					output = curLen - 1;
					curLen = Long.MAX_VALUE;
				}
				
			}//curlen
		}
		return output;
	}

	
	public long calcMaxLengthString() {
		System.out.println("Start");
		LenCmp lc = new LenCmp();
		lc.cl = this.SeedSource;
		lc.smplPos = (StringChunkLink) this.SeedSource.getFirstChunk();
		lc.maxLen = 1;
		
		lc.cl.moveFirstChunk();
		for(long i = lc.maxLen; i < lc.cl.getChunkCount(); i++) {
			
			System.out.println("i: " + i);
			while(lc.compareMode1() == 0 && lc.smplPos != lc.cl.getLastChunk()) {
				lc.smplPos = (StringChunkLink)lc.smplPos.getNextChunk();
				System.out.println("ml: " + lc.smplPos.getIndex() + "," + lc.maxLen + " " + lc.smpl);
 			}
			
			System.out.println("jump modes maxLen : " + lc.maxLen);
			lc.compareMode2();
			System.out.println("compareMode2() done!");
		}

		return 0;
	}
	
	
	/***
	 * Helper class to find max length repeatable string
	 * @author harrisonc
	 *
	 */
	class LenCmp {
		public ChunkList cl = null;
		public StringBuilder smpl = null;
		public StringBuilder cmpr = null;
		public long maxLen = -1;
		public StringChunkLink smplPos = null;
		public StringChunkLink cmprPos = null;
		
		
		public long compareMode1() {
			long output = -1;
			cl.setCurrentChunk(smplPos);
			cl.setRelativeOffset(maxLen);
			cmprPos = (StringChunkLink) cl.getCurrentChunk();
			
			smpl = new StringBuilder(smplPos.toString(maxLen));
			cmpr = new StringBuilder(cmprPos.toString(maxLen));
			
			while(smpl.compareTo(cmpr) != 0 && cmpr.length() == maxLen) {
				//System.out.println("m1: " + smpl + " v " + cmpr + " ~ " + cmprPos.getIndex());
				cmprPos = (StringChunkLink) cmprPos.getNextChunk();
				cmpr = new StringBuilder(cmprPos.toString(maxLen));
			}
			if(smpl.compareTo(cmpr) == 0) {
				System.out.println("m1 compare!: " + smpl + " v " + cmpr + " ~ " + cmprPos.getIndex() + "," + smplPos.getIndex() +  "," + maxLen + "  <<==(match)");
				output = cmprPos.getIndex();
				//maxLen++;
			}
			else output = 0;
			
			return output;
		}
		
		public long compareMode2() {
			long output = -1;
			smpl = new StringBuilder(smplPos.toString(maxLen));
			cmpr = new StringBuilder(cmprPos.toString(maxLen));
			
			System.out.println("m2: " + smpl + " v " + cmpr + " ~ " + cmprPos.getIndex() + "," + smplPos.getIndex() +  "," + maxLen);
			
			while(smpl.compareTo(cmpr) == 0) {
				System.out.println("m2: up! "+ smpl + " v " + cmpr + " ~ " + cmprPos.getIndex() + "," + smplPos.getIndex() +  "," + maxLen);
				maxLen++;
				smpl = new StringBuilder(smplPos.toString(maxLen));
				cmpr = new StringBuilder(cmprPos.toString(maxLen));
			}
			
			System.out.println("m2: b4 maxLen: " + smpl + " v " + cmpr + " ~ " + cmprPos.getIndex() + "," + smplPos.getIndex() +  "," + maxLen);
			smpl = new StringBuilder(smplPos.toString(maxLen-1));
			cmpr = new StringBuilder(cmprPos.toString(maxLen-1));
			
			if(smpl.compareTo(cmpr) == 0) {
				//maxLen--;
				output = maxLen - 1;
				
				System.out.println("m2: comp!!! maxLen: "+ smpl + " v " + cmpr + " ~ " + cmprPos.getIndex() + "," + smplPos.getIndex() +  "," + maxLen);
				//smplPos = (StringChunkLink) cl.getFirstChunk();
				cl.moveFirstChunk();
			}
			else output = 0;
			
			return output;
		}
	}
	

	
	/**
	 * 
	 * @param lookBack
	 */
	public void buildPartsOfSpeechStats(int lookBack) {

		ArrayList<String> gs = new ArrayList<String>();
		
		for(SyntaxDatasetEntry entry : SeedSourceGrammar) {
			gs.add(entry.getMsrPDP());
		}
		
		//ArrayList<String> tempKey;
		//ArrayList<String> tempFrom;
		
		String[] key;
		
		@SuppressWarnings("unused")
		String[] from;
		String[] prevFrom;
		
		@SuppressWarnings("unused")
		String to;
		long cnt;
		double rt = 0.0;
	
		for(int curLen = 0; curLen < lookBack; curLen++) {
			TreeMap<String[], Long> tmap = new TreeMap<>(Arrays::compare);
			StringArrayStatEntryList list = new StringArrayStatEntryList();
			StatArrayLenEntry lenEntry = new StatArrayLenEntry();
			
			key = null;
			from = null;
			to = null;
			cnt = 0;
			
			for(int k = 0; k < gs.size() - curLen;k++) {
				key = gs.subList(k, k + curLen + 1).toArray(new String[0]);
				
				if(key.length == curLen + 1) {
					if(tmap.get(key) == null) {
						tmap.put(key, (long) 1);
					}
					else {
						cnt = (long)tmap.get(key);
						tmap.put(key, ++cnt);
					}
				}
			}
			
			for(Entry<String[], Long> entry: tmap.entrySet()) {
				
				if(curLen == 0) {
					list.addEntry(
						entry.getKey(),
						(String[]) null,
						entry.getKey()[curLen],
						entry.getValue() );
				}
				else {
					list.addEntry(
						entry.getKey(),
						Arrays.copyOfRange(entry.getKey(),0,curLen),
						entry.getKey()[curLen],
						entry.getValue() );
				}
				
				/*
				for(int j = 0; j < entry.getKey().length; j++) {
					System.out.print(entry.getKey()[j] + " ");
				}
				*/
				//System.out.println(entry.getValue());
			}
			//System.out.println(tmap.size());
			
			//Let's sort it out
			Collections.sort(list.list, new StringArrayStatEntryList().new SortByFromCount());
			
			rt = 0.0;
			prevFrom = null;
			
			//now calculate both chances and roll stats
			for(StringArrayStatEntry stringStatEntry : list.list) {
				
				if(Arrays.equals(prevFrom, stringStatEntry.getFrom()) == false) 
					rt = 0.0;
				
				if(curLen == 0) {
					
					stringStatEntry.setChance(
							(double)stringStatEntry.getCount()/
							(double)SeedSourceGrammar.size());
				}
				else {
					
					stringStatEntry.setChance(
							(double)stringStatEntry.getCount()/
 						    (double)PosStringArrayStatLen.get(curLen - 1).StringSet.get(stringStatEntry.getFrom()));
				}
				
				rt = stringStatEntry.getChance() + rt;
				stringStatEntry.setRoll(rt);
				
				prevFrom = stringStatEntry.getFrom();
				
				/*
				System.out.print(stringStatEntry.getKey());
				System.out.print("\t"); 
				System.out.print(stringStatEntry.getFrom());
				System.out.print("\t");
				*/
				
				if(stringStatEntry.getCount() > 1) {
					if(stringStatEntry.getFrom() != null) {
						for(int b = 0; b < stringStatEntry.getFrom().length; b++) {
							System.out.print(stringStatEntry.getFrom()[b] + " ");
						}
					}
					
					System.out.print(stringStatEntry.getTo());
					System.out.print("\t");
					System.out.print(stringStatEntry.getCount());
					System.out.print("\t");
					System.out.print(stringStatEntry.getChance());
					System.out.print("\t");
					System.out.println(stringStatEntry.getRoll());
					
				}
			}
			
			lenEntry.setLen(curLen);
			lenEntry.StringSet = tmap;
			lenEntry.StringChanceList = list;
			
			PosStringArrayStatLen.put(curLen, lenEntry);
		}
	}
	
	public void buildPosToWordStats() {
		//parts of speach to words
		
		//how ugly would a double tree map be?
		TreeMap<String, Long> pt = new TreeMap<String, Long>();
		TreeMap<String, TreeMap<String, Long>> pts = new TreeMap<String, TreeMap<String, Long>>();
		//StringStatEntryList posToWord = new StringStatEntryList();
		
		
		long cnt = 0;
		String prevFrom = "";
		double rt = 0.0;
		for(SyntaxDatasetEntry entry : SeedSourceGrammar) {
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
				/*
				System.out.print(posEntry.getKey() + " - ");
				System.out.print(wordEntry.getKey() + " : ");
				System.out.print(wordEntry.getValue() + " out of ");
				System.out.println(pt.get(posEntry.getKey()));
				*/
				
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
			/*
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
			*/
		}
		
		//System.out.println(posToWordStats.list.size());
		posToWordStats = PosToWordStringArrayStat;
	}
	 
	/**
	 * Keeps track of unique string mappings and basic stats of each combination
	 * of "From" lookback characters and a single "To" character.
	 * 
	 * CharChanceList
	 * For example: cat
	 * Len =	3
	 * Key = 	cat
	 * From = 	ca
	 * To = 	t
	 * 
	 *  From  To Count
	 *  ca -> t     23
	 *  ca -> u     28
	 *  etc...
	 * @author James Chad Harrison
	 *
	 */
	public class StatLenEntry {
		int len;
		
		public int getLen() {
			return len;
		}
		
		public void setLen(int len) {
			this.len = len;
		}
		
		public TreeMap<String, Long> CharSet;
		public StringStatEntryList CharChanceList;
	}
	
	
	/**
	 * Keeps track unique combinations of strings
	 * @author hydro
	 *
	 */
	public class StatArrayLenEntry {
		int len;
		
		public int getLen() {
			return len;
		}
		 
		public void setLen(int len) {
			this.len = len;
		}
		
		public TreeMap<String[], Long> StringSet;
		public StringArrayStatEntryList StringChanceList;
	}
	
}
