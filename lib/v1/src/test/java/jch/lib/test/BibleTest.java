package jch.lib.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import jch.lib.analytics.text.book.SyntaxDatasetEntry;
import jch.lib.analytics.text.book.bible.Bible;
import jch.lib.analytics.text.book.bible.BibleChapter;
import jch.lib.analytics.text.book.bible.BibleVerse;
import jch.lib.analytics.text.book.bible.KingJamesVersionBible;
import jch.lib.analytics.text.book.dictionary.WebstersUnabridgedDictionary;
import jch.lib.analytics.text.book.dictionary.WordDictionary;
import jch.lib.list.ChunkList;

public class BibleTest {

	public BibleTest() {
		// TODO Auto-generated constructor stub
	}


	/* Text analysis Considerations
	 * 
	 * 
	 * 
	 * ----------------------------------------
	 * Language
	 * 	Dialects
	 * Library
	 * Story
	 * 	Fiction
	 * 	Non-Fiction
	 * 	Reference
	 * 	Dimensions 
	 * Paragraphs
	 * Sentences
	 * 	Structure
	 * Phrases
	 * Compound-Words
	 * Word
	 * 	Parts of Speech
	 * 	Identifiers
	 * Sub-Word
	 * 	Abbreviations
	 * 	Prefixes
	 * 	Suffixes
	 * Letters
	 * Punctuation
	 * 
	 * 
	 * ----------------------------------------
	 * Author
	 * Author Location
	 * Publish Date
	 * 
	 * 
	 * 
	 * 
	 * ----------------------------------------
	 * Dimensions
	 * 
	 * 
	 * 
	 * 
	 */
	
	
	//Start from: *** START OF THIS PROJECT GUTENBERG EBOOK THE KING JAMES BIBLE ***
	//End from: End of the Project Gutenberg EBook of The King James Bible
	public static void kjvT1() {
		String seedLocation = "data/lang/eng/seed/published/project_gutenburg/txt/";
		String seedFile = "pg10.txt";
		String compileLocation = "data/lang/eng/stats/words/";
		
		//File folder = new File(seedLocation);
		//File[] listOfFiles = folder.listFiles();
		
		System.out.println("Loading file: " + seedLocation + seedFile);
		ChunkList kjv = ChunkList.loadFile(seedLocation + seedFile);
		System.out.println("Chunk Count:  " + kjv.getChunkCount());
		
		System.out.println("Cleaning...");
		
		//This current recipe gets the job done
		//I like to show the chunk size reduction to how each method impacts the ChunkList
		System.out.println(kjv.getChunkCount());
		kjv.removeBeforeKey("Start from: *** START OF THIS PROJECT GUTENBERG EBOOK THE KING JAMES BIBLE ***", true);
		System.out.println(kjv.getChunkCount());
		kjv.removeAfterKey("End of the Project Gutenberg EBook of The King James Bible", true);
		System.out.println(kjv.getChunkCount());
		
		
		System.out.println("Generating TreeMap...");
		kjv.moveFirstChunk();
		TreeMap<String, Long> seedsDict = ChunkList.toStringTreeMapDictionary(kjv, getMedDelims());
		
		System.out.println("Printing...");
		
		System.out.println(kjv.toString(kjv.getChunkCount()));
		
		BufferedWriter writer =null;
		try {
			writer = new BufferedWriter(new FileWriter(compileLocation + "kjv_stats.txt"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	kjv.moveFirstChunk();
		System.out.println("Write location: " + compileLocation + "kjv_stats.txt");
		for(Map.Entry<String, Long> entry : seedsDict.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue()); 
			
			try {

				writer.write(entry.getKey() + "\t" + entry.getValue() + "\r\n");
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		}
		
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Done?");
		
	}
	

	public static void biblegrammartest() {
		ArrayList<SyntaxDatasetEntry> kjvGrammar = KingJamesVersionBible.buildGrammar();
		TreeMap<String, Long> tmap = new TreeMap<String, Long>();		
		System.out.println(kjvGrammar.size());
		
		Long cnt;
		String tempVal = "";
		for(SyntaxDatasetEntry syntaxEntry : kjvGrammar) {
			
			//System.out.println(entry.getText() + "\t" + 
		    //                   entry.getMsrPDP());
			
			//get current character ascii value
			tempVal = syntaxEntry.getMsrPDP();
			
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
		
		int lookBack = 5;
		
		for(int i = 0; i < lookBack; i++) {
			
		}	
	}
	
	

	
	public static void bible_dictionary_test1() {
		
		//Integer.parseInt("4.");
		WordDictionary wud = null; 
		try {
			wud = WebstersUnabridgedDictionary.buildDictionary();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Bible kjv = KingJamesVersionBible.buildBible();
		
		System.out.println("Both are loaded. Let's begin.");
		
		for(int a = 0; a < kjv.Testaments.get(0).Books.get(0).Chapters.size();a++) {
			for(int b = 0; b < kjv.Testaments.get(0).Books.get(0).Chapters.get(a).Verses.size(); b++) {
				
				String ver1 = kjv.Testaments.get(0).Books.get(0).Chapters.get(a).Verses.get(b).getCleanRefValue();
				ArrayList<String> tokens = ChunkList.toStringList(
						ChunkList.stringToChunks(ver1), new String[]{",","."," ",";","?","!"});
				for(int i = 0; i < tokens.size() ; i++) {
					String wrd1 = tokens.get(i).trim().toUpperCase();
					System.out.print(wrd1 + "  ");
					
					if( wud.WordMap.get(wrd1) != null ) {
						String wrd2 = wud.WordMap.get(wrd1).get(0).PartsOfSpeech.get(0).getPartOfSpeech();
						System.out.println(wrd2);
					}
					else {
						System.out.println();
					}
				}
			}
		}
	}

	
	public static void runKJV_Search() {
		
		String searchValue;
		Scanner scan;
		KingJamesVersionBible.clKJV.moveFirstChunk();
		Bible bible = KingJamesVersionBible.buildBible();
		
		int len = 0;
		
		
		for(int i = 0; i < bible.Testaments.size() ; i++) {
			for(int j = 0; j < bible.Testaments.get(i).Books.size() ; j++) {
				for(int k = 0; k < bible.Testaments.get(i).Books.get(j).Chapters.size();k++) {
					for(int l = 0; l < bible.Testaments.get(i).Books.get(j).Chapters.get(k).Verses.size(); l++) {

						BibleChapter chapter = bible.Testaments.get(i).Books.get(j).Chapters.get(k);
						BibleVerse verse = bible.Testaments.get(i).Books.get(j).Chapters.get(k).Verses.get(l);
						
						if(verse.getCleanRefValue().length() > len) {
							System.out.print(bible.Testaments.get(i).Books.get(j).getCleanRefValue());
							System.out.println(" - " + chapter.getChapterNumber() + ":" + verse.getVerseNumber() );
							System.out.println(verse.getCleanRefValue());
							len = verse.getCleanRefValue().length();
						}
					}
				}
			}
		}
		
		
		
		for(int t = 0; t < 10 ; t++) {
			
			int cnt = 0;
			searchValue = "";
			System.out.println("Type search value");
			scan=new Scanner(System.in);
			searchValue = scan.nextLine();
			System.out.println("You typed " + searchValue);		
			
			//we do some drilling here
			
			for(int i = 0; i < bible.Testaments.size() ; i++) {
				for(int j = 0; j < bible.Testaments.get(i).Books.size() ; j++) {
					for(int k = 0; k < bible.Testaments.get(i).Books.get(j).Chapters.size();k++) {
						for(int l = 0; l < bible.Testaments.get(i).Books.get(j).Chapters.get(k).Verses.size(); l++) {

							BibleChapter chapter = bible.Testaments.get(i).Books.get(j).Chapters.get(k);
							BibleVerse verse = bible.Testaments.get(i).Books.get(j).Chapters.get(k).Verses.get(l);
							
							if(verse.getCleanRefValue().toUpperCase().indexOf(searchValue.toUpperCase()) > 0) {
								System.out.print(bible.Testaments.get(i).Books.get(j).getCleanRefValue());
								System.out.println(" - " + chapter.getChapterNumber() + ":" + verse.getVerseNumber() );
								System.out.println(verse.getCleanRefValue());
								cnt++;
							}
						}
					}
				}
			}
			System.out.println("Found: " + cnt);
		}

	}
	
	public static void runThroughKJV() {
		Bible bible = KingJamesVersionBible.buildBible();
		for(int i = 0; i < bible.Testaments.size() ; i++) {
			for(int j = 0; j < bible.Testaments.get(i).Books.size() ; j++) {
				for(int k = 0; k < bible.Testaments.get(i).Books.get(j).Chapters.size();k++) {
					for(int l = 0; l < bible.Testaments.get(i).Books.get(j).Chapters.get(k).Verses.size(); l++) {
						BibleChapter chapter = bible.Testaments.get(i).Books.get(j).Chapters.get(k);
						BibleVerse verse = bible.Testaments.get(i).Books.get(j).Chapters.get(k).Verses.get(l);
						
						System.out.print( bible.Testaments.get(i).Books.get(j).getCleanRefValue() + " ");
						System.out.print(chapter.getChapterNumber() + " - " + verse.getVerseNumber() + " : ");
						System.out.println(verse.getCleanRefValue());
						try {
							System.in.read();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		
		/*
		for(int i = 0; i < bible.Testaments.size() ; i++) {
			System.out.print(bible.Testaments.get(i).getRefValue() + "\t");
			System.out.println(bible.Testaments.get(i).getStartContentRef().getIndex() + " - " + 
					           bible.Testaments.get(i).getEndContentRef().getIndex() );
			for(int k = 0; k < bible.Testaments.get(i).Books.size(); k++) {
				System.out.print(bible.Testaments.get(i).Books.get(k).getRefValue()  + "\t");
				System.out.println(bible.Testaments.get(i).Books.get(k).getStartContentRef().getIndex() + " - " + 
				           		   bible.Testaments.get(i).Books.get(k).getEndContentRef().getIndex() );
			}
		}
		*/
	}
	
	
	
	public static void kjvT2() {
		String seedLocation = "data/lang/eng/seed/published/project_gutenburg/txt/";
		String seedFile = "pg10.txt";
		String compileLocation = "data/lang/eng/stats/phrases/";
		
		//File folder = new File(seedLocation);
		//File[] listOfFiles = folder.listFiles();
		
		System.out.println("Loading file: " + seedLocation + seedFile);
		ChunkList kjv = ChunkList.loadFile(seedLocation + seedFile);
		System.out.println("Chunk Count:  " + kjv.getChunkCount());
		
		System.out.println("Cleaning...");
		
		//This current recipe gets the job done
		//I like to show the chunk count reduction to how each method impacts the ChunkList
		System.out.println(kjv.getChunkCount());
		kjv.removeBeforeKey("Start from: *** START OF THIS PROJECT GUTENBERG EBOOK THE KING JAMES BIBLE ***", true);
		System.out.println(kjv.getChunkCount());
		kjv.removeAfterKey("End of the Project Gutenberg EBook of The King James Bible", true);
		System.out.println(kjv.getChunkCount());
		
		//Because we are looking at phrasing, we want more cleaning
		kjv.moveFirstChunk();
		kjv.replaceAll("\r\n", " ");
		System.out.println(kjv.getChunkCount());
		kjv.moveFirstChunk();
		kjv.replaceAll("\n", " ");
		System.out.println(kjv.getChunkCount());
		
		String[] delims = getMedDelims();
		for(int i = 0; i < delims.length ; i++) {
			kjv.moveFirstChunk();
			kjv.replaceAll(delims[i], " ");
			System.out.println(i + " of " + delims.length);
			System.out.println(kjv.getChunkCount());
		}
		System.out.println("replacing spaces");
		kjv.moveFirstChunk();
		kjv.replaceAll("  ", " ");
		System.out.println(kjv.getChunkCount());
		
		System.out.println("Generating TreeMap...");
		kjv.moveFirstChunk();
		TreeMap<String, Long> seedsDict = ChunkList.toStringTreeMapPhrase(kjv, 2, 10, getMedDelims());
		
		System.out.println("Printing...");
		System.out.println(kjv.toString(kjv.getChunkCount())); 
		
		BufferedWriter writer =null;
		try {
			writer = new BufferedWriter(new FileWriter(compileLocation + "kjv_stats.txt"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String prevKey = "";
		Long prevVal = (long) 0;
    	kjv.moveFirstChunk();
		System.out.println("Write location: " + compileLocation + "kjv_stats.txt");
		for(Map.Entry<String, Long> entry : seedsDict.entrySet()) {
			
			try {
				//if(entry.getValue() >= 2) {
					//System.out.println(entry.getKey() + "\t" + entry.getValue()); 
					//writer.write(entry.getKey() + "\t" + entry.getValue() + "\r\n");
					//String sub1 = entry.getKey().substring(1,prevKey.length());
					
					
					if(entry.getKey().contains(prevKey) &&
					   prevVal == entry.getValue()) {
						System.out.println("Now this is what im talking about!!");
					}
					else {
						if(prevVal>= 2) {
							System.out.println(prevKey + "\t" + prevVal); 
							writer.write(prevKey + "\t" + prevVal + "\r\n");
						}
					}
				//}
	
				
				
				prevKey = entry.getKey();
				prevVal = entry.getValue();

			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		}
		
		try {
			System.out.println(prevKey + "\t" + prevVal); 
			writer.write(prevKey + "\t" + prevVal + "\r\n");			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Done?");
		
	}
	
	public static String[] getMedDelims() {
		String[] output = {new String(new char[]{0}),
						   new String(new char[]{1}),
						   new String(new char[]{2}),
						   new String(new char[]{3}),
						   new String(new char[]{4}),
						   new String(new char[]{5}),
						   new String(new char[]{6}),
						   new String(new char[]{7}),
						   new String(new char[]{8}),
						   new String(new char[]{9}),	//horizontal tab
						   new String(new char[]{10}),	//newline
						   new String(new char[]{11}),
						   new String(new char[]{12}),
						   new String(new char[]{13}),	//carriage return
						   new String(new char[]{14}),
						   new String(new char[]{15}),
						   new String(new char[]{16}),
						   new String(new char[]{17}),
						   new String(new char[]{18}),
						   new String(new char[]{19}),
						   new String(new char[]{20}),
						   new String(new char[]{21}),
						   new String(new char[]{22}),
						   new String(new char[]{23}),
						   new String(new char[]{24}),
						   new String(new char[]{25}),
						   new String(new char[]{26}),
						   new String(new char[]{27}),
						   new String(new char[]{28}),
						   new String(new char[]{29}),
						   new String(new char[]{30}),
						   new String(new char[]{31}),
						   new String(new char[]{32}),
			           		"!",
			           	   new String(new char[]{34}),	//Double quote
			           		"#",
			           		"$",
			           		"%",
			           		"&",
			           		new String(new char[]{39}),	//Single Quote
			           		"(",
			           		")",
			           		"*",
			           		"+",
			           		",",
			           		".",
			           		"/",
			           		":",
			           		";",
			           		"<",
			           		"=",
			           		">",
			           		"@",
			           		"[",
			           		new String(new char[]{92}),		//Forward Slash
			           		"]",
			           		"^",
			           		"_",
			           		"`",
			           		"{",
			           		"|",
			           		"}",
			           		"�",
			           		"�",
			           		"�",
			           		"?",
			           		"�",
			           		"�"
						   
		};
		return output;
	}
	
	public static short[] getMedDelimsChar() {
		 short[] output= {0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,				                       					                       		
           		9,	//horizontal tab
				10,	//newline
				13,	//carriage return
                32,	//space
           		'!',
           		34,	//Double quote
           		'#',
           		'$',
           		'%',
           		'&',
           		39,	//Single Quote
           		'(',
           		')',
           		'*',
           		'+',
           		',',
           		'.',
           		'/',
           		':',
           		';',
           		'<',
           		'=',
           		'>',
           		'@',
           		'[',
           		92,		//Forward Slash
           		']',
           		'^',
           		'_',
           		'`',
           		'{',
           		'|',
           		'}'
		 	};
		 return output;
	}
}
