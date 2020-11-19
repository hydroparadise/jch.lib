package jch.lib.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import jch.lib.analytics.investment.nyse.xdp.TaqMsg;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgFactory;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType003;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType034;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType100;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType101;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType102;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType103;
import jch.lib.analytics.investment.stock.StockDay;
import jch.lib.analytics.text.GhostWriter;
import jch.lib.analytics.text.StringStatEntryList;
import jch.lib.analytics.text.GhostWriter.StatLenEntry;
import jch.lib.analytics.text.StringStatEntryList.SortByFromCount;
import jch.lib.analytics.text.StringStatEntryList.StringStatEntry;
import jch.lib.analytics.text.book.SyntaxDatasetEntry;
import jch.lib.analytics.text.book.bible.Bible;
import jch.lib.analytics.text.book.bible.BibleChapter;
import jch.lib.analytics.text.book.bible.BibleSyntaxDatasetEntry;
import jch.lib.analytics.text.book.bible.BibleVerse;
import jch.lib.analytics.text.book.bible.KingJamesVersionBible;
import jch.lib.analytics.text.book.dictionary.WebstersUnabridgedDictionary;
import jch.lib.analytics.text.book.dictionary.WordDictionary;
import jch.lib.common.HttpWorker;
import jch.lib.common.ReadWorker;
import jch.lib.common.chunk.ChunkList;
import jch.lib.common.chunk.StringChunkLink;



public class JchLib_Tests {
	

	

	
	static void localTimeTest1() {
		LocalTime t1 = LocalTime.parse("00:22:16.321358336");
		LocalTime t2 = LocalTime.parse("00:22:17.321358336");
		
		System.out.println(t1.getMinute());
		
		System.out.println(t1.compareTo(t2));
		System.out.println(t2.compareTo(t1));
		
		System.out.println(t1.toString());
	}

	

	
	public static void stringArrayTreeMapTest() {
        String arr1[] = {"1", "2", "3"}; 
        String arr2[] = {"1", "2", "3"}; 
        
        TreeMap<String[], String> tmap = new TreeMap<>(Arrays::compare);
        tmap.put(arr1, "yay!");
        
        System.out.println(tmap.get(arr2));
	}
	
	public static void stringArrayTreeMapTest2() {
		ArrayList<String> arr1 = new ArrayList<String>();
		ArrayList<String> arr2 = new ArrayList<String>();
		
		arr1.add("1");
		arr1.add("2");
		
		arr2.add("1");
		arr2.add("2");
		
		TreeMap<ArrayList<String>, String> tmap2 = new TreeMap<>();
		tmap2.put(arr1, "yay!");
		
		System.out.println(tmap2.get(arr2));
	}
	

	
	public static void testReplaceWithin() {
		String loadPath = "J:\\Load\\";
		
		File folder = new File(loadPath);
		File[] listOfFiles = folder.listFiles();
		List<String> seedFiles = new ArrayList<String>();
		
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				if(listOfFiles[i].getName().equals("desktop.ini") == false)
					seedFiles.add(listOfFiles[i].getName());
			} else if (listOfFiles[i].isDirectory()) {
				//directory
			}
		}
		
		for (int i = 0; i < seedFiles.size() ; i++) {
			ChunkList tempList = null;
			System.out.println(loadPath + seedFiles.get(i));
			tempList = ChunkList.loadFile(loadPath + seedFiles.get(i));
			System.out.println("file size? " + tempList.getChunkCount());
			System.out.println(tempList.replaceAll("\"\"", "'"));
			System.out.println(tempList.replaceOutsideKeyRangeAll("\"", "\"", ",", "|"));
			
			System.out.println(tempList.replaceAll("\"", ""));
			System.out.println(tempList.replaceAll("|\n", "\r\n"));
			tempList.reindex();
			
			System.out.println("file size? " + tempList.getChunkCount());
			
			BufferedWriter writer;
			try {
				tempList.moveFirstChunk();
				writer = new BufferedWriter(new FileWriter(loadPath + "a-" + seedFiles.get(i)));
				writer.write(tempList.toString(tempList.getChunkCount()));
				writer.close();
				System.out.print("~");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
	
	//Start from: *** START OF THIS PROJECT GUTENBERG EBOOK THE KING JAMES BIBLE ***
	//End from: End of the Project Gutenberg EBook of The King James Bible

	
	public static void blacksLawDictionaryGrabAndClean() {
		web_grabber_blacks();
		htmlScrubs_blacks();
		fileCompile_blacks();
	}
	
	public static void fileCompile_blacks() {
		String cleanLocation = "data/lang/eng/seed/published/blacks_law_dictionary/clean/";
		String compileLocation = "data/lang/eng/seed/published/blacks_law_dictionary/";
		
		File folder = new File(cleanLocation);
		File[] listOfFiles = folder.listFiles();
		List<String> seedFiles = new ArrayList<String>();
		
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				if(listOfFiles[i].getName().equals("desktop.ini") == false)
					seedFiles.add(listOfFiles[i].getName());
			} else if (listOfFiles[i].isDirectory()) {
				//directory
			}
		}
		
		System.out.println("file count: " +  seedFiles.size());
		
		try {
			System.in.read();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		ChunkList seeds = new ChunkList();
		ChunkList tempList = null;
		for (int i = 0; i < seedFiles.size() ; i++) {
			
			try {
				System.out.println(seedFiles.get(i));
				tempList = ChunkList.loadFile(cleanLocation + seedFiles.get(i));
				System.out.println("file size? " + tempList.getChunkCount());
				seeds.appendChunkList(tempList);
				//seeds.reindex();
				System.out.println("total seed size? " + seeds.getChunkCount());
			}
			catch(NullPointerException e) {
				
			}
		}
		
		seeds.reindex();
		seeds.moveFirstChunk();
		
		System.out.println(seeds.toString(seeds.getChunkCount()));
	    BufferedWriter writer = null;
	    
		try {
			writer = new BufferedWriter(new FileWriter(compileLocation + "blacks_law_dictionary.txt"));
	    	seeds.moveFirstChunk();
			System.out.println("Write location: " + compileLocation + "blacks_law_dictionary.txt");
			writer.write(seeds.toString(seeds.getChunkCount()));
			writer.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("done?");	
	}
	
	public static void htmlScrubs_blacks() {
		String seedLocation = "data/lang/eng/seed/published/blacks_law_dictionary/html/";
		String cleanLocation = "data/lang/eng/seed/published/blacks_law_dictionary/clean/";
		
		File folder = new File(seedLocation);
		File[] listOfFiles = folder.listFiles();
		List<String> seedFiles = new ArrayList<String>();
		
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				if(listOfFiles[i].getName().equals("desktop.ini") == false)
					seedFiles.add(listOfFiles[i].getName());
			} else if (listOfFiles[i].isDirectory()) {
				//directory
			}
		}
		
		ChunkList seeds = new ChunkList();
		ChunkList tempList = null;
		for (int i = 0; i < seedFiles.size() ; i++) {

			try {
				System.out.println(seedFiles.get(i));
				tempList = ChunkList.loadFile(seedLocation + seedFiles.get(i));
				System.out.println("file size? " + tempList.getChunkCount());
				System.out.println("total seed size? " + seeds.getChunkCount());
			}
			catch(NullPointerException e) {
				
			}

			ChunkList l2 = tempList;
			
			
			//This current recipe gets the job done
			//I like to show the chunk size reduction to how each method impacts the ChunkList
			System.out.println(l2.getChunkCount());
			l2.removeBeforeKey("<!-- #content Starts -->", true);
			System.out.println(l2.getChunkCount());
			l2.removeAfterKey("Comments are closed", true);
			System.out.println(l2.getChunkCount());
			l2.moveFirstChunk();
			l2.removeKeyRangeAll("<script", "/script>");
			System.out.println(l2.getChunkCount());
			l2.moveFirstChunk();
			l2.removeKeyRangeAll("<style", "/style>");
			System.out.println(l2.getChunkCount());
			l2.moveFirstChunk();
			l2.removeKeyRangeAll("<", ">");
			System.out.println(l2.getChunkCount());			
			l2.moveFirstChunk();
			l2.replaceAll("Comments are closed", "");
			System.out.println(l2.getChunkCount());
			l2.moveFirstChunk();
			l2.replaceAll("\t", "");
			System.out.println(l2.getChunkCount());			
			l2.moveFirstChunk();
			l2.replaceAll("\n\n", "\n");
			System.out.println(l2.getChunkCount());			
			l2.moveFirstChunk();
			l2.replaceAll("  ", " ");
			System.out.println(l2.getChunkCount());
			l2.moveFirstChunk();
			l2.replaceAll(" \n ", "");
			System.out.println(l2.getChunkCount());		
			l2.moveFirstChunk();
			l2.removeKeyRangeAll("Archive |","RSS feed for this section");
			System.out.println(l2.getChunkCount());		

		    BufferedWriter writer =null;
			try {
				writer = new BufferedWriter(new FileWriter(cleanLocation + seedFiles.get(i)));
		    	l2.moveFirstChunk();
				System.out.println(cleanLocation + seedFiles.get(i));
				writer.write(l2.toString(l2.getChunkCount()));
				writer.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		
		System.out.print("done?");
	}
	
	public static void web_grabber_blacks() {
		String base_uri = "https://thelawdictionary.org/";
		String part1_uri = "letter/";
		char part2_uri = 'c';
		String part3_uri = "/page/";
		short part4_uri = 1;

		short timeOutEncountered = 0;
		
		while(part2_uri < 'a' + 26) {
			timeOutEncountered = 0;
			while(part4_uri < 999 && timeOutEncountered < 5) {
				StringBuilder request_uri = new StringBuilder(base_uri +
															  part1_uri +
															  String.valueOf(part2_uri) +
															  part3_uri +
															  String.valueOf(part4_uri) 
															  +"/");
				//System.out.println(request_uri.toString());
				URL request = null;
				HttpURLConnection con = null;
				//if(part2_uri == 'a' && part4_uri == 1) {
					System.out.println(request_uri.toString());
					try {
						request = new URL(request_uri.toString());
						con = (HttpURLConnection) request.openConnection();
						con.setRequestMethod("GET");
						con.setReadTimeout(15*1000);  //15 seconds
						con.setDoOutput(true);
						con.connect();
						StringBuilder response = new StringBuilder();
						BufferedReader reader = 
								new BufferedReader(new InputStreamReader(con.getInputStream()));
						String line = null;
						while((line = reader.readLine()) != null) {
							response.append(line + "\n");
						}
						//System.out.println(response.toString());
						
						if(response != null && response.toString().length() > 0) {
							StringBuilder fileBase = new StringBuilder("data/lang/eng/seed/published/blacks_law_dictionary/html/");
							StringBuilder fileName = new StringBuilder(String.valueOf(part2_uri) + 
									                                   "_" +
									                                   String.valueOf(part4_uri) +
									                                   ".html.txt");
						    BufferedWriter writer = new BufferedWriter(new FileWriter(fileBase.toString() + fileName.toString()));
						    System.out.println(fileName.toString());
						    writer.write(response.toString());
						    writer.close();
						}
						
						con.disconnect();
						
						timeOutEncountered = 0;
						System.out.println("Made it to the end.");
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						timeOutEncountered++;
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						timeOutEncountered++;
					}
					finally {
						con.disconnect();
					}

				//}
				
				try {
					System.in.read();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
					
				part4_uri ++;
			}
			part2_uri++;
			part4_uri = 1;
		}
		
		
		//URL url = new URL("htt");
	}
	
	private static String readFile(String pathname) {
	    File file = new File(pathname);
	    StringBuilder fileContents = new StringBuilder((int)file.length());        
	
	    try (Scanner scanner = new Scanner(file)) {
	        while(scanner.hasNextLine()) {
	            fileContents.append(scanner.nextLine() + System.lineSeparator());
	        }
	        return fileContents.toString();
	    } catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 
	    return fileContents.toString();
	}
	

	
	public static void test16() {
		System.out.println(System.getProperty("os.name"));
		String seedLocation = "data/lang/eng/seed/published/project_gutenburg/txt/";
		
		File folder = new File(seedLocation);
		File[] listOfFiles = folder.listFiles();
		List<String> seedFiles = new ArrayList<String>();
		
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				seedFiles.add(seedLocation + listOfFiles[i].getName());
				
			} else if (listOfFiles[i].isDirectory()) {
				//directory
			}
		}
		
		ChunkList seeds = new ChunkList();
		ChunkList tempList = null;
		for (int i = 0; i < seedFiles.size() ; i++) {
			System.out.println(seedFiles.get(i));
			tempList = ChunkList.loadFile(seedFiles.get(i));
			System.out.println("file size? " + tempList.getChunkCount());
			seeds.appendChunkList(tempList);
			//seeds.reindex();
			System.out.println("total seed size? " + seeds.getChunkCount());
		}
		seeds.reindex();
		
		System.out.println(seeds.getChunkCount());
		//try {
		//	System.in.read();
		//} catch (IOException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
		
		System.out.println("Compiling Dictionary...");
		seeds.moveFirstChunk();
		TreeMap<String, Long> seedsDict = ChunkList.toStringTreeMapDictionary(seeds, getMedDelims());
		
		int i = 0;
		long wcntt = 0;
		for(Map.Entry<String, Long> entry : seedsDict.entrySet()) {
			wcntt++;
			System.out.print(entry.getValue());
			System.out.print("\t");
			System.out.print(entry.getKey());
			System.out.print("\t");
			
			for(int k = 0; k < entry.getKey().length(); k++) {
				System.out.print((int)entry.getKey().charAt(k));
				if(k < entry.getKey().length() - 1) System.out.print("-");
			}
			
			System.out.println();
			
			
			
			//if(++i % 1000 == 0) {
			//	try {
			//		System.in.read();
			//	} catch (IOException e) {
			//		// TODO Auto-generated catch block
			//		e.printStackTrace();
			//	}
			//}
		}
		System.out.println(wcntt + " words compiled");
		System.out.println("done!");
	}
	
	public static void test15() {
		//String fileLocation2 = "C:\\temp\\29765-8.txt";
		String fileLocation2 = "C:\\temp\\pg29765.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		System.out.println("Size of " + cl.getChunkCount());
		//cl.moveLastChunk();
		System.out.println((int)((StringChunkLink)cl.getCurrentChunk()).getValue().charAt(0));
		System.out.println(cl.toString(1));
		cl.moveLastChunk();
		cl.setRelativeOffset(-6);
		System.out.println(cl.toString(8));
	}
	 
	public static void test14(){
		TreeMap<String, Long> tmap = new TreeMap<String, Long>();
		
		short med_delims[] = getMedDelimsChar();
		//String fileLocation2 = "C:\\temp\\pg5.txt";
		//String fileLocation2 = "C:\\temp\\pg29765.txt";
		String fileLocation2 = "C:\\temp\\29765-8.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		cl.moveFirstChunk();
		System.out.println(cl.toString(cl.getChunkCount()).toUpperCase());
		
		System.out.println("size of: " + cl.getChunkCount());
		System.out.println("Now replacing stuff");
		System.out.println(cl.replaceAll("\r\n", new String(" ")));
		cl.reindex();
		cl.moveFirstChunk();
		
		System.out.println("size of: " + cl.getChunkCount());
		
		String tC = null;
		String tS = null;
		
		boolean delimOn = false;
		long valLen = 0;
		String tempVal = "";
		long cnt = 0;
		long wcnt = 0;
		//cl.setPosition(27000000);
		
		//while chunklist is reading
		do {
			//loop through character delimiters
			for(int i = 0; i < med_delims.length; i++) {
				tC = String.valueOf((char)med_delims[i]);
				tS = cl.toString(tC.length());
				delimOn = false;
				
				if(tC.equals(tS) || cl.getCurrentChunk().getNextChunk() == null) {
					if(valLen > 0 ) {
							//grab value
						cl.setRelativeOffset(-valLen);
						//System.out.print(cl.getCurrentChunk().getIndex() + "\t");
						
						if(tC.equals(tS) == true) {
							//System.out.print("A-");
							//System.out.println(cl.toString(valLen));
							tempVal = cl.toString(valLen);
						}
						else if(tC.equals(tS) == false && cl.getCurrentChunk().getNextChunk() == null) {
							//System.out.print("B-");
							//System.out.println(cl.toString(valLen + 1));
							tempVal = cl.toString(valLen);
						}
						else if(tC.equals(tS) == false || cl.getCurrentChunk().getNextChunk() == null) {
							//System.out.print("C-");
							//System.out.println(cl.toString(valLen + 1));
							tempVal = cl.toString(valLen);
						}
							
						tempVal = tempVal.toUpperCase();
						cl.setRelativeOffset(valLen);
					}
					valLen = 0;
					delimOn = true;
					
					//short circuit
					i = med_delims.length;
					
					//need to move current position to end of delimiter
					//used for delimiters > 1
					for(long j = 0; j < tC.length() - 1; j++)
						cl.moveNextChunk();
				}
			}
			
			if (delimOn == false) 
				valLen++;
			else if (tempVal != null && tempVal != "") {
				//do something tempVal
				wcnt++;
				
				//System.out.println(wcnt + " " + tempVal);
				//long c = tmap.get(cl.toString(i));
				if(tmap.get(tempVal) == null) {
					tmap.put(tempVal, (long) 1);
					//System.out.println(" doesnt exist");
				}
				else { 
					cnt = (long)tmap.get(tempVal);
					tmap.put(tempVal, ++cnt);
					//System.out.println(" it does exist");
				}
				tempVal = null;
			}
		}
		while(cl.moveNextChunk());
		
		
		//setup for writing output file
		BufferedWriter bWriter = null;
		String outPath = "C:\\temp\\dict1.txt";
		try {
			bWriter = new BufferedWriter(new FileWriter(outPath, true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long wcntt = 0;
		for(Map.Entry<String, Long> entry : tmap.entrySet()) {
			wcntt++;
			System.out.print(entry.getValue());
			System.out.print("\t");
			System.out.println(entry.getKey());
			
			try {
				bWriter.append(entry.getKey() + "\t" + entry.getValue() + "\n");

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
				
		}
		System.out.println(wcntt);
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
			           		"“",
			           		"”",
			           		"’",
			           		"?",
			           		"—",
			           		"‘"
						   
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
	
	public static void test9() {
		String fileLocation2 = "C:\\temp\\test2.txt";
		ChunkList cl1 = ChunkList.loadFile(fileLocation2);
		ChunkList cl2 = cl1.clone();
		
		System.out.println(((StringChunkLink)cl1.getCurrentChunk()).getValue());
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getValue());
		
		cl2.removeCurrentChunk();
		
		System.out.println(((StringChunkLink)cl1.getCurrentChunk()).getValue());
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getValue());
		
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getIndex());
		cl2.reindex();
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getIndex());
		
		System.out.println(cl1.getChunkCount());
		System.out.println(cl2.getChunkCount());
		
		cl2.moveNextChunk();
		cl2.moveNextChunk();
		
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getIndex());
		
		cl2.removeCurrentChunk();
		cl2.reindex();
		
		System.out.println(cl1.getChunkCount());
		System.out.println(cl2.getChunkCount());
		
		System.out.println(((StringChunkLink)cl1.getCurrentChunk()).getValue());
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getValue());
		
		cl2.moveLastChunk();
		cl2.removeCurrentChunk();
		cl2.reindex();
		
		System.out.println(cl1.getChunkCount());
		System.out.println(cl2.getChunkCount());
		
		cl2.moveFirstChunk();
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getIndex());
		cl2.moveLastChunk();
		System.out.println(((StringChunkLink)cl2.getCurrentChunk()).getIndex());
		
		
	}
	
	public static void test8() {
		//pre-processing techniques
		short removes[] = new short[] { 9,	//horizontal tab
						                10,	//newline
						                13	//carriage return
		};
		
		short repeats[] = new short[] { 9,	//horizontal tab
						                10,	//newline
						                13,	//carriage return
						                32	//space
		};

		short soft_delims[] = new short[] { 9,	//horizontal tab
                							10,	//newline
                							13,	//carriage return
                							32,
                							'!',
                							34,	//Double quote
                							39,	//Single Quote
                							'(',
                							')',
                							'*',
                							'+',
                							',',
                							'.',
                							':',
                							';',
                							'<',
                							'=',
                							'>',
				                       		'[',
				                       		92,		//Forward Slash
				                       		']',
				                       		'{',
				                       		'|',
				                       		'}'
                							
		};		
		
		short hard_delims[] = new short[] { 0,
											1,
											2,
											3,
											4,
											5,
											6,
											7,
											8,
											9,//horizontal tab
											10,//newline
											11,
											12,
											13,//carriage return
											14,
											15,
											16,
											17,
											18,
											19,
											20,
											21,
											22,
											23,
											24,
											25,
											26,
											27,
											28,
											29,
											30,
											31,				                       					                       		
				                       		32,
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
		
		
		String fileLocation2 = "C:\\temp\\pg5.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		System.out.println("cl size: " + cl.getChunkCount());
		
		System.out.println("removing.....");
		ChunkList cl2 = useRemoveFilter(cl, removes);
		
		
		System.out.println(cl.getChunkCount());
		System.out.println(cl2.getChunkCount());
		
		
		cl2.reindex();
		
		System.out.println(cl.getChunkCount());
		System.out.println(cl2.getChunkCount());
		
		cl.moveFirstChunk();
		for(int i = 1; i < 1000; i++) {
			System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
			cl.moveNextChunk();
		}
		
		System.out.println("\n\nVS \n\n");
		
		cl2.moveFirstChunk();
		for(int i = 1; i < 1000; i++) {
			System.out.print(((StringChunkLink)cl2.getCurrentChunk()).getValue());
			cl2.moveNextChunk();
		}
		//cl2.moveFirstChunk();
		//do {
		//	System.out.print((int)((StringChunkLink)cl2.getCurrentChunk()).getValue().charAt(0)); 
		//	System.out.print("\t");
		//	System.out.println(cl2.getCurrentChunk().getIndex());
		//}
		//while(cl2.moveNextChunk());
		

		
		//cl2.moveFirstChunk();
		//do {
		//	System.out.print((int)((StringChunkLink)cl2.getCurrentChunk()).getValue().charAt(0)); 
		//	System.out.print("\t");
		//	System.out.println(cl2.getCurrentChunk().getIndex());
		//}
		//while(cl2.moveNextChunk());

	
	}
	
	public static void test10() {
		//String[][] replace_filter = {{"test","test1"},
		//		                     {"test5","test7"},
		//		                     {new String(new char[]{'a','b'}),"t"},
		//};
		//for(int i = 0; i < replace_filter.length; i++) {
		//	for(int j = 0; j < replace_filter[i].length; j++) {
		//		System.out.println(replace_filter[i][j]);
		//	}
		//}
		
		String[][] replace_filter = {{"\r\n"," "},
			                         {"\t"," "},
			                         {"\r,", " "},
			                         {"\n"," "}
		};
		
		
		String fileLocation2 = "C:\\temp\\pg5.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		System.out.println("cl size: " + cl.getChunkCount());
		
		cl.setPosition(50);
		System.out.println(cl.toString(10));
		
		System.out.println(cl.getCurrentChunk().getIndex());
		System.out.println(cl.insertAfterCurrentChunk(" ~~~ you're a giant fairy faggot ~~~ "));
		cl.reindex();
		cl.moveFirstChunk();
		for(int i = 0; i < 200 ; i++) {
			System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
			cl.moveNextChunk();
		}
		
		System.out.println("");
		cl.setPosition(70);
		System.out.println(cl.getCurrentChunk().getIndex());
		System.out.println("cl size: " + cl.getChunkCount());
		cl.removeAfterCurrentChunk(10);
		
		System.out.println("cl size: " + cl.getChunkCount());
		cl.reindex();
		
		
		System.out.println("");
		System.out.println("cl size: " + cl.getChunkCount());
		System.out.println("");
		cl.moveFirstChunk();
		for(int i = 0; i < 200 ; i++) {
			System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
			cl.moveNextChunk();
		}
		
		System.out.println("");
		cl.setPosition(70);
		
		System.out.println(cl.getCurrentChunk().getIndex());
		System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
		System.out.println("");
		cl.removeFromCurrentChunk(10);
		cl.reindex();
		
		cl.moveFirstChunk();
		for(int i = 0; i < 200 ; i++) {
			System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
			cl.moveNextChunk();
		}
		
		System.out.println("");
		System.out.println("");
		
		cl.setPosition(1);
		System.out.println(cl.toString(10));
		
	};
	
	public static void test11() {
		String[][] replace_filter = {{"\r\n"," "},
					                {"\t"," "},
					                {"\r,", " "},
					                {"\n"," "}
		};


		String fileLocation2 = "C:\\temp\\pg5.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		cl.moveFirstChunk();
		System.out.println(cl.getChunkCount());
		
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(cl.toString(500));
		
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(cl.toString(5000));
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(cl.toString(50000));
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(cl.toString(500000));
		
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(cl.toString(cl.getChunkCount() - 1));
		

		System.out.println("whoa, you're done!");
		
		System.out.println(cl.replaceAll("\r\n", " "));
	}
	
	public static void test12() {
		String[][] replace_filter = {{"\r\n"," "},
		                {"\t"," "},
		                {"\r,", " "},
		                {"\n"," "}
		};
		
		
		//String fileLocation2 = "C:\\temp\\pg5.txt";
		String fileLocation2 = "C:\\temp\\pg29765.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		cl.moveFirstChunk();
		System.out.println("size of: " + cl.getChunkCount());
		System.out.println(cl.toString(cl.getChunkCount()));
		
		System.out.println("Now replacing stuff");
		
		System.out.println(cl.replaceAll("\r\n", new String(" ")));
		cl.reindex();
		System.out.println("now size of: " + cl.getChunkCount());
		
		System.out.println(cl.getCurrentChunk().getIndex());
		cl.moveFirstChunk();
		
		System.out.println("ready to print very large?");
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(cl.toString(cl.getChunkCount()));
	}
	
	public static void test13() {
		//String fileLocation2 = "C:\\temp\\pg5.txt";
		String fileLocation2 = "C:\\temp\\pg29765.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		cl.moveFirstChunk();
		System.out.println(cl.toString(cl.getChunkCount()).toUpperCase());
		
		System.out.println("size of: " + cl.getChunkCount());
		System.out.println("Now replacing stuff");
		System.out.println(cl.replaceAll("\r\n", new String(" ")));
		cl.reindex();
		cl.moveFirstChunk();
		
		System.out.println("size of: " + cl.getChunkCount());
	}
	
	public static ChunkList useReplaceFilter(ChunkList input, String[][] filter) {
		ChunkList output = input.clone();
		output.moveFirstChunk();
		
		return output;
	}
	
	public static ChunkList useRemoveFilter(ChunkList input, short[] filter) {
		long removed = 0;
		ChunkList output = input.clone();
		output.moveFirstChunk();
		do {
			
			String t1 = ((StringChunkLink)output.getCurrentChunk()).getValue();
			
			for (int i = 0; i < filter.length; i++) {
				
				String t2 = String.valueOf((char)filter[i]);
				if(t1.equals(t2)) {
					
					output.removeCurrentChunk();
					i = filter.length;
				}
			}
			
			
		}
		while(output.moveNextChunk());
		return output;
	}
	
	
	public static void test7() {
		String fileLocation2 = "C:\\temp\\29765-8.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		//StringChunkLink currentChunk = null;
		
	
		TreeMap<String, Long> tmap = new TreeMap<String, Long>();


		String sKey = "";
		long cnt = 0;
		//i - Chunk Size
		//j - List Position
		for(long i = 1; i < cl.getChunkCount(); i++) {
			cl.moveFirstChunk();
			tmap = new TreeMap<String, Long>();
			String maxString = "";
			for(long j = 1; j <= cl.getChunkCount() - (i - 1); j++) {
				//System.out.println(cl.toString(i));
				sKey = cl.toString(i).toUpperCase();
				
				//long c = tmap.get(cl.toString(i));
				if(tmap.get(sKey) == null) {
					tmap.put(sKey, (long) 1);
				}
				else { 
					cnt = (long)tmap.get(sKey);
					tmap.put(cl.toString(i), ++cnt);
				}
				
				cl.moveNextChunk();
			}
			
			long maxCount = 0;
			for(Map.Entry<String, Long> entry : tmap.entrySet()) {
				char c = 0;
				maxCount = entry.getValue();
				maxString = entry.getKey();
				
				System.out.print(i);
				System.out.print("\t");		
				if (i == 1) {
					c = maxString.charAt(0);
				}	
					
				System.out.print("\t");
				System.out.print((int)c);
				
				if((int)c >= 32) {
					System.out.print("\t");
					System.out.print(c);
				}
				
				System.out.print("\t");
				System.out.println(maxCount);
					
			}
			
			
			try {
				System.in.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static void test6() {
		String fileLocation2 = "C:\\temp\\pg5.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		//StringChunkLink currentChunk = null;
		
		System.out.println(cl.toString(580));
		cl.moveLastChunk();
		System.out.println(cl.toString(580));
		
		TreeMap<String, Long> tmap = new TreeMap<String, Long>();


		String sKey = "";
		long cnt = 0;
		//i - Chunk Size
		//j - List Position
		for(long i = 1; i < cl.getChunkCount(); i++) {
			cl.moveFirstChunk();
			tmap = new TreeMap<String, Long>();
			String maxString = "";
			for(long j = 1; j <= cl.getChunkCount() - (i - 1); j++) {
				//System.out.println(cl.toString(i));
				sKey = cl.toString(i);
				
				//long c = tmap.get(cl.toString(i));
				if(tmap.get(sKey) == null) {
					tmap.put(sKey, (long) 1);
				}
				else { 
					cnt = (long)tmap.get(sKey);
					tmap.put(cl.toString(i), ++cnt);
				}
				
				cl.moveNextChunk();
			}
			
			long maxCount = 0;
			for(Map.Entry<String, Long> entry : tmap.entrySet()) {
				if (entry.getValue() > maxCount) {
					maxCount = entry.getValue();
					maxString = entry.getKey();
				}
			}
			
			System.out.print(i);
			System.out.print("\t");
			System.out.print(maxCount);
			System.out.print("\t");
			System.out.print(tmap.size());
			System.out.print("\t");
			System.out.println(maxString);
			
		
			try {
				System.in.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	public static void test5() {
		String fileLocation2 = "C:\\temp\\29765-8.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		//StringChunkLink currentChunk = null;
		
		System.out.println(cl.toString(580));
		cl.moveLastChunk();
		System.out.println(cl.toString(580));
		
		TreeMap<String, Long> tmap = new TreeMap<String, Long>();


		String sKey = "";
		long cnt = 0;
		//i - Chunk Size
		//j - List Position
		for(long i = 1; i < cl.getChunkCount(); i++) {
			cl.moveFirstChunk();
			tmap = new TreeMap<String, Long>();
			String maxString = "";
			for(long j = 1; j <= cl.getChunkCount() - (i - 1); j++) {
				//System.out.println(cl.toString(i));
				sKey = cl.toString(i);
				
				//long c = tmap.get(cl.toString(i));
				if(tmap.get(sKey) == null) {
					tmap.put(sKey, (long) 1);
				}
				else { 
					cnt = (long)tmap.get(sKey);
					tmap.put(cl.toString(i), ++cnt);
					
					
				}
				
				cl.moveNextChunk();
			}
			
			long maxCount = 0;
			for(Map.Entry<String, Long> entry : tmap.entrySet()) {
				if (entry.getValue() > maxCount) {
					maxCount = entry.getValue();
					maxString = entry.getKey();
				}
			}
			
			System.out.print(i);
			System.out.print("\t");
			System.out.print(maxCount);
			System.out.print("\t");
			System.out.print(tmap.size());
			System.out.print("\t");
			System.out.println(maxString);
			
		
			try {
				System.in.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void test4() {
		String fileLocation2 = "C:\\temp\\test2.txt";
		ChunkList cl = ChunkList.loadFile(fileLocation2);
		cl.moveFirstChunk();
		for(long i = 0; i < cl.getChunkCount(); i++) {
			System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
			cl.moveNextChunk();
		}
		cl.setPosition(2);
		System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
		System.out.print(cl.toString(5));
		System.out.print(((StringChunkLink)cl.getCurrentChunk()).getValue());
	}
	
	public static void test3() {
		String fileLocation1 = "C:\\temp\\test3.txt";
		
		try {
			DataInputStream reader = new DataInputStream(new FileInputStream(fileLocation1));
			
			
			while(reader.available()>0) {
				//System.out.println(reader.readChar());
				System.out.println(reader.readUnsignedByte());
			}
			reader.close();
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public static void test2() {
		String fileLocation1 = "C:\\temp\\test1.txt";
		String fileLocation2 = "C:\\temp\\test2.txt";
		String fileLocation3 = "C:\\temp\\test3.txt";
		String fileLocation4 = "C:\\temp\\test4.txt";
		Path path1 = new File(fileLocation1).toPath();
	    Path path2 = new File(fileLocation2).toPath();
	    Path path3 = new File(fileLocation3).toPath();
	    Path path4 = new File(fileLocation4).toPath();
		
		String fileLocation5 = "C:\\temp\\waterpail.jpg";
	    Path path5 = new File(fileLocation1).toPath();
	    
	    String mimeType = "";
	    try {
			mimeType = Files.probeContentType(path1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    System.out.println(mimeType);
	    
	    
	    BufferedReader reader = null;
	    try {
	    	/*
			 StandardCharsets.US_ASCII;
			 StandardCharsets.UTF_8;
			 StandardCharsets.UTF_16;
			 StandardCharsets.UTF_16LE;
			 StandardCharsets.UTF_16BE;
			*/
			 reader = Files.newBufferedReader(path5, StandardCharsets.UTF_16);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    
	    try {
			while(reader.ready()) {
				System.out.println(reader.read());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void test1() {
		StringChunkLink link = new StringChunkLink("test123");
		System.out.println(link.getValue());
		
		ChunkList list = new ChunkList();
		
		System.out.println(list.appendChunk(link));
		
		System.out.println(((StringChunkLink) list.getCurrentChunk()).getValue());
		System.out.println(((StringChunkLink) list.getCurrentChunk()).getIndex());
		System.out.println(list.getChunkCount());
		
		System.out.println(list.appendChunk(link));
		
		System.out.println(list.getChunkCount());
		
		System.out.println(list.getCurrentChunk().getIndex());
		
		
		link = new StringChunkLink("test456");
		System.out.println(list.appendChunk(link));
		
		System.out.println(list.getCurrentChunk().getIndex());
		System.out.println(list.getChunkCount());
		
		link = new StringChunkLink("test789");
		System.out.println(list.appendChunk(link));
		
		System.out.println(list.getCurrentChunk().getIndex());
		System.out.println(list.getChunkCount());
		
		
		link = new StringChunkLink("test1101010101101010");
		System.out.println(list.appendChunk(link));
		
		System.out.println(list.getCurrentChunk().getIndex());
		System.out.println(list.getChunkCount());
		
		System.out.println(list.moveNextChunk());
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("a");
		System.out.println(list.moveNextChunk());
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("b");
		System.out.println(list.moveNextChunk());
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("c");
		System.out.println(list.moveNextChunk());
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("d");
		System.out.println(list.moveNextChunk());
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("e");
		System.out.println(list.moveNextChunk());
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("f");
		System.out.println(list.movePreviousChunk());
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("ff");
		System.out.println(list.movePreviousChunk());
		System.out.println(list.getCurrentChunk().getIndex());

		System.out.println("g");
		System.out.println(list.setPosition(4));
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("h");
		System.out.println(list.setPosition(1));
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("i");
		System.out.println(list.setPosition(4));
		System.out.println(list.getCurrentChunk().getIndex());
		
		System.out.println("j");
		System.out.println(list.setPosition(6));
		System.out.println(list.getCurrentChunk().getIndex());
		System.out.println(list.getChunkCount());
		
		System.out.println("k");
		System.out.println(list.setPosition(1));
		System.out.println(list.getCurrentChunk().getIndex());
		System.out.println(list.getChunkCount());
	}
}
