package jch.lib.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import jch.lib.list.ChunkList;

public class DictionaryTest {

	public DictionaryTest() {
		// TODO Auto-generated constructor stub
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
}
