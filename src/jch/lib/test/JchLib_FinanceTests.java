package jch.lib.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import jch.lib.analytics.text.GhostWriter;
import jch.lib.analytics.text.GhostWriter.StatLenEntry;
import jch.lib.analytics.text.StringStatEntryList.StringStatEntry;
import jch.lib.common.ReadWorker;
import jch.lib.common.chunk.ChunkList;

public class JchLib_FinanceTests {

	public JchLib_FinanceTests() {
		// TODO Auto-generated constructor stub
	}

	

	public static void transformAchFiles() {	
		String outFileName = "AH";
		String outFileDateName = "05092019";
		String processDate = "20190509";
		
		outFileDateName = "05082019";
		processDate = "20190508";
		
		
		outFileName = outFileName + outFileDateName;

		String seedLocation = "J:/Chad's ACH files/" + processDate + "/";
		
		String ds[] = new String[] { 

				   new String(new char[]{10}),	//newline
				   new String(new char[]{13})	//carriage return
		};
		
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
			seeds.appendChunkList(ChunkList.stringToChunks("\n"));
			//seeds.reindex();
			System.out.println("total seed size? " + seeds.getChunkCount());
		}
		seeds.reindex();
		
		
		seeds.moveFirstChunk();
		ArrayList<String> stringList = ChunkList.toStringList(seeds, ds);
		System.out.println(stringList.size());
		
		int beId = 0;
		int eId = 0;
		int entryType = 0;
		int prevEntryType = entryType;
		
		String fileToWrite;
		for(int i = 0; i < stringList.size() ;  i++) {
			
			try {
				entryType = Integer.parseInt((String.valueOf(stringList.get(i).charAt(0))));
				
				if(entryType == 1) {
					beId++;
					eId = 0;
				}
				
				if(entryType == 5) {
					beId++;
					eId = 0;
				}
				
				if(entryType == 9 &&
				   prevEntryType != entryType) {
					beId++;
					eId = 0;
				}
				
				eId++;
				
				fileToWrite = seedLocation + "out/" + outFileName + "_" + (String.valueOf(stringList.get(i).charAt(0))) + ".txt";
				//System.out.println(fileToWrite);
				
				FileWriter fw = new FileWriter(fileToWrite, true);
				fw.write(processDate + "\t");
				fw.write(beId + "\t");
				fw.write(eId + "\t");
				fw.write(stringList.get(i) + "\r\n");
				fw.close();
				prevEntryType = entryType;
			}
			catch(Exception e) {
				//System.out.println(e.getMessage());
			}

		}
		
	}
	
	public static void test25() {
		
		ChunkList test = new ChunkList();
		
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		test.appendChunkList(ChunkList.stringToChunks("a"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		test.appendChunkList(ChunkList.stringToChunks("b"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		
		test.appendChunkList(ChunkList.stringToChunks("c"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		test.appendChunkList(ChunkList.stringToChunks("d"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		test.appendChunkList(ChunkList.stringToChunks("e"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		test.appendChunkList(ChunkList.stringToChunks("f"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		test.appendChunkList(ChunkList.stringToChunks("g"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
		
		test.appendChunkList(ChunkList.stringToChunks("h"));
		test.setPosition(test.getChunkCount() - 5);
		System.out.println(test.toString(5));
	}
	

	public static void test23() {
		String loadPath = "J:\\Chad\\EXTRACT.LOANTRANSACTION";
		//String loadPath = "J:\\Load\\*_69259_1_1050541_All_12202018132025.csv";
		ReadWorker read = null;
		
		try {
				read = new ReadWorker(loadPath);
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			read.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/*
	 * Transform credit report that can be safely imported into SQL Server (SSMS)
	 * 
	 */
	public static void test22() {
		String loadPath = "J:\\Load\\test\\test.csv";
		//String loadPath = "J:\\Load\\*_69259_1_1050541_All_12202018132025.csv";
		ReadWorker read = null;
		try {
			read = new ReadWorker(loadPath);
			while(read.readBytes(32) > 0) {
				System.out.println(read.prettyPrintString_F1());
			}		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			read.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ChunkList temp = ChunkList.loadFile(loadPath);
		System.out.println(temp.toString(20));
		
		temp.moveLastChunk();
		System.out.println(temp.toString(1).length());
		temp.movePreviousChunk();
		System.out.println(temp.toString(1).length());
		temp.movePreviousChunk();
		System.out.println(temp.toString(1).length());
		
	}
	
	/*
	 * Prints in hex a crereport that can be safely imported into SQL Server (SSMS)
	 * 
	 */
	public static void testReplaceWithin3() {
		String loadPath = "J:\\Load\\*54420_1_1040962_All_06192017160512.csv";

		ReadWorker read;
		try {
			read = new ReadWorker(loadPath);
			
			while(read.readBytes(32) > 0) {
				System.out.println(read.prettyPrintString_F1());
			}
			
			for(int i = 0; i < 64; i++) {
				read.readBytes(32);
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void testReplaceWithin2() {
		

		//String loadPath = "J:\\Load\\*_54420_1_1040962_All_06192017160512.csv";
		String loadPath = "J:\\Load\\*_69259_1_1050541_All_12202018132025.csv";
		ReadWorker read;
		try {
			read = new ReadWorker(loadPath);
			for(int i = 0; i < 64; i++) {
				read.readBytes(32);
				System.out.println(read.prettyPrintString_F1());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		ChunkList tempList = ChunkList.loadFile(loadPath);
		tempList.replaceAll("|\n", "\n");
		BufferedWriter writer;
		
		try {
			tempList.moveFirstChunk();
			writer = new BufferedWriter(new FileWriter(loadPath+".csv"));
			writer.write(tempList.toString(tempList.getChunkCount()));
			writer.close();
			System.out.print("~");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		System.out.println("Done!");
	}
	
	
	
	/*
	 * Load and analyze member transaction info to extract places of business to understand 
	 * where they shop.  In short, performing text analysis for common strings
	 * 
	 * 1) Char by char analysis
	 * 2) Word by word analysis
	 * 3) Phrase by phrase analysis
	 * 
	 */
	static String tdLoc = "R:\\_Analytics\\Business Intelligence\\Analysis\\20201118 Member Tran Analysis\\Tran Locs Prev 180\\TranTypes\\";
	static String tdFiles[] = {
			"01_Sig\\Sig_WB.txt",
			"02_POS\\POS_WB.txt",
	};
	
	public static void mbrTranLoad1() {
		//load file into linked list
			System.out.println("Loading file " + tdLoc + tdFiles[0]);
		ChunkList posWb = ChunkList.loadFile(tdLoc + tdFiles[0]);
		
		//do some file cleaning
		System.out.println("Cleaning...");
		posWb.replaceAll("\r\n", " ");	//remove new lines
		posWb.replaceAll("  ", " ");	//remove repeated spaces
		posWb.reindex();
		
		//Ghostwriter object will do analysis
		GhostWriter gw = new GhostWriter();
		
		//Pass linked list by reference for gw to access
		gw.SeedSource = posWb;
		
		System.out.print("Max Repeat Length: ");
		System.out.println(gw.calcMaxLengthString());
		
		
		gw.buildStringStats2(25);
		
		System.out.println("Stats Built");
		System.out.println(gw.CharStringStatLen.size());
		
		for(Map.Entry<Integer, StatLenEntry>  statLen : gw.CharStringStatLen.entrySet()) {
			//print aggregation results
			System.out.print(statLen.getKey());
			System.out.print("\t");
			System.out.println(statLen.getValue().CharChanceList.list.size());
			
			//print details
			
			/*
			for(StringStatEntry listEntry : statLen.getValue().CharChanceList.list) {
				System.out.print(listEntry.getFrom() + listEntry.getTo() +"\t");
				System.out.println(listEntry.getCount());
			}
			
			try {
				System.in.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			*/
		}
		
		
		gw.buildCharPublish(500);
		
		System.out.println("done!");
	}
	
}
