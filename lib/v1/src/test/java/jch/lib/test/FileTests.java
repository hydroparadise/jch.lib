package jch.lib.test;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import jch.lib.web.HttpWorker;
import jch.lib.file.ReadWorker;
import jch.lib.list.ChunkList;

public class FileTests {

	public FileTests() {
		// TODO Auto-generated constructor stub
	}

	public static void testmnrr() {
		String startLocation2 = "\\Shared Documents\\";
		String startLocation = "C:\\";
		magicNumberRecursiveRide(startLocation);
	}
	
	
	public static void magicNumberRecursiveRide(String startingLocation) {
		
		File folder = new File(startingLocation);
		File[] listOfFiles = folder.listFiles();
		List<String> listFiles = new ArrayList<String>();
		List<String> listFolders = new ArrayList<String>();
		BufferedWriter writer = null;
		
		if(listOfFiles == null)System.out.println(startingLocation + " is returning null");
		else {
			
			//open writer in append mode
			try {
				
				writer = new BufferedWriter(new FileWriter("C:\\temp\\file_magic_numbers.txt", true));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			//separate files and folders
			for(int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					listFiles.add(listOfFiles[i].getName());
				} else if (listOfFiles[i].isDirectory()) {
					listFolders.add(listOfFiles[i].getName());
				}
			}
			
			
			//report what was found
			if(listFiles.size() > 0)
			for(int i = 0; i < listFiles.size(); i++) {
				ReadWorker read = null;
				
				//read file
				try {
					read = new ReadWorker(startingLocation + listFiles.get(i));
					read.readBytes(16);
				} catch (NullPointerException |IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				try {
					
					writer.append(read.prettyPrintString_F1() + "\t");
				} catch (NullPointerException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				
				//print file
				try {
					writer.append(startingLocation + listFiles.get(i) + "\r\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println(startingLocation + listFiles.get(i));
				
				try {
					read.close();
				} catch (NullPointerException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			//close writer
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//Recurse through folders
			if(listFolders.size() > 0)
			for(int i = 0; i < listFolders.size(); i++) {
				magicNumberRecursiveRide(startingLocation + listFolders.get(i) + "\\");
			}
		}
	}
	
	
	//this will be a journey, i have no idea what i am about to find
	static void ffbe2() {
		String loc1 = "system/nix/android/memu/ffbe/";
		String fp1 = "read";
		int fp2 = 1;
		String fp3 =".txt";
		DataInputStream reader = null;
		
		try {
			reader = new DataInputStream(new FileInputStream(loc1 + "read.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedWriter writer = null;
		String temp = "";
		//writer.write(entry.getKey() + "\t" + entry.getValue() + "\r\n");
		long offSet = 0;
		byte[] readChunk;
		long a = 0;
		try {
			
			while(reader.available() > 0) {
				if(offSet % 10000000 == 0) System.out.println(offSet);
				
				if(offSet % 200000000 == 0 || offSet == 0) {
					if(offSet != 0) {
						writer.close();
					}
					
					try {
						writer = new BufferedWriter(new FileWriter(loc1 + fp1 + fp2 + fp3));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				//readChunk = reader.readNBytes(1);
				//writer.write((char)readChunk[0]);
				offSet++;
			}
			writer.close(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}
	
	static void ffbe1() {
		String loc1 = "system/nix/android/memu/ffbe/";
		String file1 = "disk2.vmdk";
		DataInputStream reader = null;
		
		String fp1 = "read";
		int fp2 = 1;
		String fp3 =".txt";
		//DataInputStream reader = null;
		
		long t1 = System.currentTimeMillis();
		long t2 = System.currentTimeMillis();
		long r1 = 0;
		
		Instant instant = Instant.now();
		long timeStampMillis = instant.toEpochMilli();
		
		try {
			reader = new DataInputStream(new FileInputStream(loc1 + file1));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		String temp = "";
		//writer.write(entry.getKey() + "\t" + entry.getValue() + "\r\n");
		long offSet = 0;
		BufferedWriter writer =null;
		byte[] readChunk;
		long a = 0;
		long b = 0;
		try {
			
			while(reader.available() > 0) {
				
				if(b % 100000000 == 0 || offSet == 0) {
					if(offSet != 0) {
						writer.close();
						
					}
					
					try {
						writer = new BufferedWriter(new FileWriter(loc1 + fp1 + fp2 + fp3));
						fp2++;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					b = 0;
				}
				
				if(offSet % 10000000 == 0) {
					t1 = System.currentTimeMillis();
					r1 = t1 - t2;
					System.out.print(offSet + "\t");
					System.out.println(r1/1000.0);
					t2 = t1;
				}
				
				
				if(a % 80 == 0 || offSet == 0) {
					//System.out.println("\r\n" + offSet + "\t");
					writer.write("\r\n" + offSet + "\t");
					a = 1;
				}
				
				//readChunk = reader.readNBytes(1);
				//if(readChunk[0] > 32 && readChunk[0] < 127) {
					//temp = offSet + "\t" +
					//	   readChunk[0] + "\t" +
					//	   (char)readChunk[0] + "\r\n";					
					//writer.write((char)readChunk[0]);
					//System.out.print((char)readChunk[0]);
					//a++;
					//b++;
				//}
				offSet++;
			}
			writer.close(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}

	
	
	public static void test21_fileDesc() {
		String htmlLocation = "data/sys/file/filedesc.com/html/TXT.html";
		ChunkList htmlTxt =  ChunkList.loadFile(htmlLocation);
		System.out.println(htmlTxt.getChunkCount());
		//System.out.println(htmlTxt.toString(htmlTxt.getChunkCount()));
		
		
		DocumentBuilderFactory docFactory = null;
		DocumentBuilder docBuilder = null;
		Document doc = null;
		
		docFactory = DocumentBuilderFactory.newInstance();
		try {
			docBuilder = docFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			doc = docBuilder.parse(htmlTxt.toString(htmlTxt.getChunkCount()));
		} catch (SAXException | IOException e) {
			// TODO Auto-generated catch block
			System.out.println("A");
			e.printStackTrace();
		}
		
		System.out.println(doc.getDocumentElement().getTagName());
		
	}
	
	public static void test20_fileDesc() {
		//https://www.filedesc.com/en/file/<extension>
		
		//System.out.println(System.getProperty("os.name"));
		String seedLocation = "data/sys/file/xtensions.txt";
		String htmlLocation = "data/sys/file/filedesc.com/html/";
		
		String lastXten = "";
		String t = "";
		
		File folder = new File(htmlLocation);
		File[] listOfFiles = folder.listFiles();
		List<String> listFiles = new ArrayList<String>();
		List<String> listFolders = new ArrayList<String>();
		
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				listFiles.add(listOfFiles[i].getName());
				
				t = listOfFiles[i].getName().substring(0, listOfFiles[i].getName().length() - 5);
				if(t.compareToIgnoreCase(lastXten) > 0 ) {
					lastXten = t;
				}
				
			} else if (listOfFiles[i].isDirectory()) {
				//directory
			}
		}
		
		System.out.println(lastXten + " is the last xtension...");
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		ChunkList seeds =  ChunkList.loadFile(seedLocation);
		TreeMap<String, Long> seedsDict = ChunkList.toStringTreeMapDictionary(seeds, getMedDelims());
		
		
		for(Map.Entry<String, Long> entry : seedsDict.entrySet()) {
			System.out.println(entry.getKey());
			
			//last xtension grabbed
			if(lastXten.compareToIgnoreCase(entry.getKey()) < 0) {
				HttpWorker hw = new HttpWorker("https://www.filedesc.com/en/file/");
				hw.setUriSuffix(entry.getKey().toLowerCase());
				
				System.out.println(hw.getFullUri());
				//hw.connect();
				hw.setInputStreamToResponse();
				hw.disconnect();
				
				StringBuilder writePath = new StringBuilder(htmlLocation + entry.getKey() + ".html");
				System.out.print(writePath.toString());
				
				if(hw.getResponse() != null && hw.getResponse().length() > 0) {
					BufferedWriter writer;
					try {
						writer = new BufferedWriter(new FileWriter(writePath.toString()));
						writer.write(hw.getResponse());
						writer.close();
						System.out.print("~");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	
				}
				System.out.println();
				
				/*
				try {
					int n = (int)((double)(Math.random() * 2.0 + 1.0) * 1000);
					System.out.println(n);
					Thread.sleep(1000 + n);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				*/
			}
		}
	}
	
	public static void combineFiles () {
		System.out.println(System.getProperty("os.name"));
		//String seedLocation = "lang/eng/seed/published/project_gutenburg/txt/";
		
		String seedLocation = "C:\\temp\\temp\\";
		
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

		System.out.println("files compiled...");
		System.out.println("witting to a single file...");
		seeds.moveFirstChunk();
		
		BufferedWriter writer = null;
		
		try {
			writer = new BufferedWriter(new FileWriter(seedLocation + "combo.txt", true));
			//System.out.println(seeds.toString(800000));
			System.out.println(seeds.getChunkCount());
			System.out.println(seeds.toString(8000000).length());
			System.out.println(seeds.toString(seeds.getChunkCount()).length());
			writer.append(seeds.toString(seeds.getChunkCount()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void test19() {
		String fileLocation = "C:\\temp\\";
		String fileName = "pg5.txt";
		ReadWorker reader = null;
		String test = "";
		String testHex = "";
		String currentOffsetHex = "";
		String printString= "";
		
		try {
				reader = new ReadWorker(fileLocation + fileName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		for(int i = 0; i < 8; i++) {
			//open

			
			//read
			try {
				reader.readBytes(1);
				test = reader.currentBufferToString();
				testHex = reader.currentBufferToHexString();
				currentOffsetHex = reader.currentOffsetToHexString();
				printString = reader.prettyPrintString_F1();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//print
			//System.out.println(test);
			//System.out.println(testHex);
			//System.out.println(currentOffsetHex);
			System.out.println(printString);
			
			

		}
		
		//close
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
