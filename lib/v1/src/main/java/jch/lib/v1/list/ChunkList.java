package jch.lib.list;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 *  A basic double-linked list of "chunks" with editing features like
 *  iterate, insert, or remove parts of the list based on a cursor object
 *  called currentChunk, or .getCurrentChunk().  Because of the 
 *  expensive nature of repairing or reindexing of a given list, it is up
 *  to the object user to call the .reindex() method which is recommended any
 *  time list modifications have been made. 
 *  
 *  The intended use is to create a basic unit of analysis for the purpose
 *  of performing some statistical calculations. A chunk can support a
 *  text type, numerical type, or byte array.
 *  
 *  ~~~ Notes ~~~
 *  - Thread Safety
 *  - OpenCL Friendly
 *  
 * @author James Chad Harrison
 * @version 0.0.0
 */
public class ChunkList {


	
	/**
	 * Used as a factory method to generate ChunkList from a given file path.
	 * Very basic attempts are made to determine the file type, therefore, the
	 * data, that each chunk type will be initialized as... 
	 * 
	 * If a text file is detected, various encodings will be attempted for
	 * a proper file read.
	 * 
	 * 
	 * @param filePath
	 * @return String based ChunkList that turns each character into a chunk,
	 * there are various instance methods that supports this format/scheme
	 */
	public static ChunkList loadFile(String filePath) {
		//consider refactoring this function
		ChunkList outputList = null;
		Path path = new File(filePath).toPath();	    
	    String mimeType = "";
	    
	    try {
			mimeType = Files.probeContentType(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    //System.out.println(mimeType);
	    
	    //read file as text
	    if(mimeType != null &&
	    		mimeType != "" && ( 
	    		mimeType.equals("text/plain")  || 
	    		mimeType.equals("text/xml")  || 
	    		mimeType.equals("text/html") ||
	    		mimeType.equals("application/vnd.ms-excel"))) {
	    	//this method tries to read a text file in different encodings
	    	List<Charset> csl = new ArrayList<Charset>();
		    csl.add(StandardCharsets.US_ASCII);
		    csl.add(StandardCharsets.ISO_8859_1);
		    csl.add(StandardCharsets.UTF_8);
		    csl.add(StandardCharsets.UTF_16);
		    csl.add(StandardCharsets.UTF_16LE);
		    csl.add(StandardCharsets.UTF_16BE);
		    
		    BufferedReader reader = null;
		    boolean readSuccess = false;
		    boolean openSuccess = false;
		    short tryCharSet = 0;
		    short openTry = 0;
		    String sChunk = "";
		    
		    while(((openSuccess == false || readSuccess == false) && 
		    		tryCharSet < csl.size()) && openTry < 3) {
			    try {
					 reader = Files.newBufferedReader(path, csl.get(tryCharSet));
					 openSuccess = true;
					 //outputList.setListCharset((Charset)csl.get(tryCharSet));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					//System.out.println("Could not open.");
					e.printStackTrace();
					openSuccess = false;
					openTry++;
				}
			    if(openSuccess == true) {
				    outputList = new ChunkList();
				    try {
						while(reader.ready()) {
							
							sChunk = Character.toString((char)reader.read());
							outputList.appendChunk(new StringChunkLink(sChunk));
							//Character.toString((char)reader.read());
						}
						//if(reader.ready() == false) System.out.println("last: " + );
						readSuccess = true;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						//System.out.println("Could not read.");
						//e.printStackTrace();
						tryCharSet++;
						outputList = null;
					}	
			    }
		    }
	    }
	    //read file as binary
	    else {
	    	//if(mimeType == null) {System.out.println("mimetype is null");}
	    	//DataInputStream dataReader = new DataInputStream(new FileInputStream(filePath));
	    	//System.out.println("unsuported file type");
	    }
	    return outputList;
	}

	/**
	 * Converts a string to a ChunkList of StringChunkLink's
	 * @param input
	 * @return ChunkList, or null if input string was invalid
	 */
	public static ChunkList stringToChunks(String input) {
		ChunkList newChunks = null;
		//Basic string check
		if(input != null && input.length() > 0) {
			newChunks = new ChunkList();
			//Loop through string
			for(int i = 0; i < input.length() ; i++) {
				//feed StringChunkLink's in the ChunkList
				newChunks.appendChunk(new StringChunkLink(String.valueOf(input.charAt(i))));
			}
		}
		return newChunks;
	}
	
	/**
	 * Splits up chunklist into an array list of strings based on an array of delimiters
	 * @param input
	 * @param delims
	 * @return
	 */
	
	public static ArrayList<String> toStringList(ChunkList input, String[] delims) {
		ArrayList<String> stringList = null;
		if(input.getFirstChunk() != null && delims.length > 0) {
	
			//ChunkLink temp1 = input.getFirstChunk();
			String tC = null;
			String tS = null;
			boolean delimOn = false;
			long valLen = 0;
			String tempVal = "";
			//long cnt = 0;
			long wcnt = 0;
			
			do {
				//loop through character delimiters
				for(int i = 0; i < delims.length; i++) {
					tC = delims[i];
					tS = input.toString(tC.length());
					
					
					delimOn = false;
					if(tC.equals(tS) || input.getCurrentChunk().getNextChunk() == null) {
						
						if(valLen > 0 ) {
							//grab value
							input.setRelativeOffset(-valLen);					
							if(tC.equals(tS) == true) {
								tempVal = input.toString(valLen);
							}
							else if(tC.equals(tS) == false && input.getCurrentChunk().getNextChunk() == null) {
								tempVal = input.toString(valLen);
							}
							else if(tC.equals(tS) == false || input.getCurrentChunk().getNextChunk() == null) {
								tempVal = input.toString(valLen);
							}
								
							tempVal = tempVal.toUpperCase();
							input.setRelativeOffset(valLen);
						}
						valLen = 0;
						delimOn = true;
						
						//short circuit
						i = delims.length;

						//need to move current position to end of delimiter
						//used for delimiters > 1
						for(long j = 0; j < tC.length() - 1; j++)
							input.moveNextChunk();
					}
				}
	
				if (delimOn == false){
					valLen++;
				}
				else if (tempVal != null && tempVal != "") {
					wcnt++;
					if (wcnt == 1) stringList = new ArrayList<String>();
					stringList.add(tempVal);
					tempVal = null;
				}
			}
			while(input.moveNextChunk());	
		}
		
		return stringList;
	}
	
	/**
	 * Generates a TreeMap from a string based ChunkList.
	 * 
	 * A good way to generate a dictionary of words for a given text, and understand
	 * word usage and statics
	 * @param input - A string based ChunkList to be converted to a TreeMap
	 * @param delims - an array of string(s) used to chop of the file into string values
	 * @return
	 */
	public static TreeMap<String, Long>  toStringTreeMapDictionary(ChunkList input, String[] delims) {
		TreeMap<String, Long> tmap = null;
		
		if(input.getFirstChunk() != null && delims.length > 0) {
			//ChunkLink temp1 = input.getFirstChunk();
			tmap = new TreeMap<String, Long>();
			String tC = null;
			String tS = null;
			boolean delimOn = false;
			long valLen = 0;
			String tempVal = "";
			long cnt = 0;
			@SuppressWarnings("unused")
			long wcnt = 0;
			
			do {
				//loop through character delimiters
				for(int i = 0; i < delims.length; i++) {
					tC = delims[i];
					tS = input.toString(tC.length());
					delimOn = false;
					if(tC.equals(tS) || input.getCurrentChunk().getNextChunk() == null) {
						if(valLen > 0 ) {
							//grab value
							input.setRelativeOffset(-valLen);					
							if(tC.equals(tS) == true) {
								tempVal = input.toString(valLen);
							}
							else if(tC.equals(tS) == false && input.getCurrentChunk().getNextChunk() == null) {
								tempVal = input.toString(valLen);
							}
							else if(tC.equals(tS) == false || input.getCurrentChunk().getNextChunk() == null) {
								tempVal = input.toString(valLen);
							}
								
							tempVal = tempVal.toUpperCase();
							input.setRelativeOffset(valLen);
						}
						valLen = 0;
						delimOn = true;
						
						//short circuit
						i = delims.length;
	
						//need to move current position to end of delimiter
						//used for delimiters > 1
						for(long j = 0; j < tC.length() - 1; j++)
							input.moveNextChunk();
					}
				}
				
				if (delimOn == false) 
					valLen++;
				else if (tempVal != null && tempVal != "") {
					wcnt++;
					if(tmap.get(tempVal) == null) {
						tmap.put(tempVal, (long) 1);
					}
					else { 
						cnt = (long)tmap.get(tempVal);
						tmap.put(tempVal, ++cnt);
					}
					tempVal = null;
				}
			}
			while(input.moveNextChunk());	
		}	
		return tmap;
	}
	
	/**
	 * Generates a TreeMap from a string based ChunkList.
	 * 
	 * A good way to generate a dictionary of words for a given text, and understand
	 * word usage and statics
	 * @param input - A string based ChunkList to be converted to a TreeMap
	 * @param delims - an array of string(s) used to chop of the file into string values
	 * @return
	 */
	public static TreeMap<String, Long>  toStringTreeMapPhrase(ChunkList input, int lowWordCount, int highWordCount, String[] delims) {
		TreeMap<String, Long> tmap = null;
		System.out.println("tree mapping...");
		
		if(input.getFirstChunk() != null && delims.length > 0) {
			//ChunkLink temp1 = input.getFirstChunk();
			tmap = new TreeMap<String, Long>();
			boolean on1 = true;
			long vl1 = 0;
			@SuppressWarnings("unused")
			long dl1 = 0;
			
			boolean on2 = true;
			long vl2 = 0;
			long dl2 = 0;
			long wordCount;
			
			@SuppressWarnings("unused")
			long wcnt = 0;
			long cnt = 0;
			
			long valLen = 0; 
			String tempVal = "";
			input.moveFirstChunk();
			
			System.out.println("getting ready to loop");
			do {
				//first determine if current chunk is on a delimiter
				on1 = input.compareStringArrayToCurrentChunk(delims);
				
				if(on1 == true) 
					{dl1++;vl1=0;}
				else 
					{dl1=0;vl1++;}
				
				//System.out.println("dl1: " + dl1 + " vl1: " + vl1 + " " + input.getCurrentChunk().getIndex() + " " + valLen);
				
				//now looking forward, will bring back
				if(vl1 == 1) {
					dl2 = 0;
					vl2 = 0;
					
					//System.out.println("grab it fat rabbit");
					ChunkLink temp = input.getCurrentChunk();
					wordCount = 0;
					valLen = 0;
					do {
						
						on2 = input.compareStringArrayToCurrentChunk(delims);
						
						if(on2 == true) 
							{dl2++;vl2=0; }
						else 
							{dl2=0;vl2++;}
						
						valLen++;
						
						if(dl2 == 1) {
							//input.movePreviousChunk();
							wordCount++;
							//System.out.println("ha!!! " + wordCount);
						}
						
						
						//System.out.println("dl2: " + dl2 + " vl2: " + vl2 + " " + input.getCurrentChunk().getIndex() + " " + valLen);
						if(vl2 == 1 || wordCount == highWordCount) {
							if(wordCount >= lowWordCount) {
								input.setRelativeOffset(-(valLen - 1));
								tempVal = input.toString(valLen - 1).toUpperCase();
								input.setRelativeOffset(valLen - 1);
								
								//System.out.println(tempVal);
								
								if (wordCount == highWordCount) {
									//System.out.println("now going back back back");
									input.currentChunk = temp.getNextChunk();
									valLen = 0;
								}
								
								if (tempVal != null && tempVal != "") {
									wcnt++;
									if(tmap.get(tempVal) == null) {
										tmap.put(tempVal, (long) 1);
									}
									else { 
										cnt = (long)tmap.get(tempVal);
										tmap.put(tempVal, ++cnt);
									}
									tempVal = null;
								}
	
							} 
						}
						
					}
					while(input.moveNextChunk() && wordCount < highWordCount);
				}		
							
			}
			while(input.moveNextChunk());		
		
		}
		return tmap;
	}
	
	/**
	 * Adds a ChunkLink in the the current ChunkList
	 * @param newChunkLink
	 * @return True if successful, False if not
	 */
	public boolean appendChunk(ChunkLink newChunkLink) {
		if (newChunkLink != null && newChunkLink.getIndex() == 0) {
			chunkCount++;
			newChunkLink.setIndex(chunkCount);
			if(chunkCount == 1) {
				currentChunk = newChunkLink;
				firstChunk = newChunkLink;
				lastChunk = newChunkLink;
			}
			else {
				lastChunk.setNextChunk(newChunkLink);
				newChunkLink.setPreviousChunk(lastChunk);
				lastChunk = newChunkLink;
				
			}
			return true;
		}
		else return false;
	}
	
	/**
	 * Inserts an instantiated ChunkList instance after the last chunk
	 * of the current chunk list.
	 * 
	 * Remember to use .reindex() after this method.
	 * @param input
	 * @return
	 */
	public boolean appendChunkList(ChunkList input) {
		if(input != null && input.getChunkCount() > 0) {
			ChunkLink temp1 = this.lastChunk;
			//make sure a current chunk is set
			if(temp1 != null) {
				//append insert


				ChunkLink temp3 = input.getFirstChunk();
				//ChunkLink temp4 = input.getLastChunk();
				temp1.setNextChunk(temp3);
				temp3.setPreviousChunk(temp1);
				this.lastChunk = input.getLastChunk();
				
			}
			else {
				this.currentChunk = input.getCurrentChunk();
				this.firstChunk = input.getFirstChunk();
				this.lastChunk = input.getLastChunk();
			}
			this.chunkCount = this.chunkCount + input.getChunkCount();
			return true;
		}
		else { 
			return false;
		}
	}
	
	/**
	 * Generatetes a new ChunkList deep-copy of a specific instance of
	 * ChunkList. This is a chunk type-sensitive method.
	 * 
	 * This method requires further development
	 * 
	 * @return null if firstChunk is not set
	 */
	public ChunkList clone() {
		ChunkList output = null;
		ChunkLink temp = null;
		//Always check for firstChunk
		if(this.firstChunk != null) {
			output = new ChunkList();
			temp = this.firstChunk;
			moveFirstChunk();
			do {
				temp = this.getCurrentChunk();
				
				//System.out.println(temp.getIndex());
				if(temp.getChunkType().equals("StringChunkLink")) 
					output.appendChunk(new StringChunkLink(((StringChunkLink)temp).getValue()));
			}
			while(this.moveNextChunk());	
		}

		return output;
	}
	
	/**
	 * Takes an array of strings, and compares each string to the  
	 * @param strings
	 * @return
	 */
	public boolean compareStringArrayToCurrentChunk(String[] strings) {
		String tC = "";
		String tS = "";
	    boolean fnd = false;
		for(int i = 0; i < strings.length; i++) {
			tC = strings[i];
			tS = this.toString(tC.length());
			if(tC.equals(tS)) { 							
				i = strings.length;
				fnd = true;
			}
		}
		return fnd;
	}
	
	/**
	 * Returns the current number of chunks within the chunk list.
	 * @return long value >= 0
	 */
	public long getChunkCount(){
		return this.chunkCount;
	}
	
	/**
	 * Returns the current chunk, or cursor chunk, for a given list.
	 * @return ChunkLink or null
	 */
	public ChunkLink getCurrentChunk() {
		return currentChunk;
	}
	
	/**
	 * Returns the first chunk for a given list.
	 * @return A ChunkLink or null
	 */
	public ChunkLink getFirstChunk() {
		return firstChunk;
	}
	
	/**
	 * Returns the last chunk for a given list.
	 * @return ChunkLink or null
	 */
	public ChunkLink getLastChunk() {
		return lastChunk;
	}
	
	/**
	 * Inserts an instantiated ChunkList instance directly after the
	 * currentChunk, and directly before the link the appears after
	 * the currentChunk.
	 * 
	 * Remember to use .reindex() after this method.
	 * @param input
	 * @return
	 */
	public boolean insertAfterCurrentChunk(ChunkList input) {
		
		if(input != null && input.getChunkCount() > 0) {
			ChunkLink temp1 = this.currentChunk;
			//make sure a current chunk is set
			if(temp1 != null) {
				//True insert
				if(temp1.getNextChunk() != null) {
					ChunkLink temp2 = temp1.getNextChunk(); 
					ChunkLink temp3 = input.getFirstChunk();
					ChunkLink temp4 = input.getLastChunk();
					temp1.setNextChunk(temp3);
					temp3.setPreviousChunk(temp1);
					temp2.setPreviousChunk(temp4);
					temp4.setNextChunk(temp2);
					
				}
				//This is more like an append, but we won't argue
				else {
					ChunkLink temp3 = input.getFirstChunk();
					ChunkLink temp4 = input.getLastChunk();
					temp1.setNextChunk(temp3);
					temp3.setPreviousChunk(temp1);
					this.lastChunk = temp4;
				}		
			}
			//Why wasn't there a current chunk set? Oh well, handling it
			else {//lets make some wild assumptions
				//reindex();
				this.currentChunk = this.firstChunk;
				ChunkLink temp3 = input.getFirstChunk();
				ChunkLink temp4 = input.getLastChunk();
				ChunkLink temp5 = this.getLastChunk();
				

				try {
					temp5.setNextChunk(temp3);
					temp3.setPreviousChunk(temp5);
					this.lastChunk = temp4;
				} catch (NullPointerException e) {
					//System.out.println("are we getting to the null pointer excepiton block?");
					//e.printStackTrace();
				}
				
				

			}
			this.chunkCount = this.chunkCount + input.getChunkCount();
			return true;
		}
		else return false;
	}
	
	/**
	 * Takes in a string, converts to a ChunkList, and then generates
	 * a ChunkList made up of StringChunkLink's. This method is
	 * dependent on the .insertAfterCurrentChunk(ChunkList input)
	 * method for passing over the generated ChunkList.
	 * 
	 * Remember to use .reindex() after this method
	 * @param input
	 * @return
	 */
	public boolean insertAfterCurrentChunk(String input) {
		
		if(input != null && input.length() > 0) {
			return insertAfterCurrentChunk(stringToChunks(input));
		}
		else return false;
	}
	
	/**
	 * Inserts an instantiated ChunkList instance directly before the
	 * currentChunk, and directly after the link the appears before
	 * the currentChunk.
	 * 
	 * Remember to use .reindex() after this method
	 * @param input
	 * @return
	 */
	public boolean insertBeforeCurrentChunk(ChunkList input) {
		if(input != null && input.getChunkCount() > 0) {
			ChunkLink temp1 = this.currentChunk;
			if(temp1.getPreviousChunk() != null) {
				ChunkLink temp2 = this.currentChunk.getPreviousChunk(); 
				ChunkLink temp3 = input.getFirstChunk();
				ChunkLink temp4 = input.getLastChunk();
				temp1.setPreviousChunk(temp4);
				temp4.setNextChunk(temp1);
				temp2.setPreviousChunk(temp3);
				temp3.setNextChunk(temp2);				
			}
			else {
				ChunkLink temp2 = input.getLastChunk();
				temp1.setPreviousChunk(temp2);
				temp2.setNextChunk(temp1);
				this.firstChunk = input.getFirstChunk();
			}
			this.chunkCount =+ input.getChunkCount();
			return true;
		}
		else return false;
	}
	
	
	/**
	 * Takes in a string, converts to a ChunkList, and then generates
	 * a ChunkList made up of StringChunkLink's. This method is
	 * dependent on the .insertBeforeCurrentChunk(ChunkList input)
	 * method for passing over the generated ChunkList.
	 * 
	 * Remember to use .reindex() after this method
	 * @param input
	 * @return
	 */
	public boolean insertBeforeCurrentChunk(String input) {
		
		if(input != null && input.length() > 0) {
			return insertBeforeCurrentChunk(stringToChunks(input));
		}
		else return false;
	}
		
	/**
	 * Sets the current chunk to the first position of the ChunkList
	 * Error safe
	 * @return True if position change actually occurred, False if not
	 */
	public boolean moveFirstChunk() {
		if(currentChunk != null &&
		   firstChunk != null &&
		   currentChunk != firstChunk) {
			currentChunk = firstChunk;
			return true;
		}
		else return false;
	}
	
	/**
	 * Sets the current chunk to the last position of the ChunkList
	 * Error safe
	 * @return True if position change actually occurred, False if not
	 */
	public boolean moveLastChunk() {
		if( currentChunk != null &&
			lastChunk != null &&
			currentChunk != lastChunk) {
			currentChunk = lastChunk;
			return true;
		}
		else return false;
	}

	/**
	 * Sets the current chunk to the next available chunk
	 * Error safe
	 * @return  True if position change actually occurred, False if not
	 */
	public boolean moveNextChunk() {
		if(chunkCount > 1 &&  
			currentChunk != null &&
			currentChunk.getNextChunk() != null) {
			currentChunk = currentChunk.getNextChunk();
			return true;
		}
		else return false;
		
	}

	/**
	 * Sets the current chunk to the previous available chunk
	 * Error safe
	 * @return  True if position change actually occurred, False if not
	 */
	public boolean movePreviousChunk() {
		if(chunkCount > 1 && currentChunk.getPreviousChunk() != null) {
			currentChunk = currentChunk.getPreviousChunk();
			return true;
		}
		else return false;
	}
	
	/**
	 * Used to reindex and/or repair the chunk list in cases where chunks are inserted or removed.
	 * Use if list mangling is suspected 
	 * Methods recommended for use of .reindex()
	 * 	- .insertBeforeCurrentChunk(String input)
	 * 	- .insertBeforeCurrentChunk(ChunkList input)
	 * 	- .insertAfterCurrentChunk(String input)
	 * 	- .insertAfterCurrentChunk(ChunkList input)
	 * 	- .removeAfterCurrentChunk(long length) 
	 *  - .removeFromCurrentChunk(long length)
	 * 	- .removeCurrentChunk()
	 *  - .append(input ChunkList)
	 */
	public void reindex() {
		//0 size case
		if(currentChunk == null && 
		   firstChunk == null &&
		   lastChunk == null) {
			chunkCount = 0;
		}
		//1 size case A
		else if((currentChunk.getNextChunk() == null &&
				 currentChunk.getPreviousChunk() == null) ||
				(currentChunk == firstChunk && 
				 currentChunk == lastChunk)) {
			currentChunk.setPrimeForReindex();
			currentChunk.setIndex(1);
			currentChunk.setNextChunk(null);
			currentChunk.setPreviousChunk(null);
			lastChunk = currentChunk;
			firstChunk = currentChunk;
			chunkCount = 1;
		}
		//1 size case B
		else if(firstChunk != null &&
				firstChunk.getNextChunk() == null) {
			firstChunk.setPreviousChunk(null);
			currentChunk = firstChunk;
			lastChunk = firstChunk;
			chunkCount = 1;
			currentChunk.setPrimeForReindex();
			currentChunk.setIndex(1);
		}
		//1 size case C
		else if(lastChunk != null && 
				lastChunk.getPreviousChunk() == null) {
			lastChunk.setNextChunk(null);
			currentChunk = lastChunk;
			firstChunk = lastChunk;
			chunkCount = 1;
			currentChunk.setPrimeForReindex();
			currentChunk.setIndex(1);
		}
		//+1 size - Set new index values for all chunks starting with 1
		else if(this.firstChunk != null) {
			long cnt = 0;
			
			this.moveFirstChunk();
			do {
				cnt++;
				currentChunk.setPrimeForReindex();
				currentChunk.setIndex(cnt);
			}
			while(this.moveNextChunk());
			chunkCount = cnt;
			this.moveFirstChunk();
		}	
	}
	
	/**
	 * Removes some number of chunks following the current chunk from
	 * left to right.
	 * 
	 * Remember to use .reindex() after this method.
	 * @param length
	 * @return
	 */
	public boolean removeAfterCurrentChunk(long length) {
		if(length > 0 && 
		   this.currentChunk != null  &&
		   this.currentChunk.getNextChunk() != null) {
			ChunkLink temp1 = this.currentChunk;
			ChunkLink temp2 = this.currentChunk.getNextChunk();
			long removed = 0;
			boolean endReached = false;
			
			for(long i = 0; i < length; i++) {	
				if(temp2.getNextChunk() != null) {
					temp2 = temp2.getNextChunk();
					removed++;
				}
				else {
					i = length; 
					endReached = true;
				}
			}
			
			if(endReached == false) {
				temp1.setNextChunk(temp2);
				temp2.setPreviousChunk(temp1);
			}
			else {
				temp1.setNextChunk(null);
				
				this.lastChunk = temp1;
			}
			
			this.chunkCount = this.chunkCount - removed;
			return true;
		} 
		else return false;
	}
	
	/**
	 * Removes all chunks up to the first encounter of the key search parameter from the right
	 * 
	 * @param key - The string value to be searched
	 * @param inclusive - a flag that indicate to also removing the key value found
	 * @return
	 */
	public long removeAfterKey(String key, boolean inclusive) {
		this.moveLastChunk();
		boolean startKeyOn = false;
		long removeCnt = 0;
		
		do {
			if(this.toString(key.length()).equals(key)) {
				startKeyOn = true;
			}
			else {
				removeCnt++;
			}	
		}
		while (startKeyOn == false && this.movePreviousChunk());
		
		if(startKeyOn == true && removeCnt > 0) {
			if(inclusive == true) {
				for(long i = 1; i < key.length(); i++) {
					this.moveNextChunk();
					removeCnt++;
				}
			}
			if(this.currentChunk == this.firstChunk) {
				this.currentChunk = null;
				this.lastChunk = null;
				this.firstChunk = null;
				this.chunkCount = 0;
			}
			else {
				this.currentChunk.setNextChunk(null);
				this.lastChunk = this.currentChunk;
				this.chunkCount = this.chunkCount - removeCnt;
			}

			
		}
		else
			removeCnt = 0;
		
		return removeCnt;
	}
	
	/**
	 * Removes all chunks up to the first encounter of the key search parameter from the left.
	 * 
	 *  
	 * @param key - A string value to be searched.
	 * @param inclusive - a boolean that indicate to also removing the key value found.
	 * @return
	 */
	public long removeBeforeKey(String key, boolean inclusive) {
		this.moveFirstChunk();
		boolean startKeyOn = false;
		long removeCnt = 0;
		do {
			if(this.toString(key.length()).equals(key)) {
				startKeyOn = true;
			}
			else {
				removeCnt++;
			}	
		}
		while (startKeyOn == false && this.moveNextChunk());
		if(startKeyOn == true && removeCnt > 0) {
			this.currentChunk.setPreviousChunk(null);
			this.firstChunk = this.currentChunk;
			if(inclusive == true) {
				this.removeFromCurrentChunk(key.length());
				removeCnt = removeCnt + key.length();
			}
			this.chunkCount = this.chunkCount - removeCnt;
		}
		else
			removeCnt = 0;
		return removeCnt;
	}
	
	/**
	 * Removes the link within an instantiated list identified by currentChunk and sets the
	 * current chunk to the previous chunk before it.  Run the .reindex() method to properly
	 * set new index positions. If current chunk is not an ending node, the new current
	 * position be will set to the previous position.
	 * 
	 * Remember to use .reindex() after this method
	 * @return
	 */
	public boolean removeCurrentChunk() {
		if(currentChunk != null) {
			if(currentChunk.getNextChunk() == null && 
			   currentChunk.getPreviousChunk() == null) {
				currentChunk = null;
				lastChunk = null;
				currentChunk = null;
			}
			else if(currentChunk.getNextChunk() == null || 
					currentChunk.getPreviousChunk() == null) {
				if(currentChunk.getNextChunk() != null) {
					currentChunk = currentChunk.getNextChunk();
					currentChunk.setPreviousChunk(null);
				}
				else if (this.currentChunk.getPreviousChunk() != null) {
					currentChunk = currentChunk.getPreviousChunk();
					currentChunk.setNextChunk(null);
				}
			}
			else {
				//ChunkLink temp = null;
				
				if(currentChunk == firstChunk && 
				   getCurrentChunk().getNextChunk() != null) {
					currentChunk = getCurrentChunk().getNextChunk();
					currentChunk.setPreviousChunk(null);
					firstChunk = currentChunk;
				}
				else if (currentChunk == lastChunk &&
						 getCurrentChunk().getPreviousChunk() != null) {
					currentChunk = currentChunk.getPreviousChunk();
					currentChunk.setNextChunk(null);
					lastChunk = currentChunk;
				}
				else if (getCurrentChunk().getNextChunk() != null &&
						 getCurrentChunk().getPreviousChunk() != null){
					//ChunkLink tempCurrent = currentChunk;
					ChunkLink tempNext = currentChunk.getNextChunk();
					ChunkLink tempPrev = currentChunk.getPreviousChunk();
					
					tempPrev.setNextChunk(tempNext);
					tempNext.setPreviousChunk(tempPrev);
					currentChunk = tempPrev;
					
				}
				else {
					System.out.println("why am i getting this?");
				}
				
			}
			
			this.chunkCount--;
			return true;
		}
		else return false;
	}
	
	/**
	 * Removes some number of chunks, including the current chunk, from left
	 * to right, and sets the new current chunk to the previous available 
	 * chunk.
	 * 
	 * Remember to use .reindex() after this method
	 * @param length
	 * @return false if specified length is invalid or currentChunk is null
	 */
	public boolean removeFromCurrentChunk(long length) {
		//make sure there is a positive length value and currentChunk is set
		if(length > 0 && this.currentChunk != null) {
			long removed = 0;
			boolean endReached = false;
			
			//Is there something behind me?
			if(this.currentChunk.getPreviousChunk() != null) {
				//take a step back with temp1
				ChunkLink temp1 = this.currentChunk.getPreviousChunk();
				ChunkLink temp2 = this.currentChunk;
				
				//chug away! length or bust!
				for(long i = 0; i < length; i++) {	
					if(temp2.getNextChunk() != null) {
						temp2 = temp2.getNextChunk();
						removed++;
					}
					else {
						i = length; 
						endReached = true;
					}
				}
				
				//make the link normally
				if(endReached == false) {
					temp1.setNextChunk(temp2);
					temp2.setPreviousChunk(temp1);
					this.currentChunk = temp1;
				}
				else {
					//Remove last chunk by moving the last chunk: see temp1 above
					this.currentChunk = temp1;
					this.currentChunk.setNextChunk(null);
					this.lastChunk = this.currentChunk;
				}
				this.chunkCount = this.chunkCount - removed;

			}
			//No, nothing is behind me
			else {
				//System.out.println("nothing behind " + length);
				ChunkLink temp1 = this.currentChunk;
				//chug away! length or bust!
				for(long i = 0; i < length; i++) {	
					if(temp1.getNextChunk() != null) {
						temp1 = temp1.getNextChunk();
						removed++;
					}
					else {
						i = length; 
						endReached = true;
					}
				}
				//well then, i guess we removed everything
				if(endReached == true) {
					this.currentChunk = null;
					this.firstChunk = null;
					this.lastChunk = null;
				}
				else {
					temp1.setPreviousChunk(null);
					currentChunk = temp1;
					this.firstChunk = temp1;
				}
				this.chunkCount = this.chunkCount - removed;
			}
			return true;
		}
		else return false;
	}
	
	/**
	 * This method removes a range of values in between a given pair of search key parameters.
	 * To-Do: Add inclusive option for each key
	 * 
	 * @param startKey - A string value that starts the remove range
	 * @param stopKey
	 * @return
	 */
	public long removeKeyRangeAll(String startKey, String stopKey) {
		this.moveFirstChunk();
		boolean startKeyOn = false;
		long removeCnt = 0;
		long totalRemoved = 0;
		
		//loop through entire list to remove each key-pair found
		do {
			if(this.toString(startKey.length()).equals(startKey) == true) {
				//System.out.println("start");
				startKeyOn = true;
				for(long i = 0; i < startKey.length(); i++) {
					this.moveNextChunk();
					removeCnt++;
				}
			}
			
			if(this.toString(stopKey.length()).equals(stopKey) == true) {
				//System.out.println("stop");
				startKeyOn = false;
				for(long i = 0; i < stopKey.length(); i++) {
					this.moveNextChunk();
					removeCnt++;
				}
			}				
			
			if(startKeyOn == true) {
				removeCnt++;
			}
			
			if(startKeyOn == false && removeCnt > 0) {
				//remove time
				this.setRelativeOffset(-removeCnt);			
				this.removeFromCurrentChunk(removeCnt);	
				startKeyOn = false;
				totalRemoved = removeCnt;
				removeCnt = 0;
			}
		}
		while(this.moveNextChunk());
		
		return totalRemoved;
	}
	
	/**
	 *	
	 * 
	 * @param startKey
	 * @param stopKey
	 * @param oldValue
	 * @param newValue
	 * @return
	 */
	public int replaceWithinKeyRangeAll(String startKey, String stopKey, String searchValue, String newValue) {
		int replaced = 0;
		boolean fndStart = false;
		
		this.moveFirstChunk();
		
		do {
			//System.out.println(fndStart);
			if(this.toString(searchValue.length()).equals(searchValue) == true && fndStart == true) {
				//noting that there's a more efficient way of handling the replacement
				//System.out.println(this.toString(50));
				this.removeFromCurrentChunk(searchValue.length());
				this.insertAfterCurrentChunk(newValue);
				
				//System.out.println("Rep - " + this.currentChunk.getIndex());
				replaced++;
			}
			
			if(this.toString(startKey.length()).equals(startKey) == true && fndStart == false) {
				fndStart = true;
				//used if startKey is length is > 1
				this.setRelativeOffset(startKey.length() - 1);
				
			}
			else if(this.toString(stopKey.length()).equals(stopKey) == true && fndStart == true) {
				fndStart = false;
				//System.out.println("Off - " + this.currentChunk.getIndex());
				for(int i = 0; i < stopKey.length() - 1; i++) this.moveNextChunk();	
			}
		}
		while(this.moveNextChunk());
		
		return replaced;
	}
	
	public int replaceOutsideKeyRangeAll(String startKey, String stopKey, String searchValue, String newValue) {
		int replaced = 0;
		boolean fndStart = false;
		
		this.moveFirstChunk();
		
		do {
			System.out.print(fndStart + "  " + this.toString(searchValue.length()) + "  " + this.toString(searchValue.length()).length() + " - " );
			
			if(this.toString(startKey.length()).equals(startKey) == true && fndStart == false) {
				fndStart = true;
				System.out.println("On  - " + this.currentChunk.getIndex());
				//used if startKey is length is > 1
				this.setRelativeOffset(startKey.length() - 1);
				
			}		
			else if(this.toString(stopKey.length()).equals(stopKey) == true && fndStart == true) {
				fndStart = false;
				//System.out.println("Off - " + this.currentChunk.getIndex());
				for(int i = 0; i < stopKey.length() - 1; i++) this.moveNextChunk();	
			}			
			
			
			System.out.println(fndStart + "  " + this.toString(searchValue.length()) + "  " + this.toString(searchValue.length()).length());
			if(this.toString(searchValue.length()).equals(searchValue) == true && fndStart == false) {
				//noting that there's a more efficient way of handling the replacement
				System.out.println(this.toString(50));
				this.removeFromCurrentChunk(searchValue.length());
				this.insertAfterCurrentChunk(newValue);
				
				//System.out.println("Rep - " + this.currentChunk.getIndex());
				replaced++;
			}
		}
		while(this.moveNextChunk());
		return replaced;
	}
	
	public String extractKeyRangeFromCurrentChunk(String startKey, String stopKey) {
		return "";
	}
	
	/**
	 * Searches and replaces all encounters of the searchValue to the newValue.
	 * Intended use includes preprocessing methods.  This is a case sensitive 
	 * method.  Take care of case-handling outside this method.  If any 
	 * replacements occur, .reindex() is invoked.
	 * 
	 * @param searchValue
	 * @param newValue
	 * @return the number of replacements made returned on a full pass of a 
	 * given list, or -1 if list is invalid and may need .reindex()
	 * 
	 */
	public long replaceAll(String searchValue, String newValue) {
		this.moveFirstChunk();
		long replaced = 0;
		
		if(searchValue.equals(newValue) != true ) {
			do {
				if(this.toString(searchValue.length()).equals(searchValue) == true) {
					//noting that there's a more efficient way of handling the replacement
					this.removeFromCurrentChunk(searchValue.length());
					this.insertAfterCurrentChunk(newValue);
					replaced++;
				}
			}
			while(this.moveNextChunk());
			
			if(replaced > 0) {
				this.reindex();
			}
		}
		return replaced;
	}
	
	/**
	 * Searches for a string value from the current position within the list and stops
	 * at first instance.  Returns true if value is found, and current position will 
	 * have changed.  Returns false if did not encounter searched value, and current
	 * position will remain the same.
	 * 
	 * @param searchValue
	 * @return boolean
	 */
	public boolean findFromCurrentPosition(String searchValue) {
		ChunkLink placeHolder = this.currentChunk;
		do {}
		while(this.toString(searchValue.length()).equals(searchValue) == false &&
				this.moveNextChunk());
		if(this.toString(searchValue.length()).equals(searchValue) == true) {
			return true;
		}
		else {
			//set current chunk back to where search started if search failed
			this.currentChunk = placeHolder;
			return false;
		}
	}
	
	
	public boolean findFromCurrentPosition(String searchValue, boolean setAfterSearchValue) {
		ChunkLink placeHolder = this.currentChunk;
		do {}
		while(this.toString(searchValue.length()).equals(searchValue) == false &&
				this.moveNextChunk());
		if(this.toString(searchValue.length()).equals(searchValue) == true) {
			if(setAfterSearchValue == true) {
				this.setRelativeOffset(searchValue.length());
			}
			return true;
		}
		else {
			//set current chunk back to where search started if search failed
			this.currentChunk = placeHolder;
			return false;
		}
	}
	
	/**
	 * Moves the current chunk position from the left to the specified offset number
	 * of positions.  This method is index-independent and does not rely on the
	 * .reindex() method to perform properly.  This method only depends of a non-null
	 * .getNextChunk() to proceed as far as the list allows.
	 * 
	 * @param offset
	 * @return the number of positions actually moved or -1 if offset is invalid
	 */
	public long setAbsoluteOffset(long offset) {
		long moved = 0;
		if(this.firstChunk != null &&
		   offset > 0) {
			while(moved < offset && 
				  this.currentChunk != this.lastChunk) {
				this.moveNextChunk();
				moved++;
			}
			return moved;
		} 
		else return -1;
	}
	
	/**
	 * If the chunk list is properly indexed and not mangled, this method accepts an index
	 * value, and sets the current chunk, or the list cursor, to match the chunk that holds
	 * the indexed value.  Use the method .reindex() prior to ensure the list is properly
	 * indexed in cases where links have been added, removed, or replaced.
	 * @param targetPosition
	 * @return return value of targetPosition index value was found, or -1 if 
	 * targetPosition is invalid
	 */
	public long setPosition(long targetPosition) {
		if(targetPosition > 0 && this.firstChunk != null) {
			ChunkLink temp = null;
			if(this.currentChunk != null) temp = currentChunk;
			else temp = firstChunk;
			
			this.moveFirstChunk();
			while(this.currentChunk.getIndex() != targetPosition &&
				  this.currentChunk != lastChunk) {
				this.moveNextChunk();
			}
			
			if(this.currentChunk.getIndex() == targetPosition) {
				return this.currentChunk.getIndex();
			}
			else {
				currentChunk = temp;
				return -1;
			}
		}
		return -1;
	}
	
	/**
	 * Moves forward (to the right) or backwards (to the left) the specified offset
	 * number of positions.  The offset specified can be be negative (backwards) or 
	 * positive (forward) as far as the list allows.
	 * 
	 * @param offset
	 * @return the number of positions actually moved.
	 */
	public long setRelativeOffset(long offset) {
		long moved = 0;
		if(offset > 0) {
			while(moved < offset &&
				  this.currentChunk != this.lastChunk) {
				this.moveNextChunk();
				moved++;
			}	
		}
		if(offset < 0) {
			while(moved > offset && 
				  this.currentChunk != this.firstChunk) {
				this.movePreviousChunk();
				moved--;
			}
		}
		return moved;
	}
	
	/**
	 * Quick but dangerous.
	 * This functions relies on its caller to make sure the chunk being assigned
	 * truly comes from the list it live within.  Use this option when lists are big
	 * and .setPosition becomes expensive to call because it starts from first chunk
	 * and works it's way though to find position index
	 * @param newCurrentPosistion
	 * @return
	 */
	public boolean setCurrentChunk(ChunkLink newCurrentChunk) {
		if(newCurrentChunk != null) {
			this.currentChunk = newCurrentChunk;
			return true;
		}
		else
		return false;
	}
	
	/**
	 * Returns a string on the fly from the current chunk (inclusively) at a specified length.
	 * If the specified length is greater than the number of chunks left from the current
	 * position, a truncated string will be returned.  The current position is maintained
	 * 
	 * @param length
	 * @return
	 */
	public String toString(long length) {
		StringBuilder output = new StringBuilder("");
		if (length > 0 && 
			getCurrentChunk() != null) {
			
			output = new StringBuilder();
			StringChunkLink temp1 = (StringChunkLink)getCurrentChunk();
			long moved = 0;
			

			//otherwise this will take care of it
			while(moved < length && 
				  temp1 != this.lastChunk) {
				//.getValue() in the context of StringChunkLink is always String
				//outputString = outputString.concat(temp1.getValue());
				output.append(temp1.getValue());
				if(temp1.getNextChunk() != null) {
					temp1 = (StringChunkLink)temp1.getNextChunk();
				}
				moved++;
			}
			
			if (temp1.getNextChunk() == null && length != moved) {
				output.append(temp1.getValue());
			}

		}
		//return outputString;
		return output.toString();
	}
	
	private long chunkCount = 0;
	private ChunkLink firstChunk;
	private ChunkLink lastChunk;
	private ChunkLink currentChunk;
}


