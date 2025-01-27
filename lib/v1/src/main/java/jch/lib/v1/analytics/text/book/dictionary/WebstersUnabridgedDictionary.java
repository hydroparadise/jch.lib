package jch.lib.analytics.text.book.dictionary;

//import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

//import jch.lib.analytics.text.StringStatEntryList;
//import jch.lib.analytics.text.StringStatEntryList.SortByFromCount;
//import jch.lib.analytics.text.book.bible.KingJamesVersionBible;
import jch.lib.list.ChunkLink;
import jch.lib.list.ChunkList;
import jch.lib.list.StringChunkLink;

public class WebstersUnabridgedDictionary {
	
	
	public static TreeMap<String, String> abKeyMap1;
	public static TreeMap<String, String> abKeyMap2;
	public static TreeMap<String, String> abKeyMap3;
	public static TreeMap<String, String> PartsOfSpeech;
	
	public static final String seedLocation = "lang/eng/seed/published/project_gutenburg/txt/";
	public static final String seedFile = "29765-8.txt";
	public static ChunkList clWUD;
	public WebstersUnabridgedDictionary() {
		// TODO Auto-generated constructor stub
	}

	static {
		loadWUD();
		initKeyMaps();
	}
	
	static void loadWUD() {
		clWUD = ChunkList.loadFile(seedLocation + seedFile);
		clWUD.removeBeforeKey("Produced by Graham Lawrence", true);
		clWUD.removeAfterKey("End of Project Gutenberg's Webster's Unabridged Dictionary, by Various", false);
		clWUD.reindex();
	}
	
	public static void sayHi() {
		System.out.println("Welcome to the Webster's Unabridged Dictionary!");	
		System.out.println(clWUD.getChunkCount());
	}
	
	public static WordDictionary buildDictionary() throws IOException {
		WordDictionary output = new WordDictionary();
		WebstersUnabridgedDictionary.clWUD.moveFirstChunk();
	
		ChunkList cl1 = null;
		StringChunkLink t1;
		StringChunkLink t2;		
		StringChunkLink t3;
		ChunkLink sc1;
		ChunkLink ec1;
		String ts1;
		String ts2;
		WordInstance word;
		StringBuilder strTest1;
		StringBuilder strTest2;
		StringBuilder st1;
		WordDefinition wDef;
		
		long start = 0;
		long extent = 0;
		long len = 0;
		int defCount;
		//int state = 0;
		int n;
		
		int DEFN = -1;
		int NUM = -1;
		int ALPH = -1;

		/***
		 * Section 1
		 * Words and Word Headers (Meta Data)
		 */
		//bookmark word locations and word headers
		while(WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition("\r\n\r\n", true) == true) {
			
			t1 = (StringChunkLink)WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
			WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition("\r\n");
			t2 = (StringChunkLink)WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
			ts1 = t1.toString(t2.getIndex()-t1.getIndex());
			
			word = new WordInstance();
			
			//some tests to make sure we are at a new word
			if(ts1.toUpperCase().equals(ts1) &&
				word.setRefLink(t1, t2) &&
				tryParseDouble(ts1) == false) {

				output.Words.add(word);
				
				//now looking for definition header
				WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition("\r\n\r\n", false);
				if(abKeyMap1.get( 
						((StringChunkLink)(t2)).toString(1)) == "W" ) { 
					t2 = (StringChunkLink) t2.getNextChunk();
				} 
				word.WordHeader.setRefLink(t2, WebstersUnabridgedDictionary.clWUD.getCurrentChunk());
				//System.out.println(word.WordHeader.getCleanRefValue());
				
				//Now see if there's a Synonym footer

			}
		}
		
		
		
		/***
		 * Section 2
		 * Parsing out definitions that appears in multiple formats
		 */		
		
		strTest1 = new StringBuilder("");
		strTest2 = new StringBuilder("");
		System.out.println(output.Words.size() + " words.");

		sc1 = null;
		ec1 = null;

		for(int i = 0; i < output.Words.size(); i++) {

			if(output.Words.get(i).WordHeader.getRefLinkExtent() != null) {
				//getting start of block and extent of block
				start = output.Words.get(i).WordHeader.getRefLinkExtent().getIndex();
				sc1 = output.Words.get(i).WordHeader.getRefLinkExtent();
				
				if(i + 1 < output.Words.size()) {
					extent = output.Words.get(i + 1).getRefLink().getIndex();
					ec1 = output.Words.get(i + 1).getRefLink();
				}
				else {
					ec1 = WebstersUnabridgedDictionary.clWUD.getLastChunk();
					extent =  WebstersUnabridgedDictionary.clWUD.getLastChunk().getIndex();
				}
				
				len = extent - start;
				
				strTest1.setLength(0);
				strTest2.setLength(0);
				
				//lets grab our string samples
				strTest1.append( ((StringChunkLink)
						(output.Words.get(i).WordHeader.getRefLinkExtent())).toString(len));
				strTest2.append(output.Words.get(i).WordHeader.getCleanRefValue());
				
				//System.out.println(strTest2);
				//let's gather some info
				NUM = strTest1.indexOf("\r\n1. ");
				DEFN = strTest1.indexOf("Defn: ");
				ALPH = strTest2.indexOf("(a) ");	
				
				n = 0;
				st1 = new StringBuilder("");
				defCount = 0;
				
				//reusing from above
				t1=null;
				t2=null;
				
				//Scenario 1
				if(NUM > 0) {
					
					WebstersUnabridgedDictionary.clWUD.setCurrentChunk(sc1);
					n = 1;
					st1= new StringBuilder(String.format("%d", n) + ".");
					while(WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition(st1.toString(),true) &&
					      WebstersUnabridgedDictionary.clWUD.getCurrentChunk().getIndex() < extent) {
						t1 = (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
						
						if(t2 != null) {
							wDef = new WordDefinition();
							defCount++;

							t3 = t1;
							for(int r = 0; r < st1.length(); r++) {
								t3 = (StringChunkLink) t3.getPreviousChunk();
							}
							
							wDef.setRefLink(t2, t3);
							wDef.setDefinitionNumber(defCount);
							output.Words.get(i).WordDefinitions.add(wDef);
						}
						
						t2 = t1;
						st1.setLength(0);
						st1.append(String.format("%d", ++n) + ".");
					}
					
					if(t2 != null) {
						wDef = new WordDefinition();
						defCount++;

						WebstersUnabridgedDictionary.clWUD.setCurrentChunk(ec1);
						t1= (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
						wDef.setRefLink(t2, t1);
						wDef.setDefinitionNumber(defCount);
						output.Words.get(i).WordDefinitions.add(wDef);
					}
				}
				
				//Scenario 2
				else if (NUM == -1 && DEFN > 0) {
					WebstersUnabridgedDictionary.clWUD.setCurrentChunk(sc1);
					n = 1;
					st1 = new StringBuilder("Defn: ");
					while(WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition(st1.toString(),true) &&
						      WebstersUnabridgedDictionary.clWUD.getCurrentChunk().getIndex() < extent) {
						
						t1 = (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
						
						if(t2 != null) {
							wDef = new WordDefinition();
							defCount++;

							
							wDef.setRefLink(t2, t1);
							wDef.setDefinitionNumber(defCount);
							output.Words.get(i).WordDefinitions.add(wDef);
						}	
						
						t2 = t1;
						n++;
					}
					
					if(t2 != null) {
						wDef = new WordDefinition();
						defCount++;

						WebstersUnabridgedDictionary.clWUD.setCurrentChunk(ec1);
						t1= (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
						wDef.setRefLink(t2, t1);
						wDef.setDefinitionNumber(defCount);
						output.Words.get(i).WordDefinitions.add(wDef);
					}
					
				} 
				
				//Scenario 3
				else if (NUM == -1 && DEFN == -1 && ALPH > 0) {

					WebstersUnabridgedDictionary.clWUD.setCurrentChunk(output.Words.get(i).WordHeader.getRefLink());
					n = 97; //a
					st1 = new StringBuilder( "(" + Character.toString((char)n) + ") ");
					while(WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition(st1.toString(),true) &&
						      WebstersUnabridgedDictionary.clWUD.getCurrentChunk().getIndex() < extent) {
						t1 = (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
						
						if(t2 != null) {
							wDef = new WordDefinition();
							defCount++;

							t3 = t1;
							for(int r = 0; r < st1.length(); r++) {
								t3 = (StringChunkLink) t3.getPreviousChunk();
							}
							
							wDef.setRefLink(t2, t1);
							wDef.setDefinitionNumber(defCount);
							output.Words.get(i).WordDefinitions.add(wDef);
						}
						
						st1.setLength(0);
						st1.append("(" + Character.toString((char)++n) + ") ");
						t2 = t1;
					}
					
					if(t2 != null) {
						wDef = new WordDefinition();
						defCount++;

						WebstersUnabridgedDictionary.clWUD.setPosition(extent);
						t1= (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
						wDef.setRefLink(t2, t1);
						wDef.setDefinitionNumber(defCount);
						output.Words.get(i).WordDefinitions.add(wDef);
					}
					
					WebstersUnabridgedDictionary.clWUD.setCurrentChunk(ec1);
				}
				
				DEFN = -1;
				NUM = -1;
				ALPH = -1;
			}

			//System.out.println(output.Words.get(i).getCleanRefValue());
			//Set word footer which commonly appears as a synonym list
			wDef = null;
			for(int k = 0; k < output.Words.get(i).WordDefinitions.size();k++) {
				wDef = (WordDefinition) output.Words.get(i).WordDefinitions.get(k);
				
				if(abKeyMap2.get( 
						((StringChunkLink)(wDef.getRefLink())).toString(1)) == "W" ) { 
					wDef.setRefLink(wDef.getRefLink().getNextChunk());
				}
				
				//let's see this thing has a synonym footer
				if(wDef != null &&
					wDef.getCleanRefValue().indexOf("Syn.") >= 0) {
					//System.out.println("Synonym found!!!");
					//bookmark
					t1 = (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
					
					//find this bugger
					WebstersUnabridgedDictionary.clWUD.setCurrentChunk(wDef.getRefLink());
					WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition("Syn.", false);

					
					//grab bookmark to set new extent to definition
					t3 = (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
					
					WebstersUnabridgedDictionary.clWUD.findFromCurrentPosition("Syn.", true);
					t2 = (StringChunkLink) WebstersUnabridgedDictionary.clWUD.getCurrentChunk();
					output.Words.get(i).WordFooter.setRefLink(t2, wDef.getRefLinkExtent());

					System.out.println(output.Words.get(i).WordFooter.getCleanRefValue());
					
					//remember the bookmark? now bring it back
					WebstersUnabridgedDictionary.clWUD.setCurrentChunk(t1);
					//new extent to definition
					wDef.setRefLinkExtent(t3);
					
				}
				
				if(abKeyMap2.get( 
						((StringChunkLink)(wDef.getRefLinkExtent())).toString(1)) == "W" ) { 
					wDef.setRefLinkExtent(wDef.getRefLinkExtent().getPreviousChunk());
				}
			}
			
		}
		
		/***
		 * Section 3
		 * Breaking apart multiple words 
		 */
		clWUD.moveFirstChunk();
		
		t1 = null;
		t2 = null;
		
		for(int i = 0; i < output.Words.size(); i++) {
			strTest1.setLength(0);
			strTest1.append(output.Words.get(i).getCleanRefValue());
			//int cCnt = 1;
			if(strTest1.indexOf(";") >= 0) {
				
				cl1 = ChunkList.stringToChunks(strTest1.toString());
				t1 = (StringChunkLink) cl1.getFirstChunk();
				
				while(cl1.findFromCurrentPosition("; ",true)){
					t2 = (StringChunkLink) cl1.getCurrentChunk();
					output.Words.get(i).Tokens.add(
							new WordToken(t1.toString(t2.getIndex() - t1.getIndex() - 2)));
					
					t1 = t2;
				}
				
				t2 = (StringChunkLink) cl1.getLastChunk();
				output.Words.get(i).Tokens.add(
						new WordToken(t1.toString(t2.getIndex() - t1.getIndex() + 1)));
			}	
			else {
				output.Words.get(i).Tokens.add(
						new WordToken(strTest1.toString()));
			}
			
		}
			
		/***
		 * Section 4
		 * Get Parts of Speech
		 */
		cl1 = null;
		for(int i = 0; i < output.Words.size(); i ++) {
			
			@SuppressWarnings("unused")
			int posCnt=0;
			
			ts1 = output.Words.get(i).getCleanRefValue();
			System.out.println(ts1);
			ts1 = output.Words.get(i).WordHeader.getCleanRefValue();
			
			for(Map.Entry<String, String> entry : PartsOfSpeech.entrySet() ) {
				String f1 = entry.getKey();
				String f2 = entry.getValue();
				
				if(ts1.indexOf(f1) >= 0) {
					posCnt++;
					//if(posCnt>1) System.out.print(", ");
					//System.out.print(ts1.indexOf(f1) + " - " + f2);
					
					WordPartsOfSpeech pos = new WordPartsOfSpeech();
					pos.setAbrev(f1);
					pos.setPartOfSpeech(f2);
					pos.setOrderNumber(ts1.indexOf(f1));
					output.Words.get(i).PartsOfSpeech.add(pos);
				}
			}
			//System.out.println();
			Collections.sort(output.Words.get(i).PartsOfSpeech,  
					new WordPartsOfSpeech(). new SortByOrderNumber());
		}
		
		
		/***
		 * Section 5
		 * Build word map from work tokens to word instances
		 */
		for(int i = 0; i < output.Words.size(); i ++) {		
			for(int k = 0; k < output.Words.get(i).Tokens.size(); k++) {
				ts1 = output.Words.get(i).Tokens.get(k).getTokenValue();
				if(output.WordMap.get(ts1) == null) {
					output.WordMap.put(ts1, new ArrayList<WordInstance>());
				}
				output.WordMap.get(ts1).add(output.Words.get(i));
			}
		}
		
		/***
		 * Section 6
		 * Parse out Synonyms
		 */
		ArrayList<String> synSet = new ArrayList<String>();
		for(int i = 0; i < output.Words.size(); i ++) {	
			
			System.out.println("Syn: ~" + output.Words.get(i).getCleanRefValue());
			ts1=null;
			if(output.Words.get(i).WordFooter != null &&
			   output.Words.get(i).WordFooter.getCleanRefValue() != null &&
			   output.Words.get(i).WordFooter.getCleanRefValue().length() > 0) {
				
				ts1 = output.Words.get(i).WordFooter.getCleanRefValue();
				cl1 = ChunkList.stringToChunks(ts1);
				
				//for --
				if(cl1 != null) {
					cl1.moveFirstChunk();
					if(cl1.findFromCurrentPosition("--", true)) {
						do {
							t1 = (StringChunkLink) cl1.getCurrentChunk();
							if(cl1.findFromCurrentPosition(".", true)) {
								t2 = (StringChunkLink) cl1.getCurrentChunk();
							}
							else {
								t2 = (StringChunkLink) cl1.getLastChunk();
							}
							
							//System.out.println(t1.toString(t2.getIndex() - t1.getIndex()));
							synSet.add(t1.toString(t2.getIndex() - t1.getIndex()));
						}
						while(cl1.findFromCurrentPosition("--", true));
						t2 = (StringChunkLink) cl1.getCurrentChunk();
					}
					
					if(cl1.findFromCurrentPosition(". ", true)) {
						
						do {

							t1 = (StringChunkLink) cl1.getCurrentChunk();
							//System.out.println(t2.toString(t1.getIndex() - t2.getIndex()));
							synSet.add(t2.toString(t1.getIndex() - t2.getIndex()));
							t2 = t1;
						}
						while(cl1.findFromCurrentPosition(".", true));
					}
					
					for(int k = 0; k < synSet.size(); k++) {
						
						cl1 = ChunkList.stringToChunks(synSet.get(k));
						if(cl1 != null) {
						
							int SCOL = 0;
							int SPC = 0;
							int COM = 0;
							int PRD = 0;
							
							//profile string for next step
							do {
								//System.out.println(cl1.toString(1));
								switch(cl1.toString(1)) {
								case " " : SPC++; break;
								case ";" : SCOL++; break;
								case "," : COM++; break;
								case "." : PRD++; break;
								}
								
							}
							while (cl1.moveNextChunk());
							
							/*
							System.out.println("SCOL: " + SCOL);
							System.out.println("SPC:  " + SPC);
							System.out.println("COM:  " + COM);
							System.out.println("PRD:  " + PRD);
							*/
							
							if(SPC >= 0 && SPC <= 2 && COM == 0 && SCOL == 0 && PRD >= 0 ) {
								//System.out.println("Scenario 0: Single word??  ");
								ts2 = cl1.toString(cl1.getChunkCount()).trim().toUpperCase();
								if(ts2.equals(".") == false) {
									//System.out.println(ts2);
									output.Words.get(i).Synonyms.add(new WordToken(ts2));
								}
								
							} else
							if(SPC > 0 && SCOL > 0 && COM == 0 && PRD == 1 && 
									((double)SCOL / (double)SPC) >= .5) {
								//System.out.print("Scenario 1: Semicolons  ");
								// All; total; complete; entire; integral; undivided; uninjured; unimpaired; unbroken; healthy.
								cl1.moveFirstChunk();
								ArrayList<String> synResults = ChunkList.toStringList(cl1, new String[]{";","."});
								
								//System.out.println(synResults.size() +  " words.");
								for(int l = 0; l < synResults.size() ; l++) {
									ts2 = synResults.get(l).trim().toUpperCase();
									if(ts2.equals(".") == false) {
										//System.out.println(ts2);
										output.Words.get(i).Synonyms.add(new WordToken(ts2));
									}
								}
								
							} else 
							if(SCOL == 0 && COM > 0 && SPC > 0 && PRD == 1 && 
									((double)SCOL / (double)SPC) >= .5) {
								//System.out.print("Scenario 2: Commas  ");
								// Whole, Total, Entire, Complete.
								cl1.moveFirstChunk();
								ArrayList<String> synResults = ChunkList.toStringList(cl1, new String[]{",","."});
								
								for(int l = 0; l < synResults.size() ; l++) {
									ts2 = synResults.get(l).trim().toUpperCase();
									if(ts2.equals(".") == false) {
										//System.out.println(ts2);
										output.Words.get(i).Synonyms.add(new WordToken(ts2));
									}
								}	
							}
							else {
								//System.out.println("Well, well, well.");
								//System.out.print("SCOL: " + SCOL);
								//System.out.print("   SPC:  " + SPC);
								//System.out.print("   COM:  " + COM);
								//System.out.println("   PRD:  " + PRD);
								//cl1.moveFirstChunk();
								//System.out.println(cl1.toString(cl1.getChunkCount()));
							}
						}
					}
				}
			}
		}
		return output;
	}
	
	
	
	//helper function to try a parse without throwing exception in main code
	static boolean tryParseDouble(String value) {  
	     try {  
	         Double.parseDouble(value);  
	         return true;  
	      } catch (NumberFormatException e) {  
	         return false;  
	      }  
	}
	
	static void initKeyMaps() {
		
		/*
		 * W - White Space
		 * E - End of Sentence
		 * C - Character
		 * c - Lower Case Character
		 * R - Return or Newline
		 * S - Special Character
		 * N - Numerical
		 */
		
		abKeyMap1 = new TreeMap<String, String>();
		abKeyMap2 = new TreeMap<String, String>();
		abKeyMap3 = new TreeMap<String, String>();
		PartsOfSpeech = new TreeMap<String, String>(); 
		
		//pastes in a spreadsheet
		abKeyMap1.put("\t","W");	abKeyMap2.put("\t","W");	abKeyMap3.put("\t","W");	//9	\t
		abKeyMap1.put("\n","W");	abKeyMap2.put("\n","R");	abKeyMap3.put("\n","R");	//10	\n
		abKeyMap1.put("\r","W");	abKeyMap2.put("\r","R");	abKeyMap3.put("\r","R");	//13	\r
		abKeyMap1.put(" ","W");	abKeyMap2.put(" ","W");	abKeyMap3.put(" ","W");	//32	 
		abKeyMap1.put("!","E");	abKeyMap2.put("!","E");	abKeyMap3.put("!","E");	//33	!
		abKeyMap1.put("\"","S");	abKeyMap2.put("\"","S");	abKeyMap3.put("\"","S");	//34	\"
		abKeyMap1.put("#","S");	abKeyMap2.put("#","S");	abKeyMap3.put("#","P");	//35	#
		abKeyMap1.put("$","S");	abKeyMap2.put("$","S");	abKeyMap3.put("$","S");	//36	$
		abKeyMap1.put("%","S");	abKeyMap2.put("%","S");	abKeyMap3.put("%","S");	//37	%
		abKeyMap1.put("&","S");	abKeyMap2.put("&","S");	abKeyMap3.put("&","S");	//38	&
		abKeyMap1.put("'","S");	abKeyMap2.put("'","S");	abKeyMap3.put("'","P");	//39	'
		abKeyMap1.put("(","S");	abKeyMap2.put("(","S");	abKeyMap3.put("(","P");	//40	(
		abKeyMap1.put(")","S");	abKeyMap2.put(")","S");	abKeyMap3.put(")","P");	//41	)
		abKeyMap1.put("*","S");	abKeyMap2.put("*","S");	abKeyMap3.put("*","S");	//42	*
		abKeyMap1.put("+","S");	abKeyMap2.put("+","S");	abKeyMap3.put("+","S");	//43	+
		abKeyMap1.put(",","S");	abKeyMap2.put(",","S");	abKeyMap3.put(",","P");	//44	,
		abKeyMap1.put("-","S");	abKeyMap2.put("-","S");	abKeyMap3.put("-","P");	//45	-
		abKeyMap1.put(".","E");	abKeyMap2.put(".","E");	abKeyMap3.put(".","E");	//46	.
		abKeyMap1.put("/","S");	abKeyMap2.put("/","S");	abKeyMap3.put("/","S");	//47	/
		abKeyMap1.put("0","N");	abKeyMap2.put("0","N");	abKeyMap3.put("0","N");	//48	0
		abKeyMap1.put("1","N");	abKeyMap2.put("1","N");	abKeyMap3.put("1","N");	//49	1
		abKeyMap1.put("2","N");	abKeyMap2.put("2","N");	abKeyMap3.put("2","N");	//50	2
		abKeyMap1.put("3","N");	abKeyMap2.put("3","N");	abKeyMap3.put("3","N");	//51	3
		abKeyMap1.put("4","N");	abKeyMap2.put("4","N");	abKeyMap3.put("4","N");	//52	4
		abKeyMap1.put("5","N");	abKeyMap2.put("5","N");	abKeyMap3.put("5","N");	//53	5
		abKeyMap1.put("6","N");	abKeyMap2.put("6","N");	abKeyMap3.put("6","N");	//54	6
		abKeyMap1.put("7","N");	abKeyMap2.put("7","N");	abKeyMap3.put("7","N");	//55	7
		abKeyMap1.put("8","N");	abKeyMap2.put("8","N");	abKeyMap3.put("8","N");	//56	8
		abKeyMap1.put("9","N");	abKeyMap2.put("9","N");	abKeyMap3.put("9","N");	//57	9
		abKeyMap1.put(":","S");	abKeyMap2.put(":","S");	abKeyMap3.put(":","S");	//58	:
		abKeyMap1.put(";","S");	abKeyMap2.put(";","S");	abKeyMap3.put(";","S");	//59	;
		abKeyMap1.put("<","S");	abKeyMap2.put("<","S");	abKeyMap3.put("<","S");	//60	<
		abKeyMap1.put("=","S");	abKeyMap2.put("=","S");	abKeyMap3.put("=","S");	//61	=
		abKeyMap1.put(">","S");	abKeyMap2.put(">","S");	abKeyMap3.put(">","S");	//62	>
		abKeyMap1.put("?","E");	abKeyMap2.put("?","E");	abKeyMap3.put("?","E");	//63	?
		abKeyMap1.put("@","S");	abKeyMap2.put("@","S");	abKeyMap3.put("@","S");	//64	@
		abKeyMap1.put("A","C");	abKeyMap2.put("A","C");	abKeyMap3.put("A","C");	//65	A
		abKeyMap1.put("B","C");	abKeyMap2.put("B","C");	abKeyMap3.put("B","C");	//66	B
		abKeyMap1.put("C","C");	abKeyMap2.put("C","C");	abKeyMap3.put("C","C");	//67	C
		abKeyMap1.put("D","C");	abKeyMap2.put("D","C");	abKeyMap3.put("D","C");	//68	D
		abKeyMap1.put("E","C");	abKeyMap2.put("E","C");	abKeyMap3.put("E","C");	//69	E
		abKeyMap1.put("F","C");	abKeyMap2.put("F","C");	abKeyMap3.put("F","C");	//70	F
		abKeyMap1.put("G","C");	abKeyMap2.put("G","C");	abKeyMap3.put("G","C");	//71	G
		abKeyMap1.put("H","C");	abKeyMap2.put("H","C");	abKeyMap3.put("H","C");	//72	H
		abKeyMap1.put("I","C");	abKeyMap2.put("I","C");	abKeyMap3.put("I","C");	//73	I
		abKeyMap1.put("J","C");	abKeyMap2.put("J","C");	abKeyMap3.put("J","C");	//74	J
		abKeyMap1.put("K","C");	abKeyMap2.put("K","C");	abKeyMap3.put("K","C");	//75	K
		abKeyMap1.put("L","C");	abKeyMap2.put("L","C");	abKeyMap3.put("L","C");	//76	L
		abKeyMap1.put("M","C");	abKeyMap2.put("M","C");	abKeyMap3.put("M","C");	//77	M
		abKeyMap1.put("N","C");	abKeyMap2.put("N","C");	abKeyMap3.put("N","C");	//78	N
		abKeyMap1.put("O","C");	abKeyMap2.put("O","C");	abKeyMap3.put("O","C");	//79	O
		abKeyMap1.put("P","C");	abKeyMap2.put("P","C");	abKeyMap3.put("P","C");	//80	P
		abKeyMap1.put("Q","C");	abKeyMap2.put("Q","C");	abKeyMap3.put("Q","C");	//81	Q
		abKeyMap1.put("R","C");	abKeyMap2.put("R","C");	abKeyMap3.put("R","C");	//82	R
		abKeyMap1.put("S","C");	abKeyMap2.put("S","C");	abKeyMap3.put("S","C");	//83	S
		abKeyMap1.put("T","C");	abKeyMap2.put("T","C");	abKeyMap3.put("T","C");	//84	T
		abKeyMap1.put("U","C");	abKeyMap2.put("U","C");	abKeyMap3.put("U","C");	//85	U
		abKeyMap1.put("V","C");	abKeyMap2.put("V","C");	abKeyMap3.put("V","C");	//86	V
		abKeyMap1.put("W","C");	abKeyMap2.put("W","C");	abKeyMap3.put("W","C");	//87	W
		abKeyMap1.put("X","C");	abKeyMap2.put("X","C");	abKeyMap3.put("X","C");	//88	X
		abKeyMap1.put("Y","C");	abKeyMap2.put("Y","C");	abKeyMap3.put("Y","C");	//89	Y
		abKeyMap1.put("Z","C");	abKeyMap2.put("Z","C");	abKeyMap3.put("Z","C");	//90	Z
		abKeyMap1.put("[","S");	abKeyMap2.put("[","S");	abKeyMap3.put("[","S");	//91	[
		abKeyMap1.put("\\","S");	abKeyMap2.put("\\","S");	abKeyMap3.put("\\","S");	//92	\
		abKeyMap1.put("]","S");	abKeyMap2.put("]","S");	abKeyMap3.put("]","S");	//93	]
		abKeyMap1.put("^","S");	abKeyMap2.put("^","S");	abKeyMap3.put("^","S");	//94	^
		abKeyMap1.put("_","S");	abKeyMap2.put("_","S");	abKeyMap3.put("_","S");	//95	_
		abKeyMap1.put("`","P");	abKeyMap2.put("`","P");	abKeyMap3.put("`","P");	//96	`
		abKeyMap1.put("a","C");	abKeyMap2.put("a","C");	abKeyMap3.put("a","c");	//97	a
		abKeyMap1.put("b","C");	abKeyMap2.put("b","C");	abKeyMap3.put("b","c");	//98	b
		abKeyMap1.put("c","C");	abKeyMap2.put("c","C");	abKeyMap3.put("c","c");	//99	c
		abKeyMap1.put("d","C");	abKeyMap2.put("d","C");	abKeyMap3.put("d","c");	//100	d
		abKeyMap1.put("e","C");	abKeyMap2.put("e","C");	abKeyMap3.put("e","c");	//101	e
		abKeyMap1.put("f","C");	abKeyMap2.put("f","C");	abKeyMap3.put("f","c");	//102	f
		abKeyMap1.put("g","C");	abKeyMap2.put("g","C");	abKeyMap3.put("g","c");	//103	g
		abKeyMap1.put("h","C");	abKeyMap2.put("h","C");	abKeyMap3.put("h","c");	//104	h
		abKeyMap1.put("i","C");	abKeyMap2.put("i","C");	abKeyMap3.put("i","c");	//105	i
		abKeyMap1.put("j","C");	abKeyMap2.put("j","C");	abKeyMap3.put("j","c");	//106	j
		abKeyMap1.put("k","C");	abKeyMap2.put("k","C");	abKeyMap3.put("k","c");	//107	k
		abKeyMap1.put("l","C");	abKeyMap2.put("l","C");	abKeyMap3.put("l","c");	//108	l
		abKeyMap1.put("m","C");	abKeyMap2.put("m","C");	abKeyMap3.put("m","c");	//109	m
		abKeyMap1.put("n","C");	abKeyMap2.put("n","C");	abKeyMap3.put("n","c");	//110	n
		abKeyMap1.put("o","C");	abKeyMap2.put("o","C");	abKeyMap3.put("o","c");	//111	o
		abKeyMap1.put("p","C");	abKeyMap2.put("p","C");	abKeyMap3.put("p","c");	//112	p
		abKeyMap1.put("q","C");	abKeyMap2.put("q","C");	abKeyMap3.put("q","c");	//113	q
		abKeyMap1.put("r","C");	abKeyMap2.put("r","C");	abKeyMap3.put("r","c");	//114	r
		abKeyMap1.put("s","C");	abKeyMap2.put("s","C");	abKeyMap3.put("s","c");	//115	s
		abKeyMap1.put("t","C");	abKeyMap2.put("t","C");	abKeyMap3.put("t","c");	//116	t
		abKeyMap1.put("u","C");	abKeyMap2.put("u","C");	abKeyMap3.put("u","c");	//117	u
		abKeyMap1.put("v","C");	abKeyMap2.put("v","C");	abKeyMap3.put("v","c");	//118	v
		abKeyMap1.put("w","C");	abKeyMap2.put("w","C");	abKeyMap3.put("w","c");	//119	w
		abKeyMap1.put("x","C");	abKeyMap2.put("x","C");	abKeyMap3.put("x","c");	//120	x
		abKeyMap1.put("y","C");	abKeyMap2.put("y","C");	abKeyMap3.put("y","c");	//121	y
		abKeyMap1.put("z","C");	abKeyMap2.put("z","C");	abKeyMap3.put("z","c");	//122	z
		abKeyMap1.put("{","S");	abKeyMap2.put("{","S");	abKeyMap3.put("{","S");	//123	{
		abKeyMap1.put("|","S");	abKeyMap2.put("|","S");	abKeyMap3.put("|","S");	//124	|
		abKeyMap1.put("}","S");	abKeyMap2.put("}","S");	abKeyMap3.put("}","S");	//125	}
		abKeyMap1.put("~","S");	abKeyMap2.put("~","S");	abKeyMap3.put("~","S");	//126	~
		abKeyMap1.put("","S");	abKeyMap2.put("","S");	abKeyMap3.put("","S");	//127	
		abKeyMap1.put("�","S");	abKeyMap2.put("�","S");	abKeyMap3.put("�","S");	//187	�
		abKeyMap1.put("�","S");	abKeyMap2.put("�","S");	abKeyMap3.put("�","S");	//191	�
		abKeyMap1.put("�","S");	abKeyMap2.put("�","S");	abKeyMap3.put("�","S");	//239	�
		
		

		//
		PartsOfSpeech.put(" abbrev.","abbreviated");	
		PartsOfSpeech.put(" interj.","interjection");	
		PartsOfSpeech.put(" compar.","comparative");
		PartsOfSpeech.put(" p. ple.","present participle");
		PartsOfSpeech.put(" superl.","superlative");
		PartsOfSpeech.put(" vb. n.","verbal noun");		
		PartsOfSpeech.put(" p. pr.","present participle");
		PartsOfSpeech.put(" indef.","indefinite");
		PartsOfSpeech.put(" equiv.","equivalent");		
		PartsOfSpeech.put(" contr.","contraction");		
		PartsOfSpeech.put(" p. p.","past participle");
		PartsOfSpeech.put(" v. i.","intransitive verb");
		PartsOfSpeech.put(" v. t.","transitive verb");
		PartsOfSpeech.put(" conj.","conjunction");
		PartsOfSpeech.put(" emph.","emphatic");		
		PartsOfSpeech.put(" prep.","preposition");
		PartsOfSpeech.put(" subj.","subjunctive");		
		PartsOfSpeech.put(" pron.","pronoun");	
		PartsOfSpeech.put(" p. a.","participial adjective");
		PartsOfSpeech.put(" sing.","singular");		
		PartsOfSpeech.put(" lit. ", "literally");
		PartsOfSpeech.put(" imp." , "imperfect");
		PartsOfSpeech.put(" var.","variety");	
		PartsOfSpeech.put(" ind.","indicative");		
		PartsOfSpeech.put(" acc.","accusative");		
		PartsOfSpeech.put(" inf.","infinitive");
		PartsOfSpeech.put(" obs.", "obsolete");
		PartsOfSpeech.put(" adj.","adjective");
		PartsOfSpeech.put(" adv.","adverb");
		PartsOfSpeech.put(" pl.","plural");		
		PartsOfSpeech.put(" v.","verb");
		PartsOfSpeech.put(" n.","noun");
		PartsOfSpeech.put(" a.","adjective");
		PartsOfSpeech.put(" p.","participle");
		
	}
}
