package jch.lib.analytics.text.book.bible;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Map;
//import java.util.Map.Entry;
import java.util.TreeMap;
//import java.util.Comparator;


//import jch.lib.analytics.*;
//import jch.lib.analytics.apps.*;
import jch.lib.list.ChunkLink;
import jch.lib.list.ChunkList;
import jch.lib.list.StringChunkLink;

/*
 * Testament
 *  Book
 *   Chapter
 *    Verse
 * 
 * 
 */

/*
The Old Testament of the King James Version of the Bible




The First Book of Moses:  Called Genesis


1:1 In the beginning God created the heaven and the earth.

1:2 And the earth was without form, and void; and darkness was upon
the face of the deep. And the Spirit of God moved upon the face of the
waters.

1:3 And God said, Let there be light: and there was light.

1:4 And God saw the light, that it was good: and God divided the light
from the darkness.
------------------------------------------------------------------------------------
50:24 And Joseph said unto his brethren, I die: and God will surely
visit you, and bring you out of this land unto the land which he sware
to Abraham, to Isaac, and to Jacob.

50:25 And Joseph took an oath of the children of Israel, saying, God
will surely visit you, and ye shall carry up my bones from hence.

50:26 So Joseph died, being an hundred and ten years old: and they
embalmed him, and he was put in a coffin in Egypt.




The Second Book of Moses:  Called Exodus


1:1 Now these are the names of the children of Israel, which came
into Egypt; every man and his household came with Jacob.

1:2 Reuben, Simeon, Levi, and Judah, 1:3 Issachar, Zebulun, and
Benjamin, 1:4 Dan, and Naphtali, Gad, and Asher.


 */


public class KingJamesVersionBible {
	
	/**
	 * Let's greet everybody
	 */
	public static final String seedLocation = "lang/eng/seed/published/project_gutenburg/txt/";
	public static final String seedFile = "pg10.txt";
	
	public static final String grammarLocation = "lang/eng/seed/published/project_gutenburg/dataset/KJV/";
	public static final String grammarFile = "kjv_full_syntax.txt";
	
	public static ChunkList clKJV;
	public static ArrayList<SyntaxDatasetEntry> alKJVGrammar;
	
	//Character Abstraction Key Maps - Helps to identify patterns
	public static TreeMap<String, String> abKeyMap1;
	public static TreeMap<String, String> abKeyMap2;
	public static TreeMap<String, String> abKeyMap3;
	
	static {
		loadKJV();
		initKeyMaps();
		alKJVGrammar = buildGrammar();
	}
	
	public static void sayHi() {
		System.out.println("Welcome to the KJV bible!");	
		System.out.println(clKJV.getChunkCount());
	}
	

	
	static void loadKJV() {
		clKJV = ChunkList.loadFile(seedLocation + seedFile);
		clKJV.removeBeforeKey("*** START OF THIS PROJECT GUTENBERG EBOOK THE KING JAMES BIBLE ***", true);
		clKJV.removeAfterKey("End of the Project Gutenberg EBook of The King James Bible", false);
		/*
		clKJV.replaceAll(":", "");
		clKJV.replaceAll("0", "");
		clKJV.replaceAll("1", "");
		clKJV.replaceAll("2", "");
		clKJV.replaceAll("3", "");
		clKJV.replaceAll("4", "");
		clKJV.replaceAll("5", "");
		clKJV.replaceAll("6", "");
		clKJV.replaceAll("7", "");
		clKJV.replaceAll("8", "");
		clKJV.replaceAll("9", "");
		//clKJV.replaceAll("\r", "");
		//clKJV.replaceAll("\n", "");
		clKJV.replaceAll("\r\n", " ");
		clKJV.replaceAll("  ", " ");
		*/
		clKJV.reindex();
	}
	
	
	public static ArrayList<SyntaxDatasetEntry> buildGrammar() {
		ArrayList<SyntaxDatasetEntry> output = new ArrayList<SyntaxDatasetEntry>();
		try(BufferedReader br = new BufferedReader(new FileReader(grammarLocation + grammarFile))) {
			String recordString;
			//read header
			recordString = br.readLine();
			//System.out.println(record);
			while((recordString = br.readLine()) != null) {
				BibleSyntaxDatasetEntry entry = new BibleSyntaxDatasetEntry();
				entry.setStringRecord(recordString);
				output.add(entry);
	
				//System.out.println(entry.getText() + "\t" + entry.getPartOfSpeechTag());		
			}
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return output;
	}
	
	@SuppressWarnings("unused")
	public static Bible buildBible() {
		Bible output = new Bible();
		
		ChunkLink t1;
		ChunkLink t2;
		Testament testament = null;
		BibleBook book = null;
		
		KingJamesVersionBible.clKJV.moveFirstChunk();
		
		//Bookmark testaments and books
		//2 testaments, 66 books
		for(int i = 0; i < 70 ; i++) {
			//System.out.println(i + " - ");
			//Get what's between 5 newlines and 3 newlines
			//Returns either Testaments or Books
			KingJamesVersionBible.clKJV.findFromCurrentPosition("\r\n\r\n\r\n\r\n\r\n",true);
			t1 = KingJamesVersionBible.clKJV.getCurrentChunk();
			
			KingJamesVersionBible.clKJV.findFromCurrentPosition("\r\n\r\n\r\n",false);
			t2 = KingJamesVersionBible.clKJV.getCurrentChunk();
			
			ChunkList L1 = ChunkList.stringToChunks(((StringChunkLink)(t1)).toString(t2.getIndex() - t1.getIndex()));
						
			//Cleans string a bit before printing
			if(L1 != null ) {
				//System.out.println(L1.toString(L1.getChunkCount()));
				//ensures the book after Testament proclamation doesn't clobber next book
				if(L1.findFromCurrentPosition("Testament") == false) {
					if(book != null &&
						testament.Books.isEmpty() == false) {
						book.setEndContentRef(t1);
					}
					
					//System.out.println("is testament null?");
					//if(testament == null) System.out.println("yes!");
					//else System.out.println("no!");
					
					book = new BibleBook();
					book.setRefLink(t1, t2);
					testament.Books.add(book);
					
					book.setStartContentRef(t2);
					
					KingJamesVersionBible.clKJV.findFromCurrentPosition("\r\n\r\n\r\n",true);
				}
				else {
					if (output.Testaments.isEmpty() == false) {
						if (book != null) {
							book.setEndContentRef(t1);
							//testament.Books.add(book);
						}
						testament.setEndContentRef(t1);
					}
					
					testament = new Testament();
					testament.setRefLink(t1, t2);
					output.Testaments.add(testament);
					testament.setStartContentRef(t2);
				}
				
			}
			//cleanup
			L1 = null;
			
		}
		
		if(book != null) {
			book.setEndContentRef(KingJamesVersionBible.clKJV.getLastChunk());
		}
		
		if(testament != null) {
			testament.setEndContentRef(KingJamesVersionBible.clKJV.getLastChunk());
		}
		
		
		
		BibleChapter chapter = null;
		BibleVerse verse = null;
		
		StringChunkLink t3 = null;
		StringChunkLink t4 = null;
		ChunkLink prevChunk = null;
		//Fill in chapters and verses
		for(int i = 0; i < output.Testaments.size() ; i++) {

			for(int k = 0; k < output.Testaments.get(i).Books.size(); k++) {
				int chapterNumber = 0;
				int verseNumber = 0;
				int prevVerse = 0;
				long start = output.Testaments.get(i).Books.get(k).getStartContentRef().getIndex();
				long stop = output.Testaments.get(i).Books.get(k).getEndContentRef().getIndex();
				
				
				KingJamesVersionBible.clKJV.setPosition(start);
				
				//NN:NN
				while(KingJamesVersionBible.clKJV.findFromCurrentPosition(":", true) &&
						KingJamesVersionBible.clKJV.getCurrentChunk().getIndex() < stop) {
					
					t3 = (StringChunkLink)KingJamesVersionBible.clKJV.getCurrentChunk();
					
					int n = 0;
					while(abKeyMap1.get(KingJamesVersionBible.clKJV.toString(1)) == "N") {
						KingJamesVersionBible.clKJV.moveNextChunk();
						n++;
					}

					if(n > 0) {
						//turn string into int
						//System.out.println(t3.toString(n));
						verseNumber = Integer.parseInt(t3.toString(n));
						
						//if verse number is 1, then create new chapter
						if(verseNumber == 1) {
							chapterNumber++;
							chapter = new BibleChapter();
							chapter.setChapterNumber(chapterNumber);
							output.Testaments.get(i).Books.get(k).Chapters.add(chapter);
						}
						
						//find the cleave, and set end extent of previous verse
						if(prevChunk != null) {
							t4 = (StringChunkLink) KingJamesVersionBible.clKJV.getCurrentChunk().getPreviousChunk();
							while(abKeyMap1.get(t4.toString(1)) != "W") {
								t4 = (StringChunkLink) t4.getPreviousChunk();
							}
							
							//System.out.println("this is t4: ~" + t4.toString(1) + "~");
							
							verse.setRefLinkExtent(t4);
						}
						
						//
						verse = new BibleVerse(verseNumber);
						verse.setRefLink(KingJamesVersionBible.clKJV.getCurrentChunk().getNextChunk());
						chapter.Verses.add(verse);
						prevChunk = KingJamesVersionBible.clKJV.getCurrentChunk();
					}
				}
				if(verse != null) {
					verse.setRefLinkExtent(output.Testaments.get(i).getEndContentRef());
				}
			}
		}

		return output;
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
		
	}
	
}
/*
	Testaments

<start>	


The Old Testament of the King James Version of the Bible


<end>

<start>	
***




The New Testament of the King James Bible



<end>

	Books
	
	
The First Book of Moses:  Called Genesis
The Second Book of Moses:  Called Exodus
The Third Book of Moses:  Called Leviticus
The Fourth Book of Moses:  Called Numbers
The Fifth Book of Moses:  Called Deuteronomy
The Book of Joshua
The Book of Judges
The Book of Ruth
The First Book of Samuel: The First Book of the Kings
The Second Book of Samuel: The Second Book of the Kings
The First Book of the Kings: The Third Book of the Kings
The Second Book of the Kings: The Fourth Book of the Kings
The First Book of the Chronicles
The Second Book of the Chronicles
Ezra
The Book of Nehemiah
The Book of Esther
The Book of Job
The Book of Psalms
The Proverbs
Ecclesiastes
The Song of Solomon
The Book of the Prophet Isaiah
The Book of the Prophet Jeremiah
The Lamentations of Jeremiah
The Book of the Prophet Ezekiel
The Book of Daniel
Hosea
Joel
Amos
Obadiah
Jonah
Micah
Nahum
Habakkuk
Zephaniah
Haggai
Zechariah
Malachi


The Gospel According to Saint Matthew
The Gospel According to Saint Mark
The Gospel According to Saint Luke
The Gospel According to Saint John
The Acts of the Apostles
The Epistle of Paul the Apostle to the Romans
The First Epistle of Paul the Apostle to the Corinthians
The Second Epistle of Paul the Apostle to the Corinthians
The Epistle of Paul the Apostle to the Galatians
The Epistle of Paul the Apostle to the Ephesians
The Epistle of Paul the Apostle to the Philippians
The Epistle of Paul the Apostle to the Colossians
The First Epistle of Paul the Apostle to the Thessalonians
The Second Epistle of Paul the Apostle to the Thessalonians
The First Epistle of Paul the Apostle to Timothy
The Second Epistle of Paul the Apostle to Timothy
The Epistle of Paul the Apostle to Titus
The Epistle of Paul the Apostle to Philemon
The Epistle of Paul the Apostle to the Hebrews
The General Epistle of James
The First Epistle General of Peter
The Second General Epistle of Peter
The First Epistle General of John
The Second Epistle General of John
The Third Epistle General of John
The General Epistle of Jude
The Revelation of Saint John the Devine

 */


