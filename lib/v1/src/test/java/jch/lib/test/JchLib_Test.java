package jch.lib.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;

/*
	import java.io.BufferedReader;
	import java.io.BufferedWriter;
	import java.io.DataInputStream;
	import java.io.FileInputStream;
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
	import java.util.Collections;
	import java.util.List;
	import java.util.Map;
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
*/


public class JchLib_Test {
	

	static void testDatesParse() {
		String dateString = "10/1/2022 09:53:17";
		dateString = "10/1/22";
		dateString = "20221001";
		Date date = tryDateParse(dateString); // 2010-01-02
		
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		df = new SimpleDateFormat("yyyyMMdd");
		System.out.println(df.format(date));
	}
	
	
	static List<String> dateFormatStrings = 
			Arrays.asList("MM/dd/yy","MM-dd-yy","MM/dd/yyyy","MM-dd-yyyy", "yyyyMMdd","yyyy-MM-dd");
	static Date tryDateParse(String dateString) {
	    for (String formatString : dateFormatStrings) {
	        try {
	            return new SimpleDateFormat(formatString).parse(dateString);
	        }
	        catch (java.text.ParseException e) {}
	    }
	    return null;
	}

	
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
	



}
