package jch.lib;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.Map.Entry;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javafx.application.Application;

import javax.xml.parsers.*;

import jch.lib.analytics.investment.nyse.xdp.*;
import jch.lib.analytics.investment.stock.StatEntry;
import jch.lib.analytics.investment.stock.StatFrame;
import jch.lib.analytics.investment.stock.StatFrameSet;
import jch.lib.analytics.investment.stock.StockDay;
import jch.lib.analytics.text.GhostWriter;
import jch.lib.analytics.text.GhostWriter.StatLenEntry;
import jch.lib.analytics.text.StringArrayStatEntryList;
import jch.lib.analytics.text.StringArrayStatEntryList.StringArrayStatEntry;
import jch.lib.analytics.text.StringStatEntryList;
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
import jch.lib.test.JchLib_Tests;

//Cannot be resolved since java 9
//import javax.xml.bind.DatatypeConverter;

import java.io.*;
import java.net.*;

import jch.lib.analytics.apps.*;

public class Main {


	public static void main(String[] args) {
		System.out.println("hello jch.lib");
		
		Application.launch(StockViewer.class, args);
		//aalFrameTest();
		aalFrameSplitTest();
	}
	
	static void aalFrameSplitTest() {

		String fileLoc = "D:\\Inv\\20191007\\";
		String fileName = "EQY_US_AMEX_IBF_1_20191007.txt";
		ArrayList<TaqMsg> nyseDay = TaqMsgFactory.loadFile(fileLoc + fileName);
		fileName = "EQY_US_AMEX_IBF_2_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_3_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_4_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_5_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_6_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		
		StockDay aal = new StockDay("AAL", nyseDay);
		StatFrame aalStats;
		
		aalStats = aal.toBuyOrderStatFrame();	
		System.out.println("Buy Order Stats");
		System.out.println("Start:   " + aalStats.getFirstEntry().getTime());
		System.out.println("Stop:    " + aalStats.getLastEntry().getTime());
		System.out.println("Entries: " + aalStats.calcEntryCount());
		System.out.println("Avg:     " + aalStats.calcAvgPrice());
		System.out.println("Wtd Avg: " + aalStats.calcWtdAvgVolDurPrice());
		System.out.println("Max:     " + aalStats.calcMaxPrice());
		System.out.println("Min:     " + aalStats.calcMinPrice());
		System.out.println("First:   " + aalStats.getFirstEntry().getPrice());
		System.out.println("Last:    " + aalStats.getLastEntry().getPrice());
		System.out.println("Std Dev: " + aalStats.calcStdDevPrice());
		System.out.println();
		
		aalStats.orderByTime();
		StatFrameSet sfs = aalStats.divideByTime(100);
		
		StatFrame sf = sfs.getFirstFrame();
		while(sf != null) {
			
			System.out.println("Start:   " + sf.getFirstEntry().getTime());
			System.out.println("Stop:    " + sf.getLastEntry().getTime());
			System.out.println("Entries: " + sf.calcEntryCount());
			System.out.println("Avg:     " + sf.calcAvgPrice());
			System.out.println("Wtd Avg: " + sf.calcWtdAvgVolDurPrice());
			System.out.println("Max:     " + sf.calcMaxPrice());
			System.out.println("Min:     " + sf.calcMinPrice());
			System.out.println("First:   " + sf.getFirstEntry().getPrice());
			System.out.println("Last:    " + sf.getLastEntry().getPrice());
			System.out.println("Std Dev: " + sf.calcStdDevPrice());
			System.out.println();

			sf = sf.getNextFrame();
		}

	}
/*			
			StatFrameSet aalStatSet = new StatFrameSet();
			StatFrame newFrame = new StatFrame();;
			
			double td = (x2 - x1) / d;
			int i = 1;
						
			StatEntry curEntry = aalStats.getFirstEntry();
			while(curEntry != null) {
				
				if(curEntry.calcTimeSecond() >= x1 + (td * (i - 1)) &&
				   curEntry.calcTimeSecond() <  x1 + (td * i)) {
					newFrame.addEntry(curEntry);
					curEntry = curEntry.getNextEntry();
				}
				else {
					aalStatSet.addEntry(newFrame);
					newFrame = new StatFrame();
					i++;
				}
			}
			
			StatFrame curFrame = aalStatSet.getFirstFrame();
			while(curFrame != null) {
				System.out.print(d + "\t");
				System.out.println(curFrame.calcEntryCount() + "\t");
				System.out.println("Start:   " + curFrame.getFirstEntry().getTime());
				System.out.println("Stop:    " + curFrame.getLastEntry().getTime());
				System.out.println("Entries: " + curFrame.calcEntryCount());
				System.out.println("Avg:     " + curFrame.calcAvgPrice());
				System.out.println("Wtd Avg: " + curFrame.calcWtdAvgVolDurPrice());
				System.out.println("Max:     " + curFrame.calcMaxPrice());
				System.out.println("Min:     " + curFrame.calcMinPrice());
				System.out.println("First:   " + curFrame.getFirstEntry().getPrice());
				System.out.println("Last:    " + curFrame.getLastEntry().getPrice());
				System.out.println("Std Dev: " + curFrame.calcStdDevPrice());
				System.out.println();

				curFrame = curFrame.getNextFrame();
			}
		
			*/
		
		
	static void aalFrameTest() {
		String fileLoc = "D:\\Inv\\20191007\\";
		String fileName = "EQY_US_AMEX_IBF_1_20191007.txt";
		ArrayList<TaqMsg> nyseDay = TaqMsgFactory.loadFile(fileLoc + fileName);
		fileName = "EQY_US_AMEX_IBF_2_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_3_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_4_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_5_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_6_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		
		StockDay aal = new StockDay("AAL", nyseDay);
		StatFrame aalStats;
		
		System.out.println("Buy Order Stats");
		aalStats = aal.toBuyOrderStatFrame();		
		System.out.println("Start:   " + aalStats.getFirstEntry().getTime());
		System.out.println("Stop:    " + aalStats.getLastEntry().getTime());
		System.out.println("Entries: " + aalStats.calcEntryCount());
		System.out.println("Avg:     " + aalStats.calcAvgPrice());
		System.out.println("Wtd Avg: " + aalStats.calcWtdAvgVolDurPrice());
		System.out.println("Max:     " + aalStats.calcMaxPrice());
		System.out.println("Min:     " + aalStats.calcMinPrice());
		System.out.println("First:   " + aalStats.getFirstEntry().getPrice());
		System.out.println("Last:    " + aalStats.getLastEntry().getPrice());
		System.out.println("Std Dev: " + aalStats.calcStdDevPrice());
		System.out.println();

		System.out.println("Sell Order Stats");
		aalStats = aal.toSellOrderStatFrame();		
		System.out.println("Start:   " + aalStats.getFirstEntry().getTime());
		System.out.println("Stop:    " + aalStats.getLastEntry().getTime());
		System.out.println("Entries: " + aalStats.calcEntryCount());
		System.out.println("Avg:     " + aalStats.calcAvgPrice());
		System.out.println("Wtd Avg: " + aalStats.calcWtdAvgVolDurPrice());
		System.out.println("Max:     " + aalStats.calcMaxPrice());
		System.out.println("Min:     " + aalStats.calcMinPrice());
		System.out.println("First:   " + aalStats.getFirstEntry().getPrice());
		System.out.println("Last:    " + aalStats.getLastEntry().getPrice());
		System.out.println("Std Dev: " + aalStats.calcStdDevPrice());
		System.out.println();

		
	
	}
}

