package jch.lib.test;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import javafx.application.Application;
import jch.lib.analytics.apps.StockViewer;
import jch.lib.analytics.investment.nyse.xdp.TaqMsg;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgFactory;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType003;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType034;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType100;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType101;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType102;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType103;
import jch.lib.analytics.investment.stock.StatFrame;
import jch.lib.analytics.investment.stock.StatFrameSet;
import jch.lib.analytics.investment.stock.StockDay;

public class StockTest {

	public StockTest() {
		// TODO Auto-generated constructor stub
		Application.launch(StockViewer.class);
	}

	
	static void aalTest() {
		//String fileLoc = "D:\\Inv\\20191007\\";
		String fileLoc = "data/stock/20191007/";
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
		System.out.println(aal.getRecordCount());
		
		
		for(TaqMsgType100 msg : aal.AddOrderMsgs) {
			
			if(msg.getSide().equals("B")) {
				System.out.print(msg.getSourceTime().toString() + "\t");
				System.out.print(msg.getSequenceNumber() + "\t");
				System.out.print(msg.getSide() + "\t");
				System.out.print(msg.getPrice() + "\t");
				System.out.print(msg.getVolume() + "\t");
				
				for(TaqMsgType102 del : aal.DeleteOrderMsgs) {
					if (msg.getOrderID() == del.getOrderID()) {
						System.out.print("Deleted!!!\t");
						System.out.print(del.getSourceTime().toString());
					}
				}
				
				System.out.println();
			}
			//System.out.println(msg.getPrintableFlag());
		}
		
		/*
		for(TaqMsgType102 msg : aal.DeleteOrderMsgs) {
			
			System.out.print(msg.getSourceTime().toString() + "\t");
			System.out.print(msg.getOrderID() + "\t");
			System.out.print(msg.getNumParitySplits());
			System.out.println();
			//System.out.print(msg.getPrice() + "\t");
			//System.out.println(msg.getVolume());
	
			//System.out.println(msg.getPrintableFlag());
		}
		*/

		for(TaqMsgType103 msg : aal.OrderExecutionMsgs) {
			System.out.print(msg.getSourceTime().toString() + "\t");
			System.out.print(msg.getPrice() + "\t");
			System.out.println(msg.getVolume());
			
		
			//System.out.println(msg.getPrintableFlag());
		}
	}
	

	
	public static void aalFrameSplitTest() {

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
	
	
	
	
	static void nsyeMsgTest1() {
		TaqMsgType003 t1 = new TaqMsgType003("3,26,DECK,9,51,N,C,100,146.38,0,0,N,.0001,1");
		TaqMsgType034 t2 = new TaqMsgType034("34,18023,00:22:16.321198336,HLIO,1,P,~,,,,,,~,P");
		TaqMsgType100 t3 = new TaqMsgType100("100,128637,09:42:54.267181056,DSU,65,14355262467080113,10.78,100,S,     ,0");
		TaqMsgType101 t4 = new TaqMsgType101("101,34425,09:32:54.544840704,AUY,183,14355262466986717,3.4,100,0,0,0");
		
		System.out.println(t1.getSymbol());
		System.out.println(t2.getSymbol());
		System.out.println(t3.getOrderID());
		System.out.println(t4.getOrderID());
		
	}
	
	
	
	static void nysyDayStats() {
		String fileLoc = "D:\\Inv\\20191007\\";
		String fileName = "EQY_US_AMEX_IBF_1_20191007.txt";
		
		ArrayList<TaqMsg> nyseDay = TaqMsgFactory.loadFile(fileLoc + fileName);
		
		System.out.println(nyseDay.size());
		
		TreeMap<Integer,Integer> map = new TreeMap<Integer, Integer>();
		
		for(TaqMsg msg : nyseDay) {
			if(map.containsKey(msg.getMsgType()) == false) {
				map.put(msg.getMsgType(), 0);
			}
			
			map.put(msg.getMsgType(), map.get(msg.getMsgType()) + 1) ;
		}
		
		
		for(Map.Entry<Integer, Integer> entry : map.entrySet() ) {
			System.out.print(entry.getKey() + "\t");
			System.out.println(entry.getValue());
		}
		
	}
	
	
	
	static void nysyDayStats2() {

		//String fileLoc = "D:\\Inv\\20191007\\";		
		String fileLoc = "data/stock/20191007/";		

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
		
		
		System.out.println(nyseDay.size());
		
		TreeMap<String,Integer> map = new TreeMap<String, Integer>();
		
		for(TaqMsg msg : nyseDay) {
			if(map.containsKey(msg.getSymbol()) == false) {
				map.put(msg.getSymbol(), 0);
			}
			
			map.put(msg.getSymbol(), map.get(msg.getSymbol()) + 1) ;
		}
		
		
		for(Map.Entry<String, Integer> entry : map.entrySet() ) {
			System.out.print(entry.getKey() + "\t");
			System.out.println(entry.getValue());
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

}
