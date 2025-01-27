package jch.lib.analytics.investment.stock;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;

/*
3 Symbol Index Mapping Message Y Y Y Y
34 Security Status Message Y Y Y Y
100 Add Order Message Y
101 Modify Order Message Y
104 Replace Order Message Y
102 Delete Order Message Y
103 Order Execution Message Y
110 Non-Displayed Trade Message Y
112 Trade Cancel Message Y
111 Cross Trade Message Y
113 Cross Correction Message Y
114 Retail Price Improvement Msg Y
105 Imbalance Message Y Y
106 Add Order Refresh Message Y
140 Quote Message Y
*/


/***
 * This class homes all TAQ messages for a given stock and day.
 * @author hydro
 *
 */
public class StockDay {
	
	public StockDay() {
		// TODO Auto-generated constructor stub
	}

	
	/***
	 * Constructor accepts a stock symbol and a full day's message list 
	 * and will extract all messages 
	 * @param stockSymbol
	 * @param msgList
	 */
	public StockDay(String stockSymbol, ArrayList<TaqMsg> msgList) {
		this.stockSymbol = stockSymbol;
		
		SymbolIndexMappingsMsgs = new ArrayList<TaqMsgType003>();
		SecurityStatusMsgs = new ArrayList<TaqMsgType034>();
		AddOrderMsgs = new ArrayList<TaqMsgType100>();
		ModifyOrderMsgs = new ArrayList<TaqMsgType101>();
		DeleteOrderMsgs = new ArrayList<TaqMsgType102>();
		OrderExecutionMsgs = new ArrayList<TaqMsgType103>();
		ReplaceOrderMsgs = new ArrayList<TaqMsgType104>();
		NonDisplayedTradeMsgs = new ArrayList<TaqMsgType110>();

		for(TaqMsg msg : msgList) {
			//System.out.println(msg.getSymbol());
			if(msg.getSymbol().equals(stockSymbol)) {
				
				switch(msg.getMsgType()) {
					case 3 : SymbolIndexMappingsMsgs.add((TaqMsgType003) msg); break;
					case 34 : SecurityStatusMsgs.add((TaqMsgType034) msg); break;
					case 100 : AddOrderMsgs.add((TaqMsgType100) msg); break;
					case 101 : ModifyOrderMsgs.add((TaqMsgType101) msg); break;
					case 102 : DeleteOrderMsgs.add((TaqMsgType102) msg); break;
					case 103 : OrderExecutionMsgs.add((TaqMsgType103) msg); break;
					case 104 : ReplaceOrderMsgs.add((TaqMsgType104) msg); break;
					case 110 : NonDisplayedTradeMsgs.add((TaqMsgType110) msg); break;
				}
			}
		}
	}
	
	
	/***
	 * 
	 * @return
	 */
	public StatFrame toBuyOrderStatFrame() {
		StatFrame output = new StatFrame();
		double prevPrice = 0;
		int prevVol = 0;
		//get previous price
		LocalTime startTime = LocalTime.parse("09:30:00");
		LocalTime stopTime = LocalTime.parse("16:00:00");
		for(TaqMsgType003 msg: this.SymbolIndexMappingsMsgs) {
			prevPrice = msg.getPrevClosePrice();
			prevVol = msg.getPrevCloseVolume();
		}
		
		StatEntry initEntry = new StatEntry(startTime, prevPrice, prevVol);
		output.addEntry(initEntry);
		for(TaqMsgType100 msg : this.AddOrderMsgs) {	
			if(msg.getSide().equals("B")) {
				StatEntry entry = new StatEntry(
						msg.getSourceTime(), 
						msg.getPrice(), 
						msg.getVolume());
				output.addEntry(entry);	
			}
		}
		output.addEntry(new StatEntry(stopTime, output.getLastEntry().getPrice(), 0));
		output.prepTimeDeltas();
		return output;
	}
	
	
	/***
	 * 
	 * @return
	 */
	public StatFrame toSellOrderStatFrame() {
		StatFrame output = new StatFrame();
		double prevPrice = 0;
		int prevVol = 0;
		//get previous price
		LocalTime startTime = LocalTime.parse("09:30:00");
		LocalTime stopTime = LocalTime.parse("16:00:00");
		for(TaqMsgType003 msg: this.SymbolIndexMappingsMsgs) {
			prevPrice = msg.getPrevClosePrice();
			prevVol = msg.getPrevCloseVolume();
		}
		
		StatEntry initEntry = new StatEntry(startTime, prevPrice, prevVol);
		output.addEntry(initEntry);
		for(TaqMsgType100 msg : this.AddOrderMsgs) {		
			if(msg.getSide().equals("S")) {
				StatEntry entry = new StatEntry(
						msg.getSourceTime(), 
						msg.getPrice(), 
						msg.getVolume());
				output.addEntry(entry);	
			}
		}
		output.addEntry(new StatEntry(stopTime, output.getLastEntry().getPrice(), 0));
		output.prepTimeDeltas();
		return output;
	}
	
	
	/***
	 * 
	 * @return
	 */
	public StatFrame toBuySellOrderStatFrame() {
		StatFrame output = new StatFrame();
		double prevPrice = 0;
		int prevVol = 0;
		//get previous price
		LocalTime startTime = LocalTime.parse("09:30:00");
		LocalTime stopTime = LocalTime.parse("16:00:00");
		for(TaqMsgType003 msg: this.SymbolIndexMappingsMsgs) {
			prevPrice = msg.getPrevClosePrice();
			prevVol = msg.getPrevCloseVolume();
		}
		
		StatEntry initEntry = new StatEntry(startTime, prevPrice, prevVol);
		output.addEntry(initEntry);
		for(TaqMsgType100 msg : this.AddOrderMsgs) {	

			StatEntry entry = new StatEntry(
						msg.getSourceTime(), 
						msg.getPrice(), 
						msg.getVolume());
			output.addEntry(entry);	
			
		}
		output.addEntry(new StatEntry(stopTime, output.getLastEntry().getPrice(), 0));
		output.prepTimeDeltas();
		return output;
	}
	
	
	/*
	public StatFrame toOrderExecutionStatFrame() {
		
	}
	*/
	
	public long getRecordCount() {
		return 
				SymbolIndexMappingsMsgs.size() +
				SecurityStatusMsgs.size() +
				AddOrderMsgs.size() +
				ModifyOrderMsgs.size() +
				DeleteOrderMsgs.size() +
				OrderExecutionMsgs.size() +
				ReplaceOrderMsgs.size() +
				NonDisplayedTradeMsgs.size();
		
	}
	
	public ArrayList<TaqMsgType003> SymbolIndexMappingsMsgs;
	public ArrayList<TaqMsgType034> SecurityStatusMsgs;
	public ArrayList<TaqMsgType100> AddOrderMsgs;
	public ArrayList<TaqMsgType101> ModifyOrderMsgs;
	public ArrayList<TaqMsgType102> DeleteOrderMsgs;
	public ArrayList<TaqMsgType103> OrderExecutionMsgs;
	public ArrayList<TaqMsgType104> ReplaceOrderMsgs;
	public ArrayList<TaqMsgType110> NonDisplayedTradeMsgs;
	

	String stockSymbol;
	LocalDate tradeDay;
}
