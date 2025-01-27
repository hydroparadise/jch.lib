package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 10. Non-Displayed Trade Message – Msg Type 110
A Non Displayed Trade message is sent as a result of a match between two non-displayed orders.
Customers who are only interested in building a book of displayed orders may safely ignore Non-Displayed
Trade messages. Customers who are creating statistics or displays requiring the full record of trades in this
market will need to process Non-Displayed Trade messages.
 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 TradeID 6 Numeric
 Price 7 Numeric
 Volume 8 Numeric
 PrintableFlag 9 Numeric
 DBExecID 10 Numeric

110,12323,09:30:02.744376832,BSX,30,774,40.365,100,1,0

*/
public class TaqMsgType110 implements TaqMsg {

	public TaqMsgType110() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType110(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("110")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric
				
				tradeID = Integer.parseInt(fields[5]); //6 Numeric
				price = Double.parseDouble(fields[6]); //7 Numeric
				volume = Integer.parseInt(fields[7]); //8 Numeric
				printableFlag = Integer.parseInt(fields[8]); //9 Numeric
				dbExecID = Integer.parseInt(fields[9]); //10 Numeric
				
				success = true;
			}
			/*
		}
		catch(Exception e) {
			System.out.println("TaqMsgType110: Could not parse record." + e.getMessage());
		}
			*/
		return success;
	}
	

	public int getMsgType() {
		return msgType;
	}


	public int getSequenceNumber() {
		return sequenceNumber;
	}


	public LocalTime getSourceTime() {
		return sourceTime;
	}


	public String getSymbol() {
		return symbol;
	}


	public int getSymbolSeqNum() {
		return symbolSeqNum;
	}


	public int getTradeID() {
		return tradeID;
	}


	public double getPrice() {
		return price;
	}


	public int getVolume() {
		return volume;
	}


	public int getPrintableFlag() {
		return printableFlag;
	}


	public int getDbExecID() {
		return dbExecID;
	}


	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	int tradeID = 0; //6 Numeric
	double price = 0.0; //7 Numeric
	int volume = 0; //8 Numeric
	int printableFlag = 0; //9 Numeric
	int dbExecID = 0; //10 Numeric
	
}
