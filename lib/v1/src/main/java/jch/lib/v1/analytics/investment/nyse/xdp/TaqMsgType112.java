package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 11. Trade Cancel Message – Msg Type 112 and 221
 In the event that an earlier trade has been reported in error, a Trade Cancel message is sent. This occurs
whether the initial report was an Order Execution or a Non-Displayed Trade message.
Note that since Trade Cancel messages only affect trades that occurred in the past, customers who are only
interested in building a book may safely ignore them.
Customers who are building a complete record of today’s trades should remove the cancelled trade from
their records and subtract its volume from any statistics.
 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 TradeID 6 Numeric
 
 

*/
public class TaqMsgType112 implements TaqMsg {

	public TaqMsgType112() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType112(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("112")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric				
				tradeID = Integer.parseInt(fields[5]); //6 Numeric

				success = true;
			}
			
		}
		catch(Exception e) {
			System.out.println("TaqMsgType112: Could not parse record." + e.getMessage());
		}

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


	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	int tradeID = 0; //6 Numeric
	
}
