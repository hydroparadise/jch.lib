package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 14. Cross Correction Message – Msg Type 113
In the event that an earlier Cross Trade has been reported in error, a Cross Correction message is sent.
Note that since Cross Correction messages only affect cross auctions that occurred in the past, customers
who are only interested in building a book may safely ignore them.
Customers who are building a complete record of today’s volume should remove the previously reported
volume from their statistics and add the volume of the Cross Correction to them.

 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 CrossID 6 Numeric
 Volume 7 Numeric 
 

*/
public class TaqMsgType113 implements TaqMsg {

	public TaqMsgType113() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType113(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("113")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric				
				crossID = Integer.parseInt(fields[5]);
				volume = Integer.parseInt(fields[6]);
				
				success = true;
			}
			
		}
		catch(Exception e) {
			System.out.println("TaqMsgType113: Could not parse record." + e.getMessage());
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


	public int getCrossID() {
		return crossID;
	}


	public int getVolume() {
		return volume;
	}



	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	int crossID = 0;//6 Numeric
	int volume = 0;//7 Numeric 
}
