package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 12. Retail Price Improvement Message – Msg Type 114
Published when RPI interest (hidden retail price improvement interest) is added or removed between the
best bid and best offer price. When all RPI interest for this security is removed from the book, An RPI
message with RPIIndicator = ‘ ‘ (space character) is published.
Note: This message type will not be published for NYSE Tape A symbols until they transition to trading on
Pillar in 2018.

 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Binary
 RPIIndicator 6 ASCII 

*/
public class TaqMsgType114 implements TaqMsg {

	public TaqMsgType114() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType114(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("114")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric				
				rpiIndicator = fields[5];
				
				success = true;
			}
			
		}
		catch(Exception e) {
			System.out.println("TaqMsgType114: Could not parse record." + e.getMessage());
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


	public String getRpiIndicator() {
		return rpiIndicator;
	}




	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	String rpiIndicator = "";  //6 ASCII
	
}
