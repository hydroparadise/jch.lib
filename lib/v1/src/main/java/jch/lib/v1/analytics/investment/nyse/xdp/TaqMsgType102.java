package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 8. Delete Order Message – Msg Type 102
A Delete Order message is published when an order is taken off of the book for any reason except for full
execution, in which case an Order Execution message is sent.
Immediately before a trading session changes (eg: Early session to Core session), all orders that were
submitted for the current or current+previous sessions are explicitly deleted with a Delete Order message
 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 OrderID 6 Numeric
 NumParitySplits 7 Numeric


102,34409,09:32:54.423015680,AEO,28,14355262466977572,0
102,34410,09:32:54.423982336,HSY,120,14355262466975834,0
102,34411,09:32:54.423986176,HSY,121,14355262466975843,0
102,34412,09:32:54.425104896,HI,16,14355262466974799,0
102,34413,09:32:54.448417024,GNL,62,14355262466978568,0
102,34414,09:32:54.448528384,GNL,63,14355262466978577,0
102,34416,09:32:54.476313600,FRO,140,14355262466978586,0
102,34417,09:32:54.476315392,FRO,141,14355262466978595,0

 */
public class TaqMsgType102 implements TaqMsg {

	public TaqMsgType102() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType102(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("102")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric
				orderID = Long.parseLong(fields[5]); //6 Numeric 
				numParitySplits = Integer.parseInt(fields[6]);
				
				success = true;
			}
			/*
		}
		catch(Exception e) {
			System.out.println("TaqMsgType102: Could not parse record." + e.getMessage());
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


	public long getOrderID() {
		return orderID;
	}


	public int getNumParitySplits() {
		return numParitySplits;
	}



	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	long orderID = 0; //6 Numeric 
	int numParitySplits = 0;//7 Numeric
	
}
