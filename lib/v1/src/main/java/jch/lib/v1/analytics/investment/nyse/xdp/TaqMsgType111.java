package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 13. Cross Trade Message – Msg Type 111
A Cross Trade message is published on completion of a crossing auction, and shows the bulk volume that
traded in the auction. The Reason Code field indicates the auction type. Additionally, a non-printable
Order Execution or Trade message will be published for each order that traded
 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 CrossID 6 Numeric
 Price 7 Numeric
 Volume 8 Numeric
 CrossType 9 ASCII

*/
public class TaqMsgType111 implements TaqMsg {

	public TaqMsgType111() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType111(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("111")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric

				crossID = Integer.parseInt(fields[5]); //6 CrossID
				price = Double.parseDouble(fields[6]); //7 Numeric
				volume = Integer.parseInt(fields[7]); //8 Numeric
				crossType = fields[8]; //9 ASCII
				
				success = true;
			}
			
		}
		catch(Exception e) {
			System.out.println("TaqMsgType111: Could not parse record." + e.getMessage());
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


	public double getPrice() {
		return price;
	}


	public int getVolume() {
		return volume;
	}


	public String getCrossType() {
		return crossType;
	}


	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	int crossID = 0; //6 CrossID
	double price = 0.0; //7 Numeric
	int volume = 0; //8 Numeric
	String crossType = ""; //9 ASCII
	
}
