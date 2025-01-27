package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 5. Add Order Message – Msg Type 100
An Add Order message is published when a new visible order has been received and added to the book.
The Order ID is assigned by the matching engine and is unique for this symbol for today only. It is unique
across all markets, except that for NYSE Tape A symbols, it is only unique per matching engine instance.

 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 OrderID 6 Numeric 
 Price 7 Numeric 
 Volume 8 Numeric
 Side 9 Alpha
 FirmID 10 Alpha
 NumParitySplits 11 Numeric
 

100,128623,09:42:54.101795072,ENR PRA,22,14355262467128332,90.18,100,B,     ,0
100,128626,09:42:54.146734336,BMY,1929,14355262467087140,50.74,200,B,     ,0
100,128627,09:42:54.146876672,BMY,1930,14355262467087149,50.96,200,S,     ,0
100,128630,09:42:54.195164928,CHGG,518,14355262467080193,30.45,400,B,     ,0
100,128631,09:42:54.195310080,CHGG,519,14355262467080202,30.9,400,S,     ,0
100,128636,09:42:54.267036928,DSU,64,14355262467080104,10.61,100,B,     ,0
100,128637,09:42:54.267181056,DSU,65,14355262467080113,10.78,100,S,     ,0
 
 
 */
public class TaqMsgType100 implements TaqMsg {

	public TaqMsgType100() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType100(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("100")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric
				orderID = Long.parseLong(fields[5]); //6 Numeric 
				price = Double.parseDouble(fields[6]); //7 Numeric 
				volume = Integer.parseInt(fields[7]); //8 Numeric
				side = fields[8]; //9 Alpha
				firmID = fields[9]; //10 Alpha
				numParitySplits = Integer.parseInt(fields[10]); //11 Numeric
				
				success = true;
			}
			/*
		}
		catch(Exception e) {
			System.out.println("TaqMsgType100: Could not parse record. " + e.getMessage());
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
	public double getPrice() {
		return price;
	}
	public int getVolume() {
		return volume;
	}
	public String getSide() {
		return side;
	}
	public String getFirmID() {
		return firmID;
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
	double price = 0.0; //7 Numeric 
	int volume = 0; //8 Numeric
	String side = ""; //9 Alpha
	String firmID = ""; //10 Alpha
	int numParitySplits = 0; //11 Numeric
}
