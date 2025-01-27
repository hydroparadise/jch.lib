package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 6. Modify Order Message – Msg Type 101
A Modify Order message is sent when the price or volume of an order is changed due to an event other
than a cancel-replace, or full or partial execution. The content of the price and volume fields represent the
new values after modification.

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
 
101,34423,09:32:54.535765248,AUY,181,14355262466986825,3.4,200,0,0,0
101,34425,09:32:54.544840704,AUY,183,14355262466986717,3.4,100,0,0,0
 
 
 */
public class TaqMsgType101 implements TaqMsg {

	public TaqMsgType101() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType101(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("101")) {
				
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
			System.out.println("TaqMsgType101: Could not parse record. "  + e.getMessage());
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
