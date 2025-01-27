package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 7. Replace Order Message – Msg Type 104
A Replace Order message is published when a cancel/replace order is received and executed. The sitting
order is replaced with a new one containing the same symbol, side and attribution, a new Order ID, and the
price and size specified. The sitting order must be removed from the book and replaced with the new
order.
 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 OrderID 6 Numeric
 NewOrderID 7 Numeric 
 Price 8 Numeric 
 Volume 9 Numeric
 PrevPriceParitySplits 10 Numeric 
 NewPriceParitySplits 11 Numeric

104,34294,09:32:53.418957312,CGC,35,14355262466978610,14355262466978700,22.46,100,0,0
 
 
 */
public class TaqMsgType104 implements TaqMsg {

	public TaqMsgType104() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType104(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("104")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric
				orderID = Long.parseLong(fields[5]); //6 Numeric 
				newOrderID = Long.parseLong(fields[6]);
				price = Double.parseDouble(fields[7]); //7 Numeric 
				volume = Integer.parseInt(fields[8]); //8 Numeric
				prevPriceParitySplits = Integer.parseInt(fields[9]); //10
				newPriceParitySplits = Integer.parseInt(fields[10]); //11

				success = true;
			}
			/*
		}
		catch(Exception e) {
			System.out.println("TaqMsgType104: Could not parse record. "  + e.getMessage());
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


	public long getNewOrderID() {
		return newOrderID;
	}


	public double getPrice() {
		return price;
	}


	public int getVolume() {
		return volume;
	}


	public int getPrevPriceParitySplits() {
		return prevPriceParitySplits;
	}


	public int getNewPriceParitySplits() {
		return newPriceParitySplits;
	}


	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	long orderID = 0; //6 Numeric 
	long newOrderID = 0; //7 Numeric 
	double price = 0.0; //8
	int volume = 0; //9 Numeric
	int prevPriceParitySplits = 0; //10
	int newPriceParitySplits = 0; //11
	
}
