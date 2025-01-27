package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 16. Add Order Refresh Message – Msg Type 106
The Add Order Refresh message can be sent in either of two contexts:
1) If a client sends a Refresh Request to the Request Controller, an Add Order Refresh message is sent
over the Refresh channels as part of the refresh response for every order currently sitting on the
book.
2) If a primary XDP Publisher fails over to the backup, for every symbol, the backup sends a Symbol
Clear message followed by a full refresh, which includes an Add Order Refresh message for every
order currently sitting on the book of the symbol

 Msg Type 1 Numeric 
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 
 Symbol 5 ASCII 
 SymbolSeqNum 6 Numeric
 OrderID 7 Numeric
 Price 8 Numeric
 Volume 9 Numeric
 Side 10 Alpha
 FirmID 11 ASCII 
 NumParitySplits 12 Numeric
 
 */
public class TaqMsgType106 implements TaqMsg {

	public TaqMsgType106() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType106(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("106")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				//fields[3];  //no field 4??
				symbol = fields[4]; //5 ASCII 
				symbolSeqNum = Integer.parseInt(fields[5]);//6 Numeric
				orderID = Long.parseLong(fields[6]); //7 Numeric
				price = Double.parseDouble(fields[7]);//8 Numeric
				volume = Integer.parseInt(fields[8]);//9 Numeric
				side = fields[9]; //10 Alpha
				firmID = fields[10];//11 ASCII 
				numParitySplits = Integer.parseInt(fields[11]); //12 Numeric
				

				success = true;
			}
			
		}
		catch(Exception e) {
			System.out.println("TaqMsgType106: Could not parse record. "  + e.getMessage());
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
	//4????
	String symbol = ""; //5 ASCII 
	int symbolSeqNum = 0;//6 Numeric
	long orderID = 0; //7 Numeric
	double price = 0.0;//8 Numeric
	int volume = 0;//9 Numeric
	String side = ""; //10 Alpha
	String firmID = "";//11 ASCII 
	int numParitySplits = 0; //12 Numeric
	
}
