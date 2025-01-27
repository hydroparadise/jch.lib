package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
17. Quote Message – Msg Type 140
A quote message is sent when any event results in a new top of book value on either side of the market.


 Msg Type 1 Numeric 
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 //4 gone again?
 Symbol 5 ASCII 
 SymbolSeqNum 6 Numeric
 Ask Price 7 Numeric
 Ask Volume 8 Numeric 
 Bid Price 9 Numeric 
 Bid Volume 10 Numeric
 Quote Condition 11 ASCII
 RPI Indicator 12 ASCII
 
 */

public class TaqMsgType140 implements TaqMsg {

	public TaqMsgType140() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType140(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("140")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				//fields[3];  //no field 4??
				symbol = fields[4]; //5 ASCII 
				symbolSeqNum = Integer.parseInt(fields[5]);//6 Numeric

				askPrice = Double.parseDouble(fields[6]); //7 Numeric
				askVolume = Double.parseDouble(fields[7]); //8 Numeric 
				bidPrice = Double.parseDouble(fields[8]); // 9 Numeric 
				bidVolume = Double.parseDouble(fields[9]); //10 Numeric
				quoteCondition = fields[10];//11 ASCII
				rpiIndicator = fields[11]; //12 ASCII

				success = true;
			}
			
		}
		catch(Exception e) {
			System.out.println("TaqMsgType140: Could not parse record. "  + e.getMessage());
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


	public double getAskPrice() {
		return askPrice;
	}


	public double getAskVolume() {
		return askVolume;
	}


	public double getBidPrice() {
		return bidPrice;
	}


	public double getBidVolume() {
		return bidVolume;
	}


	public String getQuoteCondition() {
		return quoteCondition;
	}


	public String getRpiIndicator() {
		return rpiIndicator;
	}





	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	//4????
	String symbol = ""; //5 ASCII 
	int symbolSeqNum = 0;//6 Numeric

	double askPrice = 0.0; //7 Numeric
	double askVolume = 0.0; //8 Numeric 
	double bidPrice = 0.0; // 9 Numeric 
	double bidVolume = 0.0; //10 Numeric
	String quoteCondition = "";//11 ASCII
	String rpiIndicator = ""; //12 ASCII
	 
	
}
