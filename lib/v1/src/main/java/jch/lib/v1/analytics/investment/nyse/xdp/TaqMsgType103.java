package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 
 9. Order Execution Message – Msg Type 103
An Order Execution message is sent when an order is partially or fully executed. The Volume field indicates
the executed quantity. If the Price field is different from the price of the order, any remaining shares keep
their original price. If the Volume field equals the number of shares previously remaining in the order, then
the order has been fully executed and should be removed from the book. If the order has been partially
executed, further Order Execution messages for this Order ID may be published.
 
 Msg Type 1 Numeric
 SequenceNumber 2 Numeric
 SourceTime 3 HH:MM:SS.nnnnnnnnn
 Symbol 4 ASCII 
 SymbolSeqNum 5 Numeric
 OrderID 6 Numeric
 TradeID 7 Numeric 
 Price 8 Numeric
 Volume 9 Numeric
 PrintableFlag 10 Numeric
 NumParitySplits 11 Numeric
 DBExecID 12 Numeric 

103,6985,09:29:30.914807040,IQV,5,14355262466951579,64,148.76,100,1,0,0
103,1825573,15:59:27.683557120,AAL,18163,15481162375427546,103713,25.84,200,1,0,0
103,1832271,15:59:37.745518848,AAL,18232,15481162375431776,104043,25.84,52,1,0,0
103,1832356,15:59:37.868159232,AAL,18233,15481162375431776,104098,25.84,1,1,0,0
103,1833108,15:59:39.041669120,AAL,18242,15481162375431776,104153,25.84,62,1,0,0

1	103,
2	1833109,
3	15:59:39.041740800,
4	AAL,
5	18243,
6	15481162375431776,
7	104208,
8	25.84,
9	85,
10	1,
11	0,
12	0

 */
public class TaqMsgType103 implements TaqMsg {

	public TaqMsgType103() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType103(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			//if(fields[3].equals("AAL")) System.out.println(record);
			if(fields[0].equals("103")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //4 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //5 Numeric
				orderID = Long.parseLong(fields[5]); //6 Numeric 
				tradeID = Integer.parseInt(fields[6]); //7 Numeric 
				price = Double.parseDouble(fields[7]);
				volume = Integer.parseInt(fields[8]); //9 Numeric
				printableFlag = Integer.parseInt(fields[9]); //10 Numeric
				numParitySplits = Integer.parseInt(fields[10]); //11 Numeric
				dbExecID = Integer.parseInt(fields[11]); //12 Numeric
				
				
				success = true;
			}
			/*
		}
		catch(Exception e) {
			System.out.println("TaqMsgType103: Could not parse record." + e.getMessage());
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


	public int getTradeID() {
		return tradeID;
	}


	public double getPrice() {
		return price;
	}


	public int getVolume() {
		return volume;
	}


	public int getPrintableFlag() {
		return printableFlag;
	}


	public int getNumParitySplits() {
		return numParitySplits;
	}


	public int getDbExecID() {
		return dbExecID;
	}



	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	String symbol = ""; //4 ASCII 
	int symbolSeqNum = 0; //5 Numeric
	long orderID = 0; //6 Numeric 
	int tradeID = 0; //7 Numeric 
	double price = 0; //8 Numeric
	int volume = 0; //9 Numeric
	int printableFlag = 0; //10 Numeric
	int numParitySplits = 0; //11 Numeric
	int dbExecID = 0; //12 Numeric 
	
}
