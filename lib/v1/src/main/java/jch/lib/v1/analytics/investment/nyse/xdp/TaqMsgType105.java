package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 * 
15. Imbalance Message – Msg Type 105
Imbalance messages are sent periodically to update price and volume information during auctions. If there
is no change to the calculated fields, no message will be generated. See Appendix A: Information on
Auctions for details on the auction process in the NYSE, Arca and American markets.
NOTE: The last 5 new fields, as well as (for Arca and American) the 2 clearing price fields, will initially be
set to 0. They will be fully populated in future releases for each market.


 Msg Type 1 Numeric 						105, 
 SequenceNumber 2 Numeric					4803,
 SourceTime 3 HH:MM:SS.nnnnnnnnn			06:30:00.007242496,
 Symbol 5 ASCII 							IBO,
 SymbolSeqNum 6 Numeric						2,
 ReferencePrice 7 Numeric					25,
 PairedQty 8 Numeric						0,
 TotalImbalanceQty 9 Numeric 				0,
 MarketImbalanceQty 10 Numeric				0,
 AuctionTime 11 Numeric 					0700,
 AuctionType 12 Alpha						O,
 ImbalanceSide 13 Alpha						 ,
 ContinuousBookClearingPrice 14 Numeric 	0,
 AuctionInterestClearingPrice 15 Numeric	0,
 SSRFilingPrice 16 Numeric					0,
 Indicative MatchPrice 17 Numeric			0,
 UpperCollar 18 Numeric						0,
 LowerCollar 19 Numeric 					0,
 AuctionStatus 20 Numeric					0,
 FreezeStatus 21 Numeric					0,
 NumExtensions 22 Numeric					0,
 UnPaired Quantity 23 Numeric 				0,
 Unpaired Side 24 Alpha						 ,
 Significant Imbalance 25 Alpha				
 
1 	105,
2 	4803,
3 	06:30:00.007242496,
4 	IBO,
5 	2,
6 	25,
7 	0,
8 	0,
9 	0,
10	0700,
11 	O,
12 	 ,
13 	0,
14 	0,
15 	0,
16 	0,
17 	0,
18 	0,
19 	0,
20 	0,
21 	0,
22 	0, 
23 	,
24

 */
public class TaqMsgType105 implements TaqMsg {

	public TaqMsgType105() {
		// TODO Auto-generated constructor stub
	}
	
	
	public TaqMsgType105(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("105")) {

				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3]; //5 ASCII 
				symbolSeqNum = Integer.parseInt(fields[4]); //6 Numeric

				

				referencePrice = Double.parseDouble(fields[5]); //7 Numeric
				pairedQty = Integer.parseInt(fields[6]);; //8 Numeric
				totalImbalanceQty = Integer.parseInt(fields[7]);; //9 Numeric 
				marketImbalanceQty = Integer.parseInt(fields[8]);; //10 Numeric
				auctionTime = Double.parseDouble(fields[9]); //11 Numeric 
				auctionType = fields[10]; //12 Alpha
				imbalanceSide = fields[11]; //13 Alpha
				continuousBookClearingPrice = Double.parseDouble(fields[12]); //14 Numeric 
				auctionInterestClearingPrice = Double.parseDouble(fields[13]); //15 Numeric
				ssrFilingPrice = Double.parseDouble(fields[14]); //16 Numeric
				indicativeMatchPrice = Double.parseDouble(fields[15]); //17 Numeric
				upperCollar = Double.parseDouble(fields[16]); //18 Numeric
				lowerCollar = Double.parseDouble(fields[17]); //19 Numeric 
				auctionStatus = Integer.parseInt(fields[18]); //20 Numeric
				freezeStatus = Integer.parseInt(fields[19]); //21 Numeric
				numExtensions = Integer.parseInt(fields[20]); //22 Numeric
				unPairedQuantity = Integer.parseInt(fields[21]); //23 Numeric 
				unpairedSide = fields[22]; //24 Alpha
				significantImbalance = fields[23]; //25 Alpha
				success = true;
			}
			/*
		}
		catch(Exception e) {
			System.out.println("TaqMsgType105: Could not parse record. "  + e.getMessage());
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


	public double getReferencePrice() {
		return referencePrice;
	}


	public int getPairedQty() {
		return pairedQty;
	}


	public int getTotalImbalanceQty() {
		return totalImbalanceQty;
	}


	public int getMarketImbalanceQty() {
		return marketImbalanceQty;
	}


	public double getAuctionTime() {
		return auctionTime;
	}


	public String getAuctionType() {
		return auctionType;
	}


	public String getImbalanceSide() {
		return imbalanceSide;
	}


	public double getContinuousBookClearingPrice() {
		return continuousBookClearingPrice;
	}


	public double getAuctionInterestClearingPrice() {
		return auctionInterestClearingPrice;
	}


	public double getSsrFilingPrice() {
		return ssrFilingPrice;
	}


	public double getIndicativeMatchPrice() {
		return indicativeMatchPrice;
	}


	public double getUpperCollar() {
		return upperCollar;
	}


	public double getLowerCollar() {
		return lowerCollar;
	}


	public int getAuctionStatus() {
		return auctionStatus;
	}


	public int getFreezeStatus() {
		return freezeStatus;
	}


	public int getNumExtensions() {
		return numExtensions;
	}


	public int getUnPairedQuantity() {
		return unPairedQuantity;
	}


	public String getUnpairedSide() {
		return unpairedSide;
	}


	public String getSignificantImbalance() {
		return significantImbalance;
	}





	int msgType = 0; //1 Numeric
	int sequenceNumber = 0; //2 Numeric
	LocalTime sourceTime; //3 HH:MM:SS.nnnnnnnnn
	//4?
	String symbol = ""; //5 ASCII 
	int symbolSeqNum = 0; //6 Numeric
	double referencePrice; //7 Numeric
	int pairedQty; //8 Numeric
	int totalImbalanceQty; //9 Numeric 
	int marketImbalanceQty; //10 Numeric
	double auctionTime; //11 Numeric 
	String auctionType; //12 Alpha
	String imbalanceSide; //13 Alpha
	double continuousBookClearingPrice; //14 Numeric 
	double auctionInterestClearingPrice; //15 Numeric
	double ssrFilingPrice; //16 Numeric
	double indicativeMatchPrice; //17 Numeric
	double upperCollar; //18 Numeric
	double lowerCollar; //19 Numeric 
	int auctionStatus; //20 Numeric
	int freezeStatus; //21 Numeric
	int numExtensions; //22 Numeric
	int unPairedQuantity; //23 Numeric 
	String unpairedSide; //24 Alpha
	String significantImbalance; //25 Alpha
	
}
