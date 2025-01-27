package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/***
 * 
 * @author hydro
 
	Msg Type	1	Numeric				3
	SequenceNumber	2	Numeric 		26
	Symbol	3	ASCII 					Deck
	Market ID	4	Numeric 			9
	System ID	5	Numeric				51
	Exchange Code	6	Alpha 			N
	Security Type	7	Alpha			C
	Lot Size	8	Numeric				100
	PrevClosePrice	9	Numeric			146.38
	PrevCloseVolume	10	Numeric 		0
	Price Resolution	11	Numeric 	0
	Round Lot	12	Alpha				N
	MPV	13	Numeric  					.0001
	Unit of Trade	14	Numeric			1


	3,26,DECK,9,51,N,C,100,146.38,0,0,N,.0001,1
	3,29,DRD,9,51,N,H,100,4.74,0,0,N,.0001,1
	3,32,EGHT,9,51,N,C,100,19.91,0,0,N,.0001,1
	3,35,DS PRC,9,51,N,P,100,23.99,0,0,N,.0001,1


 */
public class TaqMsgType003 implements TaqMsg {
	
	public TaqMsgType003() {
		// TODO Auto-generated constructor stub
	}
	
	public TaqMsgType003(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("3")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				symbol = fields[2];
				marketingID = Integer.parseInt(fields[3]);
				systemID = Integer.parseInt(fields[4]);;
				exchangeCode = fields[5];
				securityType = fields[6];
				lotSize = Integer.parseInt(fields[7]);
				prevClosePrice = Double.parseDouble(fields[8]);
				prevCloseVolume = Integer.parseInt(fields[9]);
				priceResolution = Integer.parseInt(fields[10]);
				roundLot = fields[11];
				mpv = Double.parseDouble(fields[12]);
				unitOfTrade = Integer.parseInt(fields[13]);
				
				success = true;
			}
			
		}
		catch(Exception e) {
			System.out.println("TaqMsgType003: Could not parse record. " + e.getMessage());
		}

		
		return success;
	}

	public int getMsgType() {
		return msgType;
	}
	public int getSequenceNumber() {
		return sequenceNumber;
	}
	public String getSymbol() {
		return symbol;
	}
	public int getMarketingID() {
		return marketingID;
	}
	public int getSystemID() {
		return systemID;
	}
	public String getExchangeCode() {
		return exchangeCode;
	}
	public String getSecurityType() {
		return securityType;
	}
	public int getLotSize() {
		return lotSize;
	}
	public double getPrevClosePrice() {
		return prevClosePrice;
	}
	public int getPrevCloseVolume() {
		return prevCloseVolume;
	}
	public int getPriceResolution() {
		return priceResolution;
	}
	public String getRoundLot() {
		return roundLot;
	}
	public double getMpv() {
		return mpv;
	}
	public int getUnitOfTrade() {
		return unitOfTrade;
	}
	
	public LocalTime getSourceTime() {
		return null;
	}

	int msgType = 0;
	int sequenceNumber = 0;
	String symbol = "";
	int marketingID = 0;
	int systemID = 0;
	String exchangeCode = "";
	String securityType = "";
	int lotSize = 0;
	double prevClosePrice = 0.0;
	int prevCloseVolume = 0;
	int priceResolution = 0;
	String roundLot = "";
	double mpv = 0.0;
	int unitOfTrade = 0;
}
