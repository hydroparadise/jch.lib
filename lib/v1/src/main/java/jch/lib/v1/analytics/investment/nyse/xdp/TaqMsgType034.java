package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

import org.apache.commons.lang3.StringUtils;

public class TaqMsgType034 implements TaqMsg {
/*
  	MsgType 1 Numeric
	SequenceNumber 2 Numeric 
	SourceTime 3 HH:MM:SS.nnnnnnnnn
	Symbol 4 ASCII
	SymbolSeqNum 5 Numeric
	Security Status 6 ASCII
	Halt Condition 7 ASCII
	Price 1 8 Numeric 
	Price 2 9 Numeric
	SSR Triggering Exchange ID 10 Alpha
	SSR Triggering Volume 11 Numeric
	Time 12 Numeric 
	SSRState 13 ASCII
	MarketState 14 ASCII 

	34,18023,00:22:16.321198336,HLIO,1,P,~,,,,,,~,P
	34,18026,00:22:16.321229568,CSOD,1,P,~,,,,,,~,P
	34,18029,00:22:16.321262848,ASTC,1,P,~,,,,,,~,P
	34,18032,00:22:16.321275648,GNMX,1,P,~,,,,,,~,P
	34,18035,00:22:16.321296128,GCVRZ,1,P,~,,,,,,~,P
	34,18038,00:22:16.321304320,GMLP,1,P,~,,,,,,~,P
	34,18041,00:22:16.321358336,FNK,1,P,~,,,,,,~,P
	34,3113705,20:00:00.012770048,JCI,21955,X,~,,,,,,~,X
	34,32099,09:32:37.228747520,GDV RTWI,4,A,~,,,,,,E,O

 */
	
	
	public TaqMsgType034() {
		// TODO Auto-generated constructor stub
	}
	
	public TaqMsgType034(String record) {
		parseRecord(record);
	}
	
	private boolean parseRecord(String record) {
		boolean success = false;
		
		//try {
			//Should be 14 fields
			String fields[] = record.split(",");
			
			if(fields[0].equals("34")) {
				
				msgType = Integer.parseInt(fields[0]);
				sequenceNumber = Integer.parseInt(fields[1]);
				sourceTime = LocalTime.parse(fields[2]);
				symbol = fields[3];
				symbolSeqNum = Integer.parseInt(fields[4]);
				securityStatus = fields[5];
				haltCondition = fields[6];
				price1 = Double.parseDouble(StringUtils.defaultIfEmpty(fields[7],"0"));
				price2 = Double.parseDouble(StringUtils.defaultIfEmpty(fields[8],"0"));
				ssrTriggeringExchangeID = fields[9];
				ssrTriggeringVolume = Integer.parseInt(StringUtils.defaultIfEmpty(fields[10],"0"));
				time = LocalTime.parse(StringUtils.defaultIfEmpty(fields[11],"00:00:00"));
				ssrState = fields[12];
				marketState = fields[13];
				

				/*
			  	MsgType 1 Numeric
				SequenceNumber 2 Numeric 
				SourceTime 3 HH:MM:SS.nnnnnnnnn
				Symbol 4 ASCII
				SymbolSeqNum 5 Numeric
				Security Status 6 ASCII
				Halt Condition 7 ASCII
				Price 1 8 Numeric 
				Price 2 9 Numeric
				SSR Triggering Exchange ID 10 Alpha
				SSR Triggering Volume 11 Numeric
				Time 12 Numeric 
				SSRState 13 ASCII
				MarketState 14 ASCII
				*/
				
				success = true;
			}
		/*	
		}
		catch(Exception e) {
			System.out.println("TaqMsgType034: Could not parse record. " + e.getLocalizedMessage());
			System.out.println(record);
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
	public String getSecurityStatus() {
		return securityStatus;
	}
	public String getHaltCondition() {
		return haltCondition;
	}
	public double getPrice1() {
		return price1;
	}
	public double getPrice2() {
		return price2;
	}
	public String getSsrTriggeringExchangeID() {
		return ssrTriggeringExchangeID;
	}
	public int getSsrTriggeringVolume() {
		return ssrTriggeringVolume;
	}
	public LocalTime getTime() {
		return time;
	}
	public String getSsrState() {
		return ssrState;
	}
	public String getMarketState() {
		return marketState;
	}


	int msgType = 0;
	int sequenceNumber = 0; 
	LocalTime sourceTime;// 3 HH:MM:SS.nnnnnnnnn
	String symbol = "";
	int symbolSeqNum = 0;
	String securityStatus = "";
	String haltCondition = "";
	double price1 = 0; 
	double price2 = 0;
	String ssrTriggeringExchangeID = "";
	int ssrTriggeringVolume = 0;
	LocalTime time; 
	String ssrState = "";
	String marketState = ""; 

}
