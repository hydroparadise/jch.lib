package jch.lib.analytics.investment.nyse.xdp;

import java.time.LocalTime;

/*
 * 
1.1 OVERVIEW
In NYSE Group nomenclature, the term TAQ simply denotes a historical data product.
The TAQ XDP Products are a historical record of all data that was published by the NYSE XDP feeds on a
particular day. Each TAQ XDP product corresponds to a single NYSE XDP real time feed.
Feed TAQ data is available on an end-of-day basis in flat file format, enabling you to recreate the market for
any given time. On a per-market basis, the following feeds are available in TAQ format:

1.2 ACCESS AND DATA FORMAT
All TAQ XDP product files consist of newline-terminated records in ASCII CSV format. The files are
compressed using GNU Zip, and can be downloaded via NYSE Managed File Transfer (MFT) Site,
https://mftus.nyx.com/.
For information on file paths and filenames, see Appendix B.
Note that previously, two formats were supported: this CSV format, and a pipe-delimited zipped format.
The pipe-delimited format has been eliminated.
Each record in the TAQ file corresponds to a single data event in the real time feed. TAQ file records are in
the same order as the data events were in the real time feed.

1.3 NYSE PRODUCTS
1.3.1 NYSE Product Characteristics
PRODUCT MARKETS
TAQ NYSE Integrated Feed NYSE, NYSE American, NYSE Arca, NYSE National
TAQ NYSE BBO NYSE, NYSE American, NYSE Arca, NYSE National
TAQ NYSE Trades NYSE, NYSE American, NYSE Arca, NYSE National, NYSE TRF
TAQ NYSE Order Imbalances NYSE, NYSE American, NYSE Arca
 */

public interface TaqMsg {
	
	int getMsgType();
	int getSequenceNumber();
	String getSymbol();
	public LocalTime getSourceTime();
}


/*
	Msg Type	1	Numeric
	SequenceNumber	2	Numeric 
	Symbol	3	ASCII 
	Market ID	4	Numeric 
	System ID	5	Numeric
	Exchange Code	6	Alpha 
	Security Type	7	Alpha
	Lot Size	8	Numeric
	PrevClosePrice	9	Numeric
	PrevCloseVolume	10	Numeric 
	Price Resolution	11	Numeric 
	Round Lot	12	Alpha
	MPV	13	Numeric  
	Unit of Trade	14	Numeric
*/



