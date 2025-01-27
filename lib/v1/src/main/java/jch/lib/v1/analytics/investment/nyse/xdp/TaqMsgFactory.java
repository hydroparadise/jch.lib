package jch.lib.analytics.investment.nyse.xdp;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
//import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
//import java.util.List;

//import jch.lib.analytics.investment.nyse.xdp.*;
//import jch.lib.common.chunk.ChunkList;
//import jch.lib.common.chunk.StringChunkLink;

/*
 
3 Symbol Index Mapping Message Y Y Y Y
34 Security Status Message Y Y Y Y
100 Add Order Message Y
101 Modify Order Message Y
104 Replace Order Message Y
102 Delete Order Message Y
103 Order Execution Message Y
110 Non-Displayed Trade Message Y
112 Trade Cancel Message Y
111 Cross Trade Message Y
113 Cross Correction Message Y
114 Retail Price Improvement Msg Y
105 Imbalance Message Y Y
106 Add Order Refresh Message Y
140 Quote Message Y
220 Trade Message Y
221 Trade Cancel Message Y
222 Trade Correction Message Y
223 Stock Summary Message Y Y
215 TRF Trade Message Y
216 TRF Trade Cancel Message Y
217 TRF Trade Correction Message Y
218 TRF Prior Day Trade Message Y
219 TRF Prior Day Trade Cancel Message 
 
 */

public abstract class TaqMsgFactory {

	public TaqMsgFactory() {
		// TODO Auto-generated constructor stub
	}
	
	
	
	public static ArrayList<TaqMsg> loadFile(String filePath) {
		ArrayList<TaqMsg> outputList = new ArrayList<TaqMsg>();
		
		Path path = new File(filePath).toPath();	
		
	    @SuppressWarnings("unused")
		String mimeType = "";	    
	    
	    BufferedReader reader = null;

	    try {
			reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);		
			
			while(reader.ready()) {
				String record = reader.readLine();
				
				//1. parse out string to determine record type
				String recType = record.split(",")[0];
				
				switch(recType) {
					case "3":   outputList.add((TaqMsg) new TaqMsgType003(record)); break;
					case "34":  outputList.add((TaqMsg) new TaqMsgType034(record)); break; //Security Status Message Y Y Y Y
					case "100": outputList.add((TaqMsg) new TaqMsgType100(record)); break; //Add Order Message Y
					case "101": outputList.add((TaqMsg) new TaqMsgType101(record)); break; //Modify Order Message Y
					case "104": outputList.add((TaqMsg) new TaqMsgType104(record)); break; //Replace Order Message Y
					case "102": outputList.add((TaqMsg) new TaqMsgType102(record)); break; //Delete Order Message Y
					case "103": outputList.add((TaqMsg) new TaqMsgType103(record)); break; //Order Execution Message Y
					case "110": outputList.add((TaqMsg) new TaqMsgType110(record)); break; //Non-Displayed Trade Message Y
					case "112": outputList.add((TaqMsg) new TaqMsgType112(record)); break; //Trade Cancel Message Y
					case "111": outputList.add((TaqMsg) new TaqMsgType111(record)); break; //Cross Trade Message Y
					case "113": outputList.add((TaqMsg) new TaqMsgType113(record)); break; //Cross Correction Message Y
					case "114": outputList.add((TaqMsg) new TaqMsgType114(record)); break; //Retail Price Improvement Msg Y
					case "105": outputList.add((TaqMsg) new TaqMsgType105(record)); break; //Imbalance Message Y Y
					case "106": outputList.add((TaqMsg) new TaqMsgType106(record)); break; ///Add Order Refresh Message Y
					case "140": outputList.add((TaqMsg) new TaqMsgType140(record)); break; //Quote Message Y
					case "220": break; //Trade Message Y
					case "221": break; //Trade Cancel Message Y
					case "222": break; //Trade Correction Message Y
					case "223": break; //Stock Summary Message Y Y
					case "215": break; //TRF Trade Message Y
					case "216": break; //TRF Trade Cancel Message Y
					case "217": break; //TRF Trade Correction Message Y
					case "218": break; //TRF Prior Day Trade Message Y
					case "219": break; //TRF Prior Day Trade Cancel Message 
					default : System.out.println("Can't find record type");
				
				}
				//2. create appropriate object
				//3. add to list
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Could not open.");
			e.printStackTrace();

		}

		return outputList;
	}
}
