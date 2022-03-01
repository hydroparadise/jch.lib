package jch.lib.common;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;


/***
 * A very minimal way logging actions to file or console
 * @author harrisonc
 *
 */
public class QLog {
	
	static public void log(String msg) {
		log(msg, false);
		
	}
	
	/***
	 * 
	 * @param msg
	 * @param supressConsole allows for a quick supress from printing to console but still writes to file
	 */
	static public void log(String msg, boolean supressConsole) {
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		msg = ts.toString() + ": " + msg + "\r\n";
		
		
		//limits the lentgh of output msg if specified
		if(charLimit > 0 && msg.length() > charLimit) msg = msg.substring(0,charLimit);
		
		//prints to console
		if(printConsole == true && supressConsole == false) System.out.print(msg);
		
		//prints to file
		FileWriter writer = null;
		if(filePath != null && filePath != "") {
			
			if(baseFileName == null || baseFileName == "") {
				baseFileName = "qlog";
			}
			String fullName = filePath + baseFileName + ext;
			try {
				writer = new FileWriter(fullName, true);
				writer.write(msg);
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				filePath = null;
			}
		}
	}
	

	static public String  filePath = null;
	static public String baseFileName = null;
	static public String ext = ".txt";
	static public boolean printConsole = true;
	static public int charLimit = 0;
}
