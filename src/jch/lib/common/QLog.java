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
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		msg = ts.toString() + ": " + msg + "\r\n";
		
		if(printConsole == true) System.out.print(msg);
		
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
	
}
