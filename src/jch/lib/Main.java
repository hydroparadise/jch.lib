package jch.lib;

/***
 * 
 * @author harrisonc
 */
import jch.lib.common.QLog;
import jch.lib.test.JchLib_DbScourTest;
import jch.lib.test.JchLib_SnowflakeEtlTest;
import jch.lib.test.JchLib_GoogleTest;
import jch.lib.test.JchLib_RunExec;

public class Main {

	public static void main(String[] args) {
		//console output to prove it compiles
		QLog.log("hello jch.lib");
		
		JchLib_RunExec.runH3();
		
		QLog.log("That's all folks!");
	}


	public static void testGcp() {
		String credsLoc = "c:\\temp\\gcp\\creds.json";
		JchLib_GoogleTest.gcloudReadCreds(credsLoc);
		
	}
	
}



