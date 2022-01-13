

package jch.lib;

//General imports I like to keep handy
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import io.grpc.internal.JsonParser;

import javax.xml.parsers.*;
import javafx.application.Application;

import java.io.*;
import java.nio.*;
import java.time.*;
import java.nio.file.*;

import java.util.*;
import java.util.Map.Entry;

import jch.lib.test.*;
import jch.lib.analytics.apps.*;
import jch.lib.db.sqlserver.SqlServerDiscovery;

import jch.lib.db.snowflake.*;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Main {

	public static void main(String[] args) {
		//console output to prove it compiles
		System.out.println("hello jch.lib");
		
		
		try {
			
			JchLib_SnowflakeTest.createAllTablesDatabase("gcarcu080119","ARCUSYM000");
			//JchLib_SnowflakeTest.createAccountTableTest();
			//JchLib_SnowflakeTest.snowflakeDriverTest();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			

	}
	
}

