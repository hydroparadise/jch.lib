

package jch.lib;

//General imports I like to keep handy
import org.w3c.dom.*;
import org.xml.sax.SAXException;
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

public class Main {

	public static void main(String[] args) {
		//console output to prove it compiles
		System.out.println("hello jch.lib");
		
		
		//JchLib_FinanceTests.mbrTranLoad1();
		//JchLib_DbScourTest.testSQL();
		//JchLib_DbScourTest.testDbSourCreateObjects();
		//System.out.println(SqlServerDiscovery.sqlPrint(SqlServerDiscovery.sqlAllTableColumns("Akcelerant")));
		
		//JchLib_DbScourTest.testDbScourCreateCnStrings();
		//JchLib_DbScourTest.testGetBasicStats();
		
		JchLib_DbScourTest.testUpdateRecordCounts();
		
		

	}
	
}

