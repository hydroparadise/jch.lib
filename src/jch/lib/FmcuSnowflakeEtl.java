package jch.lib;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import jch.lib.common.QLog;
import jch.lib.test.JchLib_AzureTest;
import jch.lib.test.JchLib_SnowflakeTest;

public class FmcuSnowflakeEtl {
	
	public static void runFullEtl_2(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in ARCU
		//example = "20203112"
		ssMaxValue = JchLib_SnowflakeTest.getSsMaxValue("gcarcu080119",  "ARCUSYM000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting FMCU Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "FMCU_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "ARCUSYM000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,							//file path, size limit in bytes
					"gcarcu080119",            "FMCUAnalytics","cu",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","FMCUAnalytics","cu",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,											//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_fmcuanalyticscu.json", 				//local file for azure blob creds JSON file,
					ssMaxValue, 										//azure blob conatainer directory name to store extracts
					"@stage_fmcuanalyticscu");   						//snowflake stage name to consume from azure to snowflake
			
			QLog.log("ETL Finished...");
		}
	}

	/***
	 * USE THIS ONE
	 * Currently used in production setting
	 */
	public static void runFullEtl(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in ARCU
		//example = "20203112"
		ssMaxValue = JchLib_SnowflakeTest.getSsMaxValue("gcarcu080119",  "ARCUSYM000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting FMCU Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "FMCU_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "ARCUSYM000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,						//file path, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","ARCUSYM000","dbo",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",						//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,										//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_arcusym000dbo.json", 			//local file for azure blob creds JSON file,
					ssMaxValue, 									//azure blob conatainer directory name to store extracts
					"@stage_arcusym000dbo_dev2");   				//snowflake stage name to consume from azure to snowflake

			extractDir = baseDir + "ARCUSYM000arcu\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,			//file path, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","arcu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","ARCUSYM000","arcu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,								//stritct PK, prints full file if differential doesn't apply
					"H:\\azure_blob_arcusym000arcu.json", 
					ssMaxValue, 									//azure blob conatainer directory name to store extracts,
					"@stage_arcusym000arcu_dev");   				//snowflake stage name to consume from azure to snowflake
			
			extractDir = baseDir + "CFSConnectorscu\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,			//file path, size limit in bytes
					"gcarcu080119",            "CFSConnectors","cu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","CFSConnectors","cu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE")),  //time dimensions to look for
					false, true,								//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_cfsconnectors.json", 
					ssMaxValue, "@stage_cfsconnectorscu_dev");													
			
			
			QLog.log("ETL Finished...");
																
		}
	}
	
	
	
	/*
	QLog.filePath = "C:\\temp\\";
	QLog.baseFileName = "recurse";

	try {
		java.sql.Connection cn = null;
		cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json");
		Statement statement = cn.createStatement();
		JchLib_SnowflakeTest.copySqlServerAllViews(cn,"gcarcu080119","ARCUSYM000");
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	*/
	
	
	/***
	 * 
	 */
	public static void runFullEtl_Test1() {
		FmcuSnowflakeEtl.genSfExtractDiffShip_ARCUSYM000dbo();
		FmcuSnowflakeEtl.genSfExtractDiffShip_ARCUSYM000arcu();
		FmcuSnowflakeEtl.genSfExtractDiffShip_CfsConnectorsCu();
	}
	
	
	/****
	 * 
	 */
	public static void copyInit() {
		
		//String zaCredsLoc = "H:\\azure_blob_arcusym000arcu.json";
		//String zaCredsLoc = "H:\\azure_blob_arcusym000dbo.json";
		String zaCredsLoc = "H:\\azure_blob_cfsconnectors.json";
		
		ArrayList<String> azBlobFiles = 
			JchLib_AzureTest.listContainerBlobFiles(zaCredsLoc, null);
		
		for(int i = 0; i < azBlobFiles.size() ; i++) {
			System.out.println(azBlobFiles.get(i));
			
			JchLib_AzureTest.copyBlobFile(zaCredsLoc, azBlobFiles.get(i), null, "init");
			JchLib_AzureTest.deleteBlob(zaCredsLoc, azBlobFiles.get(i), null);
		}
		
	}
	
	
	/*********************************************************************************************************************************************************************************/
	/***************************************************************************      testing area        ****************************************************************************/
	/*********************************************************************************************************************************************************************************/
		
	
	
	

	
	public static void copyTest1() {
		String testFile = "arcu.ARCUATMTerminalCategory_001.csv.gz";

		JchLib_AzureTest.copyBlobFile("H:\\azure_blob_arcusym000arcu.json", testFile, null, "test2");
	}
	
	/***
	 * 
	 */
	public static void deleteTest() {
		
		//file name in directory
		//String blobName = "arcu.ARCUATMTerminalCategory_001.csv.gz";
		//String blobPath = "test2";
		
		//directory - failed
		//String blobName = "1_test.txt";
		//String blobPath = "test";
		
		//directory - fails when folder contains files, passes when empty
		String blobName = null;
		String blobPath = "test";
		
		//directory - failed
		//String blobName = "test";
		//String blobPath = null;
		
		JchLib_AzureTest.deleteBlob("H:\\azure_blob_arcusym000arcu.json", blobName, blobPath);
	}
	

	
	/***
	 * 
	 */
	public static void listTest() {
		System.out.println(JchLib_AzureTest.printURL("H:\\azure_blob_arcusym000arcu.json", "test"));
		
		ArrayList<String> azBlobFiles = 
			JchLib_AzureTest.listContainerBlobFiles("H:\\azure_blob_arcusym000arcu.json", "test");
		

		JchLib_AzureTest.xmlListContainerBlobFiles("H:\\azure_blob_arcusym000arcu.json", null);
		
	}
	
	/***
	 * 
	 */
	public static void putTest() {
		JchLib_AzureTest.putBlobFile("H:\\azure_blob_arcusym000arcu.json", "C:\\temp\\", "1_test.txt", "test2");
	}
	

	/***
	 * Generates ARCUSYM000arcu differential extracts for all tables.
	 */
	public static void genSfExtractDiffShip_ARCUSYM000arcu() {
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\ARCUSYM000arcu\\",  4000000000L,			//file path, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","arcu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","ARCUSYM000","arcu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,								//stritct PK, prints full file if differential doesn't apply
					"H:\\azure_blob_arcusym000arcu.json", null, "@stage_arcusym000arcu_dev");
																

	}
	
	/***
	 * Generates ARCUSYM000arcu differential extracts for all tables.
	 */
	public static void genSfExtractDiffShip_ARCUSYM000dbo() {
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\ARCUSYM000dbo\\",  4000000000L,			//file path, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","ARCUSYM000","dbo",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,								//stritct PK, prints full file if differential doesn't apply
					"H:\\azure_blob_arcusym000dbo.json", null, "@stage_arcusym000dbo_dev2");
																

	}
	
	/***
	 * Generates ARCUSYM000arcu differential extracts for all tables.
	 * Several of the the CFSConnectors tables to ProcessDates are not PK, so setting strictPK to false;
	 */
	public static void genSfExtractDiffShip_CfsConnectorsCu() {
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\CFSConnectorscu\\",  4000000000L,			//file path, size limit in bytes
					"gcarcu080119",            "CFSConnectors","cu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","CFSConnectors","cu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					false, true,								//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_cfsconnectors.json", null, "@stage_cfsconnectorscu_dev");
																

	}
	
	public static void azurePutTest() {
		String azBlobCfsconnectorscuLoc = "H:\\azure_blob_cfsconnectors.json";
		String sourcePath = "C:\\temp\\CFSConnectorscu\\";
		String sourceName = "cu.MADimMemberProfileSnapshot_009.csv.gz";
		
		
		JchLib_AzureTest.putTest(azBlobCfsconnectorscuLoc, sourcePath, sourceName);
	}
	
	/***
	 * Generates CFSConnectorscu differential extracts for all tables.
	 */
	public static void genSfExtractDiff_CFSConnectorscu() {
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\CFSConnectorscu\\",  4000000000L,		//file path, size limit in bytes
					"gcarcu080119",            "CFSConnectors","cu",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","CFSCONNECTORS","CU",	//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true);								//stritct PK, prints full file if differential doesn't apply
																

	}
	
		
	/***
	 * Generates ARCUSYM000arcu differential extracts for all tables.
	 */
	public static void genSfExtractDiff_ARCUSYM000arcu() {
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\ARCUSYM000arcu\\",  4000000000L,			//file path, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","arcu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","ARCUSYM000","arcu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true);								//stritct PK, prints full file if differential doesn't apply
																

	}
	
	
	/***
	 * Generates ARCUSYM000dbo differential extracts for all tables.
	 */
	public static void genSfExtractDiff_ARCUSYM000dbo() {
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\ARCUSYM000dbo\\",  4000000000L,		//file path, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","ARCUSYM000","dbo",	//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",						 //time dimensions to look for
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),
					true, true);							//stritct PK, prints full file if differential doesn't apply
		
	}
	
	
	public static void copyArcuSym000DboTables() throws SQLException, IOException {

		JchLib_SnowflakeTest.copyApiTableData("H:\\snowflake_creds.json","gcarcu080119","ARCUSYM000","dbo","ACCOUNT");
		
	}
	

	/***
	 * Copies database schema and tables from a SQL Server host up to Snowflake instance.
	 * 
	 * host
	 * 	database
	 * 	 schemas
	 *    tables
	 *     columns
	 */
	public static void copyTableSchemas() {
		JchLib_SnowflakeTest.copyFullSqlServerDatabase("H:\\snowflake_creds.json","gcarcu080119","ARCUSYM000");
		JchLib_SnowflakeTest.copyFullSqlServerDatabase("H:\\snowflake_creds.json","gcarcu080119","FMCUAnalytics");
		JchLib_SnowflakeTest.copyFullSqlServerDatabase("H:\\snowflake_creds.json","gcarcu080119","CFSConnectors");
	}
	
	
	
	public static void genSfExtractLimit() {
 
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerTableValueLimit(
				"C:\\temp\\ARCUSYM000dbo\\", "dbo.ACCOUNT",  4000000000L,
				 "gcarcu080119", "ARCUSYM000", "dbo", "ACCOUNT",
				 "H:\\snowflake_creds.json","ARCUSYM000","dbo", "ACCOUNT",
				 "\r\n", "~", "", "\\",
				 "ProcessDate", "20220203");
		
		
		JchLib_SnowflakeTest.writeCsvFromSqlServerTableValueLimit(
				"C:\\temp\\ARCUSYM000dbo\\", "dbo.SAVINGSTRANSACTION",  4000000000L,
				 "gcarcu080119", "ARCUSYM000", "dbo", "SAVINGSTRANSACTION",
				 "H:\\snowflake_creds.json","ARCUSYM000","dbo", "SAVINGSTRANSACTION",
				 "\r\n", "~", "", "\\",
				 "DATETIME", "2021-03-25 00:00:00.0");
	
		JchLib_SnowflakeTest.writeCsvFromSqlServerTableValueLimit(
				"C:\\temp\\ARCUSYM000dbo\\", "dbo.ACTIVITY",  4000000000L,
				 "gcarcu080119", "ARCUSYM000", "dbo", "ACTIVITY",
				 "H:\\snowflake_creds.json","ARCUSYM000","dbo", "ACTIVITY",
				 "\r\n", "~", "", "\\",
				 "DATE", "2021-03-25 00:00:00.0");
	
	}
	
	
	public static void genSfExtractFullTable() {
		ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
		
		//rerun
		exe.execute(() -> 
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
				"C:\\temp\\ARCUSYM000dbo\\", "dbo.LoanDataSupp", 4000000000L,		//file path, file name pref, size limit in bytes
				"gcarcu080119",            "ARCUSYM000","dbo","LoanDataSupp",		//sql sever host, database, schema, table name
				"H:\\snowflake_creds.json","ARCUSYM000","dbo","LoanDataSupp",		//snowflake creds path, database, schema, table name
				"\r\n", "~", "", "\\"));	
		
		//rerun
		exe.execute(() -> 
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
				"C:\\temp\\ARCUSYM000dbo\\", "dbo.ShareDataSupp", 4000000000L,		//file path, file name pref, size limit in bytes
				"gcarcu080119",            "ARCUSYM000","dbo","ShareDataSupp",		//sql sever host, database, schema, table name
				"H:\\snowflake_creds.json","ARCUSYM000","dbo","ShareDataSupp",		//snowflake creds path, database, schema, table name
				"\r\n", "~", "", "\\"));	

		
		//rerun
		exe.execute(() -> 
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
				"C:\\temp\\ARCUSYM000dbo\\", "dbo.AccountDataSupp", 4000000000L,		//file path, file name pref, size limit in bytes
				"gcarcu080119",            "ARCUSYM000","dbo","AccountDataSupp",		//sql sever host, database, schema, table name
				"H:\\snowflake_creds.json","ARCUSYM000","dbo","AccountDataSupp",		//snowflake creds path, database, schema, table name
				"\r\n", "~", "", "\\"));
		
		exe.shutdown();
		while(exe.isShutdown() != false) {}
		
	}
	

	
	public static void genSfExtracts() {
		
		try {
			
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\ARCUSYM000dbo\\", "dbo.ACCOUNT", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo","ACCOUNT",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","ARCUSYM000","dbo","ACCOUNT",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 

			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\ARCUSYM000dbo\\", "dbo.SAVINGS", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo","SAVINGS",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","ARCUSYM000","dbo","SAVINGS",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 

			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\ARCUSYM000dbo\\", "dbo.SAVINGSTRANSACTION", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo","SAVINGSTRANSACTION",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","ARCUSYM000","dbo","SAVINGSTRANSACTION",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 			
			
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\ARCUSYM000dbo\\", "dbo.NAME", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo","NAME",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","ARCUSYM000","dbo","NAME",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 			
			
			
			//run test: pass
			//load test:
			
			//General CSV Specification
			/*
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", "dbo.NAME", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo","NAME",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","ARCUSYM000","dbo","NAME",		//snowflake creds path, database, schema, table name
					"\r\n", ",", "\"", "\\",									//rowDelim, colDelim, text qualifier, escape character
					"PROCESSDATE","20220114");									//field to filter, 
			*/

			//ARCU Compatible
			/*
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", "dbo.NAME", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo","NAME",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","ARCUSYM000","dbo","NAME",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\",										//rowDelim, colDelim, text qualifier, escape character
					"PROCESSDATE","20220116");									//field to filter, 
			*/
			
			//run test: pass
			//load test: fail, found 9 errors
			//errors result due to having the backslash character ("\") as last position of text value
			//Example:  "KINGS CHRISTIAN ACADEMY\"
			//https://docs.snowflake.com/en/sql-reference/functions-regexp.html
			/*
			 * 
			 * Escape Characters and Caveats
			 * As mandated by the POSIX standard, the single backslash character \ is used to escape meta-characters 
			 * (e.g. \* or \?). The backslash character is also used for so-called backslash-sequences (e.g. \w).
			 * Note that the backslash character is further used to insert control characters into SQL strings 
			 * (e.g. \n to insert a newline). As a result, to insert a single backslash character into a SQL string 
			 * literal, the backslash character needs to be escaped (i.e. \\ becomes \).
			 * For example, to insert the backreference \1 into a replacement string literal of REGEXP_LIKE, you 
			 * might need to use \\\\1.
			 */
			/*
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", "dbo.SAVINGSTRANSACTION", 4000000000L,				//file path, file name pref, size limit in bytes
					"gcarcu080119",            "ARCUSYM000","dbo","SAVINGSTRANSACTION",	//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","ARCUSYM000","dbo","SAVINGSTRANSACTION",	//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "",												//rowDelim, colDelim, textQualifier, escape character
					"POSTDATE","1/9/2016");												//field to filter, 
			/*
			
			//loaded with errors
			/*
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.NAME",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcarcu080119",				//String srcSqlHost, 
					"ARCUSYM000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"NAME",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"ARCUSYM000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"NAME");					//String sfTable	
					
			*/
			
			
			
			/*
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.LOAN",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcarcu080119",				//String srcSqlHost, 
					"ARCUSYM000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"LOAN",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"ARCUSYM000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"LOAN");					//String sfTable
			
	
			
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.TRACKING",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcarcu080119",				//String srcSqlHost, 
					"ARCUSYM000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"TRACKING",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"ARCUSYM000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"TRACKING");				//String sfTable
			
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.LOANTRANSACTION",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcarcu080119",				//String srcSqlHost, 
					"ARCUSYM000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"LOANTRANSACTION",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"ARCUSYM000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"LOANTRANSACTION");			//String sfTable
		
			JchLib_SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.SAVINGSTRACKING",		//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcarcu080119",				//String srcSqlHost, 
					"ARCUSYM000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"SAVINGSTRACKING",			//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"ARCUSYM000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"SAVINGSTRACKING");			//String sfTable
			*/
			

			
			//JchLib_SnowflakeTest.copyApiTableData("gcarcu080119","ARCUSYM000","dbo","ACCOUNT",
			//					  "H:\\snowflake_creds.json", "ARCUSYM000","dbo","ACCOUNT");
			
			
			
			
			
			//TimeUnit.SECONDS.sleep(30);
			
			//Synchronous Test: PASS
			//JchLib_SnowflakeTest.copyApiTableData("H:\\snowflake_creds.json","gcarcu080119","ARCUSYM000","dbo","ACCOUNT");
			
			//Asynchronous Test: PASS
			//JchLib_SnowflakeTest.copyApiTableData("H:\\snowflake_creds.json","gcarcu080119","ARCUSYM000","dbo","SAVINGS");
			
			
			//Passed!
			//JchLib_SnowflakeTest.createDatabase("gcarcu080119","ARCUSYM000");
			
			//Passed!
			//JchLib_SnowflakeTest.createDatabase("gcarcu080119","FMCUAnalytics");
			
			//Passed!
			//JchLib_SnowflakeTest.createDatabase("gcarcu080119","CFSConnectors");
			
			//JchLib_SnowflakeTest.createAllDatabaseSchemas("gcarcu080119","ARCUSYM000");
			//JchLib_SnowflakeTest.createAllTablesDatabase("gcarcu080119","ARCUSYM000");
			//JchLib_SnowflakeTest.createAccountTableTest();
			//JchLib_SnowflakeTest.snowflakeDriverTest();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}

	

	
	//main
	
	//java.sql.Connection cn = null;
	//Statement statement = cn.createStatement();
	
	//Pass
	//cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json", "FMCUANALYTICSTEST", "dbo");
	
	//Pass
	//cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json", "FMCUANALYTICSTEST");
	//pass
	//statement.executeUpdate("CREATE SCHEMA arcu");
	//Pass
	//statement.executeUpdate("create or replace table arcu.demo(C1 STRING)");
	
	//Pass
	//cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json");
	//Pass
	//statement.executeUpdate("CREATE DATABASE test");
	
}
