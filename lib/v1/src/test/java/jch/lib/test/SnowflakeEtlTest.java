package jch.lib.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.sql.RowSet;

import org.json.simple.*;
import org.json.simple.parser.*;

import jch.lib.log.QLog;
import jch.lib.cloud.snowflake.SnowflakeCnString;
import jch.lib.cloud.snowflake.SnowflakeDbScour;
import jch.lib.cloud.snowflake.SnowflakeDiscovery;
import jch.lib.db.sqlserver.SqlServerCnString;
import jch.lib.db.sqlserver.SqlServerDbScour;


/***
 * 
 * @author harrisonc
 *
 */
public class SnowflakeEtlTest {
	
	/***
	 * Used to fill in specific value limited portions of a table
	 * 
	 * @param tableName
	 * @param valueLimiterCol
	 * @param valueLimit
	 */
	public static void valLimitRun_ABCAnalyticsCu(String tableName, String valueLimiterCol, String valueLimit) {
		

		
		String etlTitle = "ABC db_serve to Snowflake ETL Value Limit Run";
		String baseDir = "C:\\temp\\Snowflake\\Extracts\\";
		
		String qlogBaseFileName = "ABC_Snowflake_ValLimit_Log_";
		int qlogCharLimit = 2000;
		boolean qlogPrintConsole = true;
		
		String etlName = "ABCAnalyticscu";
		//String etlMethod = null;
		long maxFileSize = 1000000000;
		String srcSqlHost = "db_serve.Targetercu.org";
		//String srcDatabase = "ABCAnalytics";
		//String srcSchema = "cu";
		String sfCredsLoc = "C:\\temp\\Snowflake\\Config\\snowflake_creds.json";
		//String sfDatabase = "ABCAnalytics";
		//String sfSchema = "cu";
		String rowDelim = "\r\n";
		String colDelim = "~";
		String textQualifier = "\"";
		String escapeChar = "\\";
		//ArrayList<String> timeDimensions = new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE"));
		//boolean strictPK = false;
		boolean optionalFull = true;
		String azCredsLoc = "C:\\temp\\Snowflake\\Config\\azure_blob_ABCanalyticscu.json";
		
		String sfStage = "@stage_ABCanalyticscu_dev";
		
		String databaseName = "ABCAnalytics";
		String schemaName = "cu";
		String filePath = baseDir + etlName + "\\" + valueLimit + "\\";
		String fileName = schemaName + "." + tableName;
		
		//-----------------------------------------------------------------------------------------------------------
		//set up logging
		QLog.filePath = baseDir;
		QLog.baseFileName = qlogBaseFileName + valueLimit;
		QLog.charLimit = qlogCharLimit;
		QLog.printConsole = qlogPrintConsole;
		
		QLog.log("Starting " + etlTitle);
		QLog.log("valueLimit: " + valueLimit);
		
		//-----------------------------------------------------------------------------------------------------------
		//Check Table and Columns
		
		//establish an Integrated Security based SQL Server Connection to source SQL Server
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , databaseName);
		
		QLog.log("Cn String: " + srcCnString.getCnString());
		RowSet ssCols = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(),databaseName,schemaName,tableName);
		
		TreeMap<String, String> datatypeCategories = SnowflakeDbScour.setDataTypeCategories(ssCols);	
		String dataTypeCat = datatypeCategories.get(valueLimiterCol.toUpperCase());
		
		QLog.log(dataTypeCat);
		
				
		String azBlobDir = SnowflakeDbScour.fileNamePrep(valueLimit,datatypeCategories.get(valueLimiterCol.toUpperCase()));
		
		
		//check if tables match
		String maxFromTable = SnowflakeDbScour.checkTableAndCols(
				srcSqlHost,	databaseName, schemaName,	tableName,		
				sfCredsLoc, databaseName,	schemaName, tableName, 
				valueLimiterCol, dataTypeCat, true);
		
		//Write SQL to file for support purposes							
		SnowflakeDbScour.writeSqlCreateTable(filePath, srcSqlHost, databaseName, schemaName, tableName, azCredsLoc, azBlobDir);
		
		//-----------------------------------------------------------------------------------------------------------
		//Run
		SnowflakeDbScour.writeCsvFromSqlServerTableValueLimit(
				filePath, fileName, maxFileSize,
				srcSqlHost, databaseName, schemaName, tableName,
				sfCredsLoc, databaseName, schemaName, maxFromTable,
				rowDelim, colDelim, textQualifier, escapeChar,
				valueLimiterCol, valueLimit, 
				azCredsLoc, azBlobDir, sfStage, optionalFull);
		
		
	}
	
	/***
	 * 
	 * @param args
	 * 
	 * .bat file example
	 * "c:\Java\jdk-11.0.14.1+1-jre\bin\java" -Xmx13048M -Dfile.encoding=Cp1252 -classpath "T:\Snowflake\Targeter Snowflake ETL\bin";"T:\Snowflake\Targeter Snowflake ETL\dependencies\*";"T:\Snowflake\Targeter Snowflake ETL\dependencies\SnowFlake v3.13.12\*";"T:\Snowflake\Targeter Snowflake ETL\dependencies\sqljdbc_9.4\enu\*";"T:\Snowflake\Targeter Snowflake ETL\dependencies\google-api-client-assembly-1.30.2-1.30.2\google-api-java-client\libs\*" org.Targetercu.snowflakeetl.Main -j "T:\Snowflake\Config\ABC_snowflake_etl.json"
	 */
	public static void processArgs(String[] args) {
		
		//no arguments provided
		if(args.length == 0) {
			//ABCSnowflakeEtl.runFullEtl("C:\\temp\\Snowflake\\");
		}
		else
		//1 argument provided: assumed file path to JSON config file
		if(args.length == 1) {
			//path to JSON config file
			SnowflakeEtlTest.runJsonFullEtl(args[0]);
		}
		else
		//multiple arguments provided 
		if(args.length > 1) {
			
			int argPos = 0;
			
			//run JSON config file
			if(args[argPos].compareTo("-j") == 0) {
				//path to JSON config file
				SnowflakeEtlTest.runJsonFullEtl(args[++argPos]);

			}
			
			//read JSON config file
			if(args[argPos].compareTo("-r") == 0) {
				//path to JSON config file
				SnowflakeEtlTest.readJsonFullEtl(args[++argPos]);

			}
		}
		else {
			QLog.log("No arguements provided.");
		}
		
		QLog.log("done!");
	}
	
	
	/***
	 * 
	 * @param baseDir
	 */
	public static void readJsonFullEtl(String jsonBaseDir) {
	    JSONObject jsonObj = null;
	    
		try {
			String jsonString = Files.readString(Path.of(jsonBaseDir));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
				    
			//get top level value
			etlTitle = (String) jsonObj.get("etlTitle");	
			baseDir = (String) jsonObj.get("baseDir");

			//get nested value
			JSONObject jsonMaxValue = (JSONObject) jsonObj.get("ssMaxValue");
			maxMethod = (String) jsonMaxValue.get("maxMethod");
			maxSqlHost = (String) jsonMaxValue.get("maxSqlHost");
			maxDatabase = (String) jsonMaxValue.get("maxDatabase");
			maxSchema = (String) jsonMaxValue.get("maxSchema");
			maxTable = (String) jsonMaxValue.get("maxTable");
			maxValueLimiterCol = (String) jsonMaxValue.get("maxValueLimiterCol");
			
			//get nested value
			JSONObject jsonQLog = (JSONObject) jsonObj.get("QLog");
			qlogBaseFileName = (String) jsonQLog.get("qlogBaseFileName");
			qlogCharLimit = new Long((long)jsonQLog.get("qlogCharLimit")).intValue();
			
			//get nested array value
			JSONArray etls = (JSONArray) jsonObj.get("ETLs");
			Iterator etlIterator = etls.iterator();
			while(etlIterator.hasNext()) {
				
				JSONObject etl = (JSONObject) etlIterator.next();
				etlName = (String) etl.get("etlName");
				etlMethod = (String) etl.get("etlMethod");
				maxFileSize = (long) etl.get("maxFileSize");
				srcSqlHost = (String) etl.get("srcSqlHost");
				srcDatabase = (String) etl.get("srcDatabase");
				srcSchema = (String) etl.get("srcSchema");
				sfCredsLoc = (String) etl.get("sfCredsLoc");
				sfDatabase = (String) etl.get("sfDatabase");
				sfSchema = (String) etl.get("sfSchema");
				rowDelim = (String) etl.get("rowDelim");
				colDelim = (String) etl.get("colDelim");
				textQualifier = (String) etl.get("textQualifier");
				escapeChar = (String) etl.get("escapeChar");
				strictPK = (boolean) etl.get("strictPK");
				optionalFull = (boolean) etl.get("optionalFull");
				azCredsLoc = (String) etl.get("azCredsLoc");
				azBlobDir = (String) etl.get("azBlobDir");
				sfStage = (String) etl.get("sfStage");

				JSONArray timeDims = (JSONArray) etl.get("timeDimensions");
				
				timeDimensions = new ArrayList<String>();
				Iterator timeDim = timeDims.iterator();
				while(timeDim.hasNext()) {
					String timeDimName = (String)(timeDim.next());
					timeDimensions.add(timeDimName);
				}
				
				
				
				showVals();
			}
			
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
		}

	}
	
	
	/***
	 * 
	 * @param baseDir
	 */
	public static void runJsonFullEtl(String jsonBaseDir) {
	    JSONObject jsonObj = null;
	    
		try {
			
			//grab and parse json file
			String jsonString = Files.readString(Path.of(jsonBaseDir));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
			//get top level value
			etlTitle = (String) jsonObj.get("etlTitle");	
			baseDir = (String) jsonObj.get("baseDir");	
			
			//get nested value
			JSONObject jsonQLog = (JSONObject) jsonObj.get("QLog");
			qlogBaseFileName = (String) jsonQLog.get("qlogBaseFileName");
			qlogCharLimit = new Long((long)jsonQLog.get("qlogCharLimit")).intValue();
			qlogPrintConsole = (boolean) jsonQLog.get("qlogPrintConsole");		
			
			//get max value parameters
			JSONObject jsonMaxValue = (JSONObject) jsonObj.get("ssMaxValue");
			maxMethod = (String) jsonMaxValue.get("maxMethod");
			maxSqlHost = (String) jsonMaxValue.get("maxSqlHost");
			maxDatabase = (String) jsonMaxValue.get("maxDatabase");
			maxSchema = (String) jsonMaxValue.get("maxSchema");
			maxTable = (String) jsonMaxValue.get("maxTable");
			maxValueLimiterCol = (String) jsonMaxValue.get("maxValueLimiterCol");
			
			//ssMaxValue = SnowflakeDbScour.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
			String ssMaxValue = SnowflakeDbScour.getSsMaxValue(maxSqlHost, maxDatabase, maxSchema, maxTable, maxValueLimiterCol);
			
			QLog.log("ssMaxValue: " + ssMaxValue);
			
			//set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = qlogBaseFileName + ssMaxValue;
			QLog.charLimit = qlogCharLimit;
			QLog.printConsole = qlogPrintConsole;
			
			QLog.log("Starting " + etlTitle);
			
			//get nested array value
			JSONArray etls = (JSONArray) jsonObj.get("ETLs");
			Iterator etlIterator = etls.iterator();
			while(etlIterator.hasNext()) {
				
				JSONObject etl = (JSONObject) etlIterator.next();
				etlName = (String) etl.get("etlName");
				etlMethod = (String) etl.get("etlMethod");
				maxFileSize = (long) etl.get("maxFileSize");
				srcSqlHost = (String) etl.get("srcSqlHost");
				srcDatabase = (String) etl.get("srcDatabase");
				srcSchema = (String) etl.get("srcSchema");
				sfCredsLoc = (String) etl.get("sfCredsLoc");
				sfDatabase = (String) etl.get("sfDatabase");
				sfSchema = (String) etl.get("sfSchema");
				rowDelim = (String) etl.get("rowDelim");
				colDelim = (String) etl.get("colDelim");
				textQualifier = (String) etl.get("textQualifier");
				escapeChar = (String) etl.get("escapeChar");
				strictPK = (boolean) etl.get("strictPK");
				optionalFull = (boolean) etl.get("optionalFull");
				azCredsLoc = (String) etl.get("azCredsLoc");
				azBlobDir = (String) etl.get("azBlobDir");
				sfStage = (String) etl.get("sfStage");

				JSONArray timeDims = (JSONArray) etl.get("timeDimensions");
				
				timeDimensions = new ArrayList<String>();
				Iterator timeDim = timeDims.iterator();
				while(timeDim.hasNext()) {
					String timeDimName = (String)(timeDim.next());
					timeDimensions.add(timeDimName);
				}
				
				showVals();
				
				
				
				String extractDir = null;
				extractDir = baseDir + etlName + "\\" + ssMaxValue + "\\";
				new File(extractDir).mkdirs();
				
				SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
						extractDir,  
						maxFileSize,						//file path, size limit in bytes
						srcSqlHost,    
						srcDatabase,
						srcSchema,				//sql server host, database, schema,
						sfCredsLoc,
						sfDatabase,
						sfSchema,	//local file location of snowflake creds JSON file, database, schema,
						rowDelim, 
						colDelim, 
						textQualifier, 
						escapeChar,						//row delim, column delim, text qualifier, escape char
						timeDimensions,  //time dimensions to look for
						strictPK, 
						optionalFull,										//strict PK, prints full file if differential doesn't apply
						azCredsLoc, 			//local file for azure blob creds JSON file,
						ssMaxValue, 									//azure blob conatainer directory name to store extracts
						sfStage);   				//snowflake stage name to consume from azure to snowflake
				

			}
			
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString());
			QLog.log(e);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString());
			QLog.log(e);
		} catch (Exception e) {
			e.printStackTrace();
			QLog.log(e.toString());
			QLog.log(e);
		}
		

	}
	
	public static void showVals() {
		QLog.log("etlTitle: " + etlTitle);
		QLog.log("baseDir: " + baseDir);
		QLog.log("maxMethod: " + maxMethod);
		QLog.log("maxSqlHost: " + maxSqlHost);
		QLog.log("maxDatabase: " + maxDatabase);
		QLog.log("maxSchema: " + maxSchema);
		QLog.log("maxTable: " + maxTable);
		QLog.log("maxValueLimiterCol: " + maxValueLimiterCol);
		QLog.log("qlogBaseFileName: " + qlogBaseFileName);
		QLog.log("qlogCharLimit: " + qlogCharLimit);
		QLog.log("etlName: " + etlName);
		QLog.log("etlMethod: " + etlMethod);
		QLog.log("maxFileSize: " + maxFileSize);
		QLog.log("srcSqlHost: " + srcSqlHost);
		QLog.log("srcDatabase: " + srcDatabase);
		QLog.log("srcSchema: " + srcSchema);
		QLog.log("sfCredsLoc: " + sfCredsLoc);
		QLog.log("sfDatabase: " + sfDatabase);
		QLog.log("sfSchema: " + sfSchema);
		QLog.log("rowDelim: " + rowDelim);
		QLog.log("colDelim: " + colDelim);
		QLog.log("textQualifier: " + textQualifier);
		QLog.log("escapeChar: " + escapeChar);
		QLog.log("strictPK: " + strictPK);
		QLog.log("optionalFull: " + optionalFull);
		QLog.log("azCredsLoc: " + azCredsLoc);
		QLog.log("azBlobDir: " + azBlobDir);
		QLog.log("sfStage: " + sfStage);
		QLog.log("timeDimensions: " );
		
		for(int i = 0; i < timeDimensions.size(); i++) {
			QLog.log("\t" + timeDimensions.get(i));
		}

	}
	
	static String etlTitle = null;
	static String baseDir = null;
	
	static String maxMethod = null;
	static String maxSqlHost = null;
	static String maxDatabase = null;
	static String maxSchema = null;
	static String maxTable = null;
	static String maxValueLimiterCol = null;
	
	static String qlogBaseFileName = null;
	static int qlogCharLimit;
	static boolean qlogPrintConsole;
	
	static String etlName = null;
	static String etlMethod = null;
	static long maxFileSize;
	static String srcSqlHost = null;
	static String srcDatabase = null;
	static String srcSchema = null;
	static String sfCredsLoc = null;
	static String sfDatabase = null;
	static String sfSchema = null;
	static String rowDelim = null;
	static String colDelim = null;
	static String textQualifier = null;
	static String escapeChar;
	static ArrayList<String> timeDimensions = null;
	static boolean strictPK;
	static boolean optionalFull;
	static String azCredsLoc = null;
	static String azBlobDir = null;
	static String sfStage = null;
	
	
	
	/***
	 */
	public static void runFullEtl(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeDbScour.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting ABC Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  1000000000L,						//file path, size limit in bytes
					"db_serve.Targetercu.org",    "db_serveSYS000","dbo",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","dbo",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",						//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,										//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000dbo.json", 			//local file for azure blob creds JSON file,
					ssMaxValue, 									//azure blob conatainer directory name to store extracts
					"@stage_db_serveSYS000dbo_dev2");   				//snowflake stage name to consume from azure to snowflake

			extractDir = baseDir + "db_serveSYS000db_serve\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  1000000000L,						//file path, size limit in bytes
					"db_serve.Targetercu.org",    "db_serveSYS000","db_serve",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","db_serve",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,								//stritct PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000db_serve.json", 
					ssMaxValue, 									//azure blob conatainer directory name to store extracts,
					"@stage_db_serveSYS000db_serve_dev");   				//snowflake stage name to consume from azure to snowflake
			
			extractDir = baseDir + "cxsConnectorscu\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  1000000000L,						//file path, size limit in bytes
					"db_serve.Targetercu.org",    "cxsConnectors","cu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","cxsConnectors","cu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE")),  //time dimensions to look for
					false, true,								//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_cxsconnectors.json", 
					ssMaxValue, "@stage_cxsconnectorscu_dev");													
			
			extractDir = baseDir + "ABCAnalyticscu\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  1000000000L,			//file path, size limit in bytes
					"db_serve.Targetercu.org",    "ABCAnalytics","cu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","ABCAnalytics","cu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE")),  //time dimensions to look for
					false, true,								//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_ABCanalyticscu.json", 
					ssMaxValue, "@stage_ABCanalyticscu_dev");													
			
			
			QLog.log("ETL Finished...");
																
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void valueLimit_debug1(String baseDir) {
		
		String databaseName = "db_serveSYS000";
		String schemaName = "dbo";
		String tableName = "SAVINGSHOLD";
		String valueLimiterCol = "PROCESSDATE";
		String valueLimit = "20220504";
		String azDir = "20220504";
		
		
		String fileName = schemaName + "." + databaseName;
		String extractDir = baseDir + "db_serveSYS000dbo\\" + valueLimit + "\\";
		new File(extractDir).mkdirs();
		
		SnowflakeDbScour.writeCsvFromSqlServerTableValueLimit(
				extractDir,								//filePath: Output file location. Include last slash (ie, "C:\\temp\\" which becomes "C:\temp\)
				fileName, 								//fileName: Output file name (ie, "dbo.ACCOUNT")
				1000000000L,							//maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
				"gcdb_serve080119",            				//srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
				databaseName,							//srcDatabse: Source Database in which table resides (ie,"DatabaseName")
				schemaName,								//srcSchema: Source Database schema table resides (ie, "dbo")
				tableName,								//srcTable: Source SQL Server table to create extract of (ie."ACCOUNT")
				"H:\\snowflake_creds.json",				//sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
				databaseName,							//sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
				schemaName,								//sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
				tableName,								//sfTable": Remote Snowflake table name to compare columns (ie,"ACCOUNT")
				"\r\n", 								//rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
				"~", 									//colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
				"\"", 									//textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"")
				"\\",									//escapeChar: POSIX control character (ie, "\\")
				valueLimiterCol, 						//valueLimterFCol: Column or Field Name used to limit dataset (ie, "ProcessDate" or "POSTDATE")
				valueLimit,								//valueLimit: Value (ie, int: "20220203" or date: "2/3/2022" or date: "2022/02/03")
				"H:\\azure_blob_db_serveSYS000dbo.json", 	//String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
				azDir,									
				"@stage_db_serveSYS000dbo_dev2", 			//String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
				true);									//delete and reload based on value limit
	}
	
	
	/****
	 * 
	 * @param baseDir
	 */
	public static void runEtl_ValueLimit(String baseDir) {
		
		TreeMap<String, String> dims = new TreeMap<String, String>();
		dims.put("PROCESSDATE", "20220324");
		dims.put("POSTDATE", "3/24/2022");
		dims.put("DATE", "3/24/2022");
		
		String databaseName = "db_serveSYS000";
		String schemaName = "dbo";
		String valueLimit = dims.get("PROCESSDATE");
		
		//Set up logging
		QLog.filePath = baseDir;
		QLog.baseFileName = "ABC_Snowflake_Log_" + valueLimit;
		QLog.charLimit = 50000;
		QLog.log("Starting ABC Snowflake ETL...");
		
		String extractDir = null;
		extractDir = baseDir + "db_serveSYS000dbo\\" + valueLimit + "\\";
		new File(extractDir).mkdirs();
		
		/*TODO: the following method shows an error
		SnowflakeDbScour.writeCsvFromSqlServerAllTablesValueLimit(
				extractDir,								//filePath: Output file location. Include last slash (ie, "C:\\temp\\" which becomes "C:\temp\)
				1000000000L,							//maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
				"gcdb_serve080119",            				//srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
				databaseName,							//srcDatabse: Source Database in which table resides (ie,"DatabaseName")
				schemaName,								//srcSchema: Source Database schema table resides (ie, "dbo")
				"H:\\snowflake_creds.json",				//sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
				databaseName,							//sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
				schemaName,								//sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
				"\r\n", 								//rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
				"~", 									//colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
				"\"", 									//textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"")
				"\\",									//escapeChar: POSIX control character (ie, "\\")
				dims,
				true,									//strictPK: Looks for the value "PK" in column constraint in conjunction timeDimnensios arrayList if set true
				false,									//optionalFull: Prints full file it time dimension can't be determined it set to true
				"H:\\azure_blob_db_serveSYS000dbo.json", 	//String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
				valueLimit,								//String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
				"@stage_db_serveSYS000dbo_dev2"); 			//String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
		*/
			 		 
		
	}
	
	
	public static void runEtl_LoanAppRerun(String baseDir) {
		
		String valueLimiterCol = "Processdate";
		String dataTypeCat = "NUMERIC";
		String valueLimit = "20220225";
		
		String databaseName = "db_serveSYS000";
		String schemaName = "dbo";
		String tableName = "LOANAPP";
		
		String sfMaxFromTable = SnowflakeDbScour.checkTableAndCols(	
				"gcdb_serve080119",            databaseName,schemaName,tableName,		//sql server host, database, schema, tableName
				"H:\\snowflake_creds.json",databaseName,schemaName,tableName,
				 valueLimiterCol,  dataTypeCat,	 true);
		
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeDbScour.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 4000;
			QLog.log("Starting ABC Snowflake ETL...");
			
			String extractDir = null;
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			String fileName = schemaName + "." + tableName;
			
				
			/*
			 * 
			 */
			SnowflakeDbScour.writeCsvFromSqlServerTableDiff(
					extractDir,								//filePath: Output file location. Include last slash (ie, "C:\\temp\\" which becomes "C:\temp\)
					fileName, 								//fileName: Output file name (ie, "dbo.ACCOUNT")
					1000000000L,							//maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
					"gcdb_serve080119",            				//srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
					databaseName,							//srcDatabse: Source Database in which table resides (ie,"DatabaseName")
					schemaName,								//srcSchema: Source Database schema table resides (ie, "dbo")
					tableName,								//srcTable: Source SQL Server table to create extract of (ie."ACCOUNT")
					"H:\\snowflake_creds.json",				//sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
					databaseName,							//sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
					schemaName,								//sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
					tableName,								//sfTable": Remote Snowflake table name to compare columns (ie,"ACCOUNT")
					"\r\n", 								//rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
					"~", 									//colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
					"\"", 									//textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"")
					"\\",									//escapeChar: POSIX control character (ie, "\\")
					valueLimiterCol, 						//valueLimterFCol: Column or Field Name used to limit dataset (ie, "ProcessDate" or "POSTDATE")
					dataTypeCat,							//valueLimit: Value (ie, int: "20220203" or date: "2/3/2022" or date: "2022/02/03")
					sfMaxFromTable,
					"H:\\azure_blob_db_serveSYS000dbo.json", 	//String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
					ssMaxValue,								//String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
					"@stage_db_serveSYS000dbo_dev2" 			//String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
			); 
				
			
			
			/*
			 * Takes source table and creates a *.csv or *.csv.gz based on a source Sql Server host, database, schema,
			 * and table, compares to specified remote Snowfalke instance, compute, database, schema, and table to
			 * build column ordinal sensitive datasets with optionaly compressing the file via GZip.
			 */
			/*
			SnowflakeDbScour.writeCsvFromSqlServerTableValueLimit(
					extractDir,								//filePath: Output file location. Include last slash (ie, "C:\\temp\\" which becomes "C:\temp\)
					fileName, 								//fileName: Output file name (ie, "dbo.ACCOUNT")
					1000000000L,							//maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
					"gcdb_serve080119",            				//srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
					databaseName,							//srcDatabse: Source Database in which table resides (ie,"DatabaseName")
					schemaName,								//srcSchema: Source Database schema table resides (ie, "dbo")
					tableName,								//srcTable: Source SQL Server table to create extract of (ie."ACCOUNT")
					"H:\\snowflake_creds.json",				//sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
					databaseName,							//sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
					schemaName,								//sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
					tableName,								//sfTable": Remote Snowflake table name to compare columns (ie,"ACCOUNT")
					"\r\n", 								//rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
					"~", 									//colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
					"\"", 									//textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"")
					"\\",									//escapeChar: POSIX control character (ie, "\\")
					valueLimiterCol, 						//valueLimterFCol: Column or Field Name used to limit dataset (ie, "ProcessDate" or "POSTDATE")
					valueLimit,								//valueLimit: Value (ie, int: "20220203" or date: "2/3/2022" or date: "2022/02/03")
					"H:\\azure_blob_db_serveSYS000dbo.json", 	//String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
					ssMaxValue,								//String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
					"@stage_db_serveSYS000dbo_dev2", 			//String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
					false);									//boolean sfForceReload: Performs delete on full table if true
			*/
					
			/*
			SnowflakeDbScour.writeCsvFromSqlServerTableFull(
					extractDir, 
					fileName,	 
					1000000000L,			//max file size in bytes
					"gcdb_serve080119",            
					databaseName,
					schemaName,
					tableName,
					"H:\\snowflake_creds.json",
					databaseName,
					schemaName,
					tableName,	//String sfCredsLoc, sfDatabase,sfSchema, sfTable
					"\r\n", 
					"~", 
					"\"", 
					"\\",	//
					"H:\\azure_blob_db_serveSYS000dbo.json",
					ssMaxValue,	
					"@stage_db_serveSYS000dbo_dev2");				
		
					//snowflake stage name to consume from azure to snowflake
			*/
			QLog.log("ETL Finished...");
		}
	}
	
	
	
	public static void runEtl_NameRerun(String baseDir) {
		
		String valueLimiterCol = "Processdate";
		String dataTypeCat = "NUMERIC";
		String valueLimit = null;
		
		String databaseName = "db_serveSYS000";
		String schemaName = "dbo";
		String tableName = "NAME";
		
		
		
		SnowflakeDbScour.checkTableAndCols(	
				"gcdb_serve080119",            databaseName,schemaName,tableName,		//sql server host, database, schema, tableName
				"H:\\snowflake_creds.json",databaseName,schemaName,tableName,
				 valueLimiterCol,  dataTypeCat,	 true);
		
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeDbScour.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 4000;
			QLog.log("Starting ABC Snowflake ETL...");
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			
			new File(extractDir).mkdirs();
			
			//String valueLimiterCol = "Processdate";
			//String valueLimit = ssMaxValue;
			

			
			String fileName = schemaName + "." + tableName;
			
			/*
			SnowflakeDbScour.writeCsvFromSqlServerTableValueLimit(
					extractDir, 
					fileName, 
					1000000000L,								//file path, filename, size limit in bytes
					"gcdb_serve080119",            
					databaseName,
					schemaName,
					tableName,		//sql server host, database, schema, tableName
					"H:\\snowflake_creds.json",
					databaseName,
					schemaName,
					tableName,		//snowflake creds path, database, schema, tableName
					"\r\n", 
					"~", 
					"\"", 
					"\\",								//row delim, column delim, text qualifier, escape char
					valueLimiterCol, 
					valueLimit,
					"H:\\azure_blob_db_serveSYS000dbo.json", 
					ssMaxValue,
					"@stage_db_serveSYS000dbo_dev2", 
					false);
			*/
					
			
			SnowflakeDbScour.writeCsvFromSqlServerTableFull(
					extractDir, 
					fileName,	 
					1000000000L,			//max file size in bytes
					"gcdb_serve080119",            
					databaseName,
					schemaName,
					tableName,
					"H:\\snowflake_creds.json",
					databaseName,
					schemaName,
					tableName,	//String sfCredsLoc, sfDatabase,sfSchema, sfTable
					"\r\n", 
					"~", 
					"\"", 
					"\\",	//
					"H:\\azure_blob_db_serveSYS000dbo.json",
					ssMaxValue,	
					"@stage_db_serveSYS000dbo_dev2");				
		
					//snowflake stage name to consume from azure to snowflake
			
			QLog.log("ETL Finished...");
		}
	}

	

	public static void runTestEtl_2(String baseDir) {
		String ssMaxValue = "20220322";
		String ssPostdate = "3/22/2022";
		
		if(ssMaxValue != null) {
			
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			QLog.log("Starting ABC Snowflake ETL...");
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			
			new File(extractDir).mkdirs();
			
			String databaseName = "db_serveSYS000";
			String schemaName = "dbo";
			String tableName = "SAVINGSTRANSACTION";
			
			String fileName = schemaName + "." + tableName;
			
			SnowflakeDbScour.writeCsvFromSqlServerTableValueLimit(
					extractDir, fileName, 4000000000L,								//file path, filename, size limit in bytes
					"gcdb_serve080119",            databaseName,schemaName,tableName,		//sql server host, database, schema, tableName
					"H:\\snowflake_creds.json",databaseName,schemaName,tableName,		//snowflake creds path, database, schema, tableName
					"\r\n", "~", "\"", "\\",								//row delim, column delim, text qualifier, escape char
					"POSTDATE", ssPostdate,
					"H:\\azure_blob_db_serveSYS000dbo.json", ssMaxValue,
					"@stage_db_serveSYS000dbo_dev2", false);
					
					//snowflake stage name to consume from azure to snowflake
			
			QLog.log("ETL Finished...");
		}
	}
	
	
	public static void runFullEtl_db_serveSYS000dbo(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeTest.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting ABC Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  1000000000L,						//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","dbo",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",						//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,										//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000dbo.json", 			//local file for azure blob creds JSON file,
					ssMaxValue, 									//azure blob conatainer directory name to store extracts
					"@stage_db_serveSYS000dbo_dev2");   				//snowflake stage name to consume from azure to snowflake
		}
	}
	
	
	public static void runFullEtl_ABCAnalyticscu(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeTest.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting ABC Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "ABCAnalyticscu\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  1000000000L,						//file path, size limit in bytes
					"gcdb_serve080119",            "ABCAnalytics","cu",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","ABCAnalytics","cu",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",						//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					false, true,										//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_ABCanalyticscu.json", 			//local file for azure blob creds JSON file,
					ssMaxValue, 									//azure blob conatainer directory name to store extracts
					"@stage_ABCanalyticscu_dev");   				//snowflake stage name to consume from azure to snowflake
		}
	}
	
	
	
	public static void runFullEtl_db_serveSYS000db_serve(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeTest.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting ABC Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeDbScour.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,						//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","dbo",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",						//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,										//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000dbo.json", 			//local file for azure blob creds JSON file,
					ssMaxValue, 									//azure blob conatainer directory name to store extracts
					"@stage_db_serveSYS000dbo_dev2");   				//snowflake stage name to consume from azure to snowflake
		}
	}



	/***
	 */
	public static void runFullEtl_test(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeTest.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting ABC Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,						//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","dbo",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",						//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,										//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000dbo.json", 			//local file for azure blob creds JSON file,
					ssMaxValue, 									//azure blob conatainer directory name to store extracts
					"@stage_db_serveSYS000dbo_dev2");   				//snowflake stage name to consume from azure to snowflake

			extractDir = baseDir + "db_serveSYS000db_serve\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,			//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","db_serve",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","db_serve",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,								//stritct PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000db_serve.json", 
					ssMaxValue, 									//azure blob conatainer directory name to store extracts,
					"@stage_db_serveSYS000db_serve_dev");   				//snowflake stage name to consume from azure to snowflake
			
			extractDir = baseDir + "cxsConnectorscu\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,			//file path, size limit in bytes
					"gcdb_serve080119",            "cxsConnectors","cu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","cxsConnectors","cu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE")),  //time dimensions to look for
					false, true,								//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_cxsconnectors.json", 
					ssMaxValue, "@stage_cxsconnectorscu_dev");													
			
			
			QLog.log("ETL Finished...");
																
		}
	}
	
	
	

	/***
	 * 
	 */
	public static void runFullEtl_Test1() {
		SnowflakeEtlTest.genSfExtractDiffShip_db_serveSYS000dbo();
		SnowflakeEtlTest.genSfExtractDiffShip_db_serveSYS000db_serve();
		SnowflakeEtlTest.genSfExtractDiffShip_cxsConnectorsCu();
	}
	
	
	/****
	 * 
	 */
	public static void copyInit() {
		
		//String zaCredsLoc = "H:\\azure_blob_db_serveSYS000db_serve.json";
		//String zaCredsLoc = "H:\\azure_blob_db_serveSYS000dbo.json";
		String zaCredsLoc = "H:\\azure_blob_cxsconnectors.json";
		
		ArrayList<String> azBlobFiles = 
			AzureTest.listContainerBlobFiles(zaCredsLoc, null);
		
		for(int i = 0; i < azBlobFiles.size() ; i++) {
			System.out.println(azBlobFiles.get(i));
			
			AzureTest.copyBlobFile(zaCredsLoc, azBlobFiles.get(i), null, "init");
			AzureTest.deleteBlob(zaCredsLoc, azBlobFiles.get(i), null);
		}
		
	}
	
	
	/*********************************************************************************************************************************************************************************/
	/***************************************************************************      testing area        ****************************************************************************/
	/*********************************************************************************************************************************************************************************/
		
	
	
	

	
	public static void copyTest1() {
		String testFile = "db_serve.db_serveATMTerminalCategory_001.csv.gz";

		AzureTest.copyBlobFile("H:\\azure_blob_db_serveSYS000db_serve.json", testFile, null, "test2");
	}
	
	/***
	 * 
	 */
	public static void deleteTest() {
		

		
		//directory - fails when folder contains files, passes when empty
		String blobName = null;
		String blobPath = "test";
		
		//directory - failed
		//String blobName = "test";
		//String blobPath = null;
		
		AzureTest.deleteBlob("H:\\azure_blob_db_serveSYS000db_serve.json", blobName, blobPath);
	}
	

	
	/***
	 * 
	 */
	public static void listTest() {
		System.out.println(AzureTest.printURL("H:\\azure_blob_db_serveSYS000db_serve.json", "test"));
		
		ArrayList<String> azBlobFiles = 
			AzureTest.listContainerBlobFiles("H:\\azure_blob_db_serveSYS000db_serve.json", "test");
		

		AzureTest.xmlListContainerBlobFiles("H:\\azure_blob_db_serveSYS000db_serve.json", null);
		
	}
	
	/***
	 * 
	 */
	public static void putTest() {
		AzureTest.putBlobFile("H:\\azure_blob_db_serveSYS000db_serve.json", "C:\\temp\\", "1_test.txt", "test2");
	}
	

	/***
	 * Generates db_serveSYS000db_serve differential extracts for all tables.
	 */
	public static void genSfExtractDiffShip_db_serveSYS000db_serve() {
		
		SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\db_serveSYS000db_serve\\",  4000000000L,			//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","db_serve",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","db_serve",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,											//stritct PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000db_serve.json", null, "@stage_db_serveSYS000db_serve_dev");
																

	}
	
	/***
	 * Generates db_serveSYS000db_serve differential extracts for all tables.
	 */
	public static void genSfExtractDiffShip_db_serveSYS000dbo() {
		
		SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\db_serveSYS000dbo\\",  4000000000L,			//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","dbo",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,								//stritct PK, prints full file if differential doesn't apply
					"H:\\azure_blob_db_serveSYS000dbo.json", null, "@stage_db_serveSYS000dbo_dev2");
																

	}
	
	/***
	 * Generates cxsConnectors differential extracts for all tables.
	 * Several of the the cxsConnectors tables to ProcessDates are not PK, so setting strictPK to false;
	 */
	public static void genSfExtractDiffShip_cxsConnectorsCu() {
		
		SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\cxsConnectorscu\\",  4000000000L,			//file path, size limit in bytes
					"gcdb_serve080119",            "cxsConnectors","cu",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","cxsConnectors","cu",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					false, true,								//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_cxsconnectors.json", null, "@stage_cxsconnectorscu_dev");
																

	}
	
	public static void azurePutTest() {
		String azBlobcxsconnectorscuLoc = "H:\\azure_blob_cxsconnectors.json";
		String sourcePath = "C:\\temp\\cxsConnectorscu\\";
		String sourceName = "cu.MADimMemberProfileSnapshot_009.csv.gz";
		
		
		AzureTest.putTest(azBlobcxsconnectorscuLoc, sourcePath, sourceName);
	}
	
	/***
	 * Generates cxsConnectorscu differential extracts for all tables.
	 */
	public static void genSfExtractDiff_cxsConnectorscu() {
		
		SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\cxsConnectorscu\\",  4000000000L,		//file path, size limit in bytes
					"gcdb_serve080119",            "cxsConnectors","cu",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","cxsCONNECTORS","CU",	//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true);								//stritct PK, prints full file if differential doesn't apply
																

	}
	
		
	/***
	 * Generates db_serveSYS000db_serve differential extracts for all tables.
	 */
	public static void genSfExtractDiff_db_serveSYS000db_serve() {
		
		SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\db_serveSYS000db_serve\\",  4000000000L,			//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","db_serve",		//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","db_serve",		//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true);								//stritct PK, prints full file if differential doesn't apply
																

	}
	
	
	/***
	 * Generates db_serveSYS000dbo differential extracts for all tables.
	 */
	public static void genSfExtractDiff_db_serveSYS000dbo() {
		
		SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					"C:\\temp\\db_serveSYS000dbo\\",  4000000000L,		//file path, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","db_serveSYS000","dbo",	//snowflake creds path, database, schema,
					"\r\n", "~", "\"", "\\",						 //time dimensions to look for
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),
					true, true);							//stritct PK, prints full file if differential doesn't apply
		
	}
	
	
	public static void copydb_serveSYS000DboTables() throws SQLException, IOException {

		SnowflakeTest.copyApiTableData("H:\\snowflake_creds.json","gcdb_serve080119","db_serveSYS000","dbo","ACCOUNT");
		
	}
	

	/***
	 * 
	 */
	public static void checkTalbeTest_1() {
		String databaseName = "db_serveSYS000";
		String schemaName = "dbo";
		String tableName = "SAVINGSTRANSACTION";
		
		SnowflakeDbScour.checkTableAndCols(
				"GCdb_serve080119", databaseName, schemaName, tableName, 
				"H:\\\\snowflake_creds.json", databaseName, schemaName, tableName, 
				"POSTDATE", "DATETIME",	true);
		
		
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
		SnowflakeTest.copyFullSqlServerDatabase("H:\\snowflake_creds.json","gcdb_serve080119","db_serveSYS000");
		SnowflakeTest.copyFullSqlServerDatabase("H:\\snowflake_creds.json","gcdb_serve080119","ABCAnalytics");
		SnowflakeTest.copyFullSqlServerDatabase("H:\\snowflake_creds.json","gcdb_serve080119","cxsConnectors");
	}
	
	
	
	public static void genSfExtractLimit() {
 
		
		SnowflakeTest.writeCsvFromSqlServerTableValueLimit(
				"C:\\temp\\db_serveSYS000dbo\\", "dbo.ACCOUNT",  4000000000L,
				 "gcdb_serve080119", "db_serveSYS000", "dbo", "ACCOUNT",
				 "H:\\snowflake_creds.json","db_serveSYS000","dbo", "ACCOUNT",
				 "\r\n", "~", "", "\\",
				 "ProcessDate", "20220203");
		
		
		SnowflakeTest.writeCsvFromSqlServerTableValueLimit(
				"C:\\temp\\db_serveSYS000dbo\\", "dbo.SAVINGSTRANSACTION",  4000000000L,
				 "gcdb_serve080119", "db_serveSYS000", "dbo", "SAVINGSTRANSACTION",
				 "H:\\snowflake_creds.json","db_serveSYS000","dbo", "SAVINGSTRANSACTION",
				 "\r\n", "~", "", "\\",
				 "DATETIME", "2021-03-25 00:00:00.0");
	
		SnowflakeTest.writeCsvFromSqlServerTableValueLimit(
				"C:\\temp\\db_serveSYS000dbo\\", "dbo.ACTIVITY",  4000000000L,
				 "gcdb_serve080119", "db_serveSYS000", "dbo", "ACTIVITY",
				 "H:\\snowflake_creds.json","db_serveSYS000","dbo", "ACTIVITY",
				 "\r\n", "~", "", "\\",
				 "DATE", "2021-03-25 00:00:00.0");
	
	}
	
	
	public static void genSfExtractFullTable() {
		ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
		
		//rerun
		exe.execute(() -> 
			SnowflakeTest.writeCsvFromSqlServerTableFull(
				"C:\\temp\\db_serveSYS000dbo\\", "dbo.LoanDataSupp", 4000000000L,		//file path, file name pref, size limit in bytes
				"gcdb_serve080119",            "db_serveSYS000","dbo","LoanDataSupp",		//sql sever host, database, schema, table name
				"H:\\snowflake_creds.json","db_serveSYS000","dbo","LoanDataSupp",		//snowflake creds path, database, schema, table name
				"\r\n", "~", "", "\\"));	
		
		//rerun
		exe.execute(() -> 
			SnowflakeTest.writeCsvFromSqlServerTableFull(
				"C:\\temp\\db_serveSYS000dbo\\", "dbo.ShareDataSupp", 4000000000L,		//file path, file name pref, size limit in bytes
				"gcdb_serve080119",            "db_serveSYS000","dbo","ShareDataSupp",		//sql sever host, database, schema, table name
				"H:\\snowflake_creds.json","db_serveSYS000","dbo","ShareDataSupp",		//snowflake creds path, database, schema, table name
				"\r\n", "~", "", "\\"));	

		
		//rerun
		exe.execute(() -> 
			SnowflakeTest.writeCsvFromSqlServerTableFull(
				"C:\\temp\\db_serveSYS000dbo\\", "dbo.AccountDataSupp", 4000000000L,		//file path, file name pref, size limit in bytes
				"gcdb_serve080119",            "db_serveSYS000","dbo","AccountDataSupp",		//sql sever host, database, schema, table name
				"H:\\snowflake_creds.json","db_serveSYS000","dbo","AccountDataSupp",		//snowflake creds path, database, schema, table name
				"\r\n", "~", "", "\\"));
		
		exe.shutdown();
		while(exe.isShutdown() != false) {}
		
	}
	

	
	public static void genSfExtracts() {
		
		try {
			
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\db_serveSYS000dbo\\", "dbo.ACCOUNT", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo","ACCOUNT",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","db_serveSYS000","dbo","ACCOUNT",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 

			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\db_serveSYS000dbo\\", "dbo.SAVINGS", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo","SAVINGS",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","db_serveSYS000","dbo","SAVINGS",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 

			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\db_serveSYS000dbo\\", "dbo.SAVINGSTRANSACTION", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo","SAVINGSTRANSACTION",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","db_serveSYS000","dbo","SAVINGSTRANSACTION",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 			
			
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\db_serveSYS000dbo\\", "dbo.NAME", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo","NAME",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","db_serveSYS000","dbo","NAME",		//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "\\");											//rowDelim, colDelim, text qualifier, escape character
					//"PROCESSDATE","20220116");									//field to filter, 			
			
			
			//run test: pass
			//load test:
			
			//General CSV Specification
			/*
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", "dbo.NAME", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo","NAME",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","db_serveSYS000","dbo","NAME",		//snowflake creds path, database, schema, table name
					"\r\n", ",", "\"", "\\",									//rowDelim, colDelim, text qualifier, escape character
					"PROCESSDATE","20220114");									//field to filter, 
			*/

			//db_serve Compatible
			/*
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", "dbo.NAME", 4000000000L,						//file path, file name pref, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo","NAME",		//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","db_serveSYS000","dbo","NAME",		//snowflake creds path, database, schema, table name
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
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", "dbo.SAVINGSTRANSACTION", 4000000000L,				//file path, file name pref, size limit in bytes
					"gcdb_serve080119",            "db_serveSYS000","dbo","SAVINGSTRANSACTION",	//sql sever host, database, schema, table name
					"H:\\snowflake_creds.json","db_serveSYS000","dbo","SAVINGSTRANSACTION",	//snowflake creds path, database, schema, table name
					"\r\n", "~", "", "",												//rowDelim, colDelim, textQualifier, escape character
					"POSTDATE","1/9/2016");												//field to filter, 
			/*
			
			//loaded with errors
			/*
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.NAME",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcdb_serve080119",				//String srcSqlHost, 
					"db_serveSYS000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"NAME",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"db_serveSYS000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"NAME");					//String sfTable	
					
			*/
			
			
			
			/*
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.LOAN",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcdb_serve080119",				//String srcSqlHost, 
					"db_serveSYS000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"LOAN",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"db_serveSYS000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"LOAN");					//String sfTable
			
	
			
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.TRACKING",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcdb_serve080119",				//String srcSqlHost, 
					"db_serveSYS000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"TRACKING",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"db_serveSYS000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"TRACKING");				//String sfTable
			
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.LOANTRANSACTION",				//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcdb_serve080119",				//String srcSqlHost, 
					"db_serveSYS000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"LOANTRANSACTION",					//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"db_serveSYS000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"LOANTRANSACTION");			//String sfTable
		
			SnowflakeTest.writeCsvFromSqlServerTableFull(
					"C:\\temp\\", 				//String filePath, 
					"dbo.SAVINGSTRACKING",		//String fileName (do not include extension),
					4000000000L,				//max file size in bytes
					"gcdb_serve080119",				//String srcSqlHost, 
					"db_serveSYS000",				//String srcDatabse, 
					"dbo",						//String srcSchema, 
					"SAVINGSTRACKING",			//String srcTable,
					"H:\\snowflake_creds.json",	//String sfCredsLoc, 
					"db_serveSYS000",				//String sfDatabase, 
					"dbo",						//String sfSchema, 
					"SAVINGSTRACKING");			//String sfTable
			*/
			

			
			//SnowflakeTest.copyApiTableData("gcdb_serve080119","db_serveSYS000","dbo","ACCOUNT",
			//					  "H:\\snowflake_creds.json", "db_serveSYS000","dbo","ACCOUNT");
			
			
			
			
			
			//TimeUnit.SECONDS.sleep(30);
			
			//Synchronous Test: PASS
			//SnowflakeTest.copyApiTableData("H:\\snowflake_creds.json","gcdb_serve080119","db_serveSYS000","dbo","ACCOUNT");
			
			//Asynchronous Test: PASS
			//SnowflakeTest.copyApiTableData("H:\\snowflake_creds.json","gcdb_serve080119","db_serveSYS000","dbo","SAVINGS");
			
			
			//Passed!
			//SnowflakeTest.createDatabase("gcdb_serve080119","db_serveSYS000");
			
			//Passed!
			//SnowflakeTest.createDatabase("gcdb_serve080119","ABCAnalytics");
			
			//Passed!
			//SnowflakeTest.createDatabase("gcdb_serve080119","cxsConnectors");
			
			//SnowflakeTest.createAllDatabaseSchemas("gcdb_serve080119","db_serveSYS000");
			//SnowflakeTest.createAllTablesDatabase("gcdb_serve080119","db_serveSYS000");
			//SnowflakeTest.createAccountTableTest();
			//SnowflakeTest.snowflakeDriverTest();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}

	
	public static void runTestEtl_1(String baseDir) {
		String ssMaxValue = "20220321";
		
		if(ssMaxValue != null) {
			
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			QLog.log("Starting ABC Snowflake ETL...");
			
			String extractDir = null;
			
			extractDir = baseDir + "cxsConnectorscu\\" + ssMaxValue + "\\";
			
			new File(extractDir).mkdirs();
			
			String databaseName = "cxsConnectors";
			String schemaName = "cu";
			
			//String tableName = "LAFactLoanAppSnapshot";
			//String tableName = "LAFactLoanSnapshot";
			//String tableName = "LAMLUnderwritingMetricsStage";
			//String tableName = "LAPSCUBalanceStatusStage";
			//String tableName = "MADimAccountSnapshot";
			//String tableName = "LAPSCUMonetaryTranStageError";
			//String tableName = "MADimMemberProfileSnapshot";
			String tableName = "LAMLProposedHousingExpenseStage";
			
			String fileName = schemaName + "." + tableName;
			
			SnowflakeDbScour.writeCsvFromSqlServerTableValueLimit(
					extractDir, fileName, 4000000000L,								//file path, filename, size limit in bytes
					"gcdb_serve080119",            databaseName,schemaName,tableName,		//sql server host, database, schema, tableName
					"H:\\snowflake_creds.json",databaseName,schemaName,tableName,		//snowflake creds path, database, schema, tableName
					"\r\n", "~", "\"", "\\",								//row delim, column delim, text qualifier, escape char
					"ProcessDate", ssMaxValue,
					"H:\\azure_blob_cxsconnectors.json", ssMaxValue,
					"@stage_cxsconnectorscu_dev", false);
					
					//snowflake stage name to consume from azure to snowflake
			
			QLog.log("ETL Finished...");
		}
	}
	
	
	public static void runFullEtl_3(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeTest.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting ABC Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,							//file path, size limit in bytes
					"gcdb_serve080119",            "ABCAnalytics","cu",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","ABCAnalytics","cu",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,											//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_ABCanalyticscu.json", 				//local file for azure blob creds JSON file,
					ssMaxValue, 										//azure blob conatainer directory name to store extracts
					"@stage_ABCanalyticscu");   						//snowflake stage name to consume from azure to snowflake
			
			QLog.log("ETL Finished...");
		}
	}
	
	
	public static void runFullEtl_2(String baseDir) {
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		
		//Pulls most recent process date from accounts table in db_serve
		//example = "20203112"
		ssMaxValue = SnowflakeTest.getSsMaxValue("gcdb_serve080119",  "db_serveSYS000", "dbo", "ACCOUNT", "PROCESSDATE");
		
		if(ssMaxValue != null) {
			QLog.log("Starting ABC Snowflake ETL...");
			
			//Set up logging
			QLog.filePath = baseDir;
			QLog.baseFileName = "ABC_Snowflake_Log_" + ssMaxValue;
			QLog.charLimit = 2000;
			
			String extractDir = null;
			
			extractDir = baseDir + "db_serveSYS000dbo\\" + ssMaxValue + "\\";
			new File(extractDir).mkdirs();
			SnowflakeTest.writeCsvFromSqlServerAllTablesDiff(
					extractDir,  4000000000L,							//file path, size limit in bytes
					"gcdb_serve080119",            "ABCAnalytics","cu",	//sql server host, database, schema,
					"H:\\snowflake_creds.json","ABCAnalytics","cu",	//local file location of snowflake creds JSON file, database, schema,
					"\r\n", "~", "\"", "\\",							//row delim, column delim, text qualifier, escape char
					new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")),  //time dimensions to look for
					true, true,											//strict PK, prints full file if differential doesn't apply
					"H:\\azure_blob_ABCanalyticscu.json", 				//local file for azure blob creds JSON file,
					ssMaxValue, 										//azure blob conatainer directory name to store extracts
					"@stage_ABCanalyticscu");   						//snowflake stage name to consume from azure to snowflake
			
			QLog.log("ETL Finished...");
		}
	}

}
