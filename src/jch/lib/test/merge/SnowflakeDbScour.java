package jch.lib.test.merge;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import jch.lib.common.*;
import jch.lib.db.snowflake.*;
import jch.lib.db.sqlserver.*;
import jch.lib.cloud.azure.*;
import jch.lib.common.compress.*;

import net.snowflake.client.jdbc.SnowflakeStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//import java.util.Map.Entry;

/*
 * Snowflake concurrent statements
 * MAX_CONCURRENCY_LEVEL = 8
 * 
 * File Sizing Best Practices and Limitations
 * For best load performance and to avoid size limitations, consider the following data file sizing 
 * guidelines. Note that these recommendations apply to bulk data loads as well as continuous loading using Snowpipe.
 *
 * General File Sizing Recommendations
 * The number of load operations that run in parallel cannot exceed the number of data files to be 
 * loaded. To optimize the number of parallel operations for a load, we recommend aiming to produce 
 * data files roughly 100-250 MB (or larger) in size compressed.
 * https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare.html#label-data-load-file-sizing-best-practices
 * 
 * @author harrisonc
 * 
 */
public class SnowflakeDbScour {
	static int API_PACK_SIZE = 1900000;
	static int MAX_CONCURRENCY_LEVEL = 4;
	static int SHOW_ROW_RECORD_COUNT = 50000;
	
	//
	SnowflakeDbScour() {
		
	}
	

	/***
	 * Connects to a Sql Server with a Database and Schema context, iterates through all tables and
	 * compares a table against a given Snowflake Database and Schema context with a table of the 
	 * same name, and creates a compressed CSV file of the full table within the SQL S
	 * Once the file has been created, the file will be sent to a specified Azure Blob Storage container.
	 * 
	 * TODO: suspend warehouse after complete
	 * 
	 * @param String filePath: Output file location. Include last slash (ie, "C:\\temp\\" which becomes "C:\temp\)
	 * @param long maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb")
	 * @param String srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param String srcDatabase: Source Database in which table resides (ie,"DatabaseName")
	 * @param String srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param String sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C:\\snowflake_creds.json")
	 * @param String sfDatabase: Remote Snowflake database name (ie, "DatabaseName")
	 * @param String sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param String rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
	 * @param String colDelim: Output file column/field delimiter(ie, "," or "~" or "\t")
	 * @param String textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"" -> ")
	 * @param String escapeChar: POSIX control character (ie, "\\" -> \)
	 * @param String azCredsLoc: Location of Azure credentials of an Azure Blob Container instance (ie, "C:\\azure_creds.json")
	 * @param String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
	 * @param String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
	 */
	static public void writeCsvFromSqlServerAllTablesFull(String filePath ,long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema,
			 String sfCredsLoc, String sfDatabase, String sfSchema,
			 String rowDelim, String colDelim, String textQualifier, String escapeChar,
			 String azCredsLoc, String azBlobDir, String sfStage) {
		
		//Get Sql Server Table information in a RowSet, connection opening and closing handled by called functions
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null, srcDatabase);
		RowSet ssTables = SqlServerDbScour.getSrcTables(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema);

		try {
			ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
			
			//start iterating through tables
			while(ssTables.next()) {
				
				final String schemaName = ssTables.getString("TABLE_SCHEMA");
				final String tableName = ssTables.getString("TABLE_NAME");
				final String fileName = schemaName + "." + tableName;
				
				//check if tables match
				//if destination table doesn't exit, create new table
				//if table columns do match exactly, rename existing table with postdate/processdate in name and create new table
				//returns true if there is a destination table that matches the source table
				SnowflakeDbScour.checkTableAndCols(
						srcSqlHost,		//Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
						srcDatabase,	//Source Database in which table resides (ie,"DatabaseName")
						srcSchema,		//Source Database schema table resides (ie, "dbo")
						tableName,		//
						sfCredsLoc,		//Location of Snowflake credentials specified via JSON (ie, "C:\\snowflake_creds.json")
						sfDatabase,		//Remote Snowflake database name (ie, "DatabaseName")
						sfSchema,		//Remote Snowflake schema name to compare columns(ie,"DBO")
						tableName, 		//
						null, 			//
						null, 			//
						true);			//	
				
				
				
				//submit method for Async 
				exe.execute(()->  
					SnowflakeDbScour.writeCsvFromSqlServerTableFull(
							filePath,		//
							fileName,		//
							maxFileSize,	//
							srcSqlHost,		//Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
							srcDatabase,	//Source Database in which table resides (ie,"DatabaseName")
							srcSchema,		//Source Database schema table resides (ie, "dbo")
							tableName,		//
							sfCredsLoc,		//Location of Snowflake credentials specified via JSON (ie, "C:\\snowflake_creds.json")
							sfDatabase,		//Remote Snowflake database name (ie, "DatabaseName")
							sfSchema,		//Remote Snowflake schema name to compare columns(ie,"DBO")
							tableName,		//
							rowDelim,		//
							colDelim,		//
							textQualifier,	//
							escapeChar,		//
							azCredsLoc,		//A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
							azBlobDir,		//Location of Azure credentials of an Azure Blob Container instance (ie, "C:\\azure_creds.json")
							sfStage)		//The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")	
				);
			}
			
			exe.shutdown();
		
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	}
	
	
	/***
	 * Connects to a Sql Server with a Database and Schema context, iterates through all tables and
	 * compares a table against a given Snowflake Database and Schema context with a table of the 
	 * same name, and creates a compressed CSV file of the difference between the two tables.
	 * The file names will contain the specific time slice the file represents in YYYYMMDD format.
	 * Once the file has been created, the file will be sent to a specified Azure Blob Storage container.
	 * 
	 *  											     Max: 12/31/2021
	 * 	SqlServer Hostname -> Database -> dbo -> Transaction(Postdate, TransAmount,.....)
	 *                                                          |
	 * 															|--> dbo.Transaction_20201231.csv
	 * 															| 	  |--> dbo.Transaction_20201231.csv.gz --> Azure Blob Container
	 *                                                          |
	 * 															|--> dbo.Transaction_20201230.csv
	 *  														| 	  |--> dbo.Transaction_20201230.csv.gz --> Azure Blob Container
	 *                                                          |
	 * 	Snowflake Instance -> Database -> dbo -> Transaction(Postdate, TransAmount,.....)
	 *  											     Max: 12/29/2021
	 * 
	 * If the source table contains data but the destination table does not, it will print the full 
	 * table. If the table doesn't have a time dimension to compare, an optional flag can be specified to print
	 * the full table.  A full table will be split based on the max file size specified in its uncompressed
	 * version of itself
	 * 
	 *	dbo.Transaction_001.csv.gz
	 *	dbo.Transaction_002.csv.gz
	 *	dbo.Transaction_***.csv.gz
	 *
	 * Once an extract file has been created and compressed, it then gets sent off to an azure blob storage
	 *
	 * This method uses a thread pool to concurrently pull tables, with the max concurrency specified by
	 * MAX_CONCURRENCY_LEVEL (ie 4 threads at any give time)
	 * 
	 * TODO: suspend warehouse after complete
	 * 
	 * @param String filePath: Output file location (ie, "C:\\temp\\")
	 * @param long maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
	 * @param String srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param String srcDatabase: Source Database in which table resides (ie,"DatabaseName")
	 * @param String srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param String sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
	 * @param String sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
	 * @param String sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param String rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
	 * @param String colDelim: Output file column/field delimiter(ie, "," or "~" or "\t")
	 * @param String textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"" -> ")
	 * @param String escapeChar: POSIX control character (ie, "\\" -> \)
	 * @param ArrayList<String> timeDimensions:  A list of fields in which to look for to split table up along a
	 * 		  dimension of time (ie, new ArrayList<String>(Arrays.asList("POSTDATE", "PROCESSDATE", "DATE")))
	 * @param boolean strictPK: Looks for the value "PK" in column constraint in conjunction timeDimnensios arrayList if set true
	 * @param boolean optionalFull: Prints full file it time dimension can't be determined it set to true
	 * @param String azCredsLoc: Location of Azure credentials of an Azure Blob Container instance (ie, "C:\\azure_creds.json")
	 * @param String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
	 * @param String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
	 */
	static public void writeCsvFromSqlServerAllTablesDiff(String filePath,long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema,
			 String sfCredsLoc, String sfDatabase, String sfSchema,
			 String rowDelim, String colDelim, String textQualifier, String escapeChar,
			 ArrayList<String> timeDimensions, boolean strictPK, boolean optionalFull,
			 String azCredsLoc, String azBlobDir, String sfStage) {

		/*Get Sql Server Table information in a RowSet, connection opening and closing handled by called functions
		 * 	ssTable field set:
			TABLE_TYPE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME
		*/
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		RowSet ssTables = SqlServerDbScour.getSrcTables(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema);

		try {
			
			ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
			
			//iterate through tables until reach the end of ssTables RowSet
			while(ssTables.next()) {
				
				//get Sql Server schema name and table name
				final String schemaName = ssTables.getString("TABLE_SCHEMA");
				final String tableName = ssTables.getString("TABLE_NAME");
				
				/*get RowSet of columns for the current table
					ssCol field set:
					TABLE_TYPE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,CU.CONSTRAINT_NAME,
					DATA_TYPE,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,
					NUMERIC_PRECISION,NUMERIC_PRECISION_RADIX,NUMERIC_SCALE,DATETIME_PRECISION, 
					CHARACTER_SET_NAME,COLLATION_CATALOG,COLLATION_SCHEMA,COLLATION_NAME,DOMAIN_CATALOG, 
					DOMAIN_SCHEMA,DOMAIN_NAME,CHARACTER_SET_CATALOG,CHARACTER_SET_SCHEMA,  
					DATA_TYPE_CAT --CASE  'TEXT', 'NUMERIC', 'DATETIME', 'OTHER' 
				 */
				RowSet ssCols = SqlServerDbScour.getSrcInformationSchema(
						srcCnString.getCnString(),srcDatabase,schemaName,tableName);
				
				final String fileName = schemaName + "." + tableName;

				String valueLimiterCol = null;
				String dataTypeCat = null;
				
				//find time dimension of table to split
				while(ssCols.next() && valueLimiterCol == null) {
					String colName = ssCols.getString("COLUMN_NAME");
					
					//checks for value "PK" to indicate Primary Key constraint
					if(strictPK == true) {
						String constraint = ssCols.getString("CONSTRAINT_NAME");
						if(constraint != null && constraint.toUpperCase().contains("PK")) {
							
							valueLimiterCol = containsString(colName, timeDimensions, true);
							dataTypeCat = ssCols.getString("DATA_TYPE_CAT");
						}
					} else {
						
						valueLimiterCol = containsString(colName, timeDimensions, true);
						dataTypeCat = ssCols.getString("DATA_TYPE_CAT");
					}
					
				}
				
				//to pass function to ThreadPoolExecutor, variables must be "final"
				final String vlc = valueLimiterCol;
				final String dtc = dataTypeCat;
				
				//check if tables match
				//if destination table doesn't exit, create new table
				//if table columns do not match exactly, rename existing table with postdate/processdate in name and create new table
				//returns true if there is a destination table that matches the source table
				String maxFromTable = SnowflakeDbScour.checkTableAndCols(
													srcSqlHost,		//
													srcDatabase,	//
													srcSchema,		//
													tableName,		//
													sfCredsLoc,		//
													sfDatabase,		//
													sfSchema,		//
													tableName, 		//
													valueLimiterCol,//
													dataTypeCat, 	//
													true);			//
				
				//Write SQL to file for support purposes							
				SnowflakeDbScour.writeSqlCreateTable(filePath, srcSqlHost, srcDatabase, srcSchema, tableName, azCredsLoc, azBlobDir);
				
				//value limiter column found, 
				if(valueLimiterCol != null) {
					
					QLog.log(fileName + ": Diff file");
					
					exe.execute(()->  
						SnowflakeDbScour.writeCsvFromSqlServerTableDiff(
								filePath, 		//
								fileName,		//
								maxFileSize,	//max file size in bytes
								srcSqlHost,		//
								srcDatabase,	//
								srcSchema,		//
								tableName,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
								sfCredsLoc,		//
								sfDatabase,		//
								sfSchema,		//
								tableName,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
								rowDelim,		//
								colDelim,		//
								textQualifier,	//
								escapeChar,		//
								vlc, 			//
								dtc, 			//
								maxFromTable,	//
								azCredsLoc, 	//
								azBlobDir, 		//
								sfStage)		//
					);
					
				}
				else if (optionalFull == true) {
					
					QLog.log(fileName + ": Full file");
					
					exe.execute(()->  
						SnowflakeDbScour.writeCsvFromSqlServerTableFull(
								filePath, 		//
								fileName,		//
								maxFileSize,	//max file size in bytes
								srcSqlHost,		//
								srcDatabase,	//
								srcSchema,		//
								tableName,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
								sfCredsLoc,		//
								sfDatabase,		//
								sfSchema,		//
								tableName,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
								rowDelim,		//
								colDelim,		//
								textQualifier,	//
								escapeChar,		//
								azCredsLoc, 	//
								azBlobDir, 		//
								sfStage)		//
					);
					
				}
			}
			
			exe.shutdown();
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	}
	
	
	
	
	/***
	 * Establishes time range to create differential extracts for specific table
	 * 
	 *  											     Max: 12/31/2021
	 * 	SqlServer Hostname -> Database -> dbo -> Transaction(Postdate, TransAmount,.....)
	 *                                                          |
	 * 															|--> dbo.Transaction_20201231.csv
	 * 															| 	  |--> dbo.Transaction_20201231.csv.gz --> Azure Blob Container
	 *                                                          |
	 * 															|--> dbo.Transaction_20201230.csv
	 *  														| 	  |--> dbo.Transaction_20201230.csv.gz --> Azure Blob Container
	 *                                                          |
	 * 	Snowflake Instance -> Database -> dbo -> Transaction(Postdate, TransAmount,.....)
	 *  											     Max: 12/29/2021
	 * 
	 * If the source table contains data but the destination table does not, it will print the full 
	 * table. If the table doesn't have a time dimension to compare, an optional flag can be specified to print
	 * the full table.  A full table will be split based on the max file size specified in its uncompressed
	 * version of itself
	 * 
	 *	dbo.Transaction_001.csv.gz
	 *	dbo.Transaction_002.csv.gz
	 *	dbo.Transaction_***.csv.gz
	 *
	 * TODO: suspend warehouse after complete
	 *
	 * @param String filePath: Output file location. Include last slash (ie, "C:\\temp\\" which becomes "C:\temp\)
	 * @param String fileName: Output base file name.  Do not include file extension. (ie, "dbo.ACCOUNT")
	 * @param long maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
	 * @param String srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param String srcDatabse: Source Database in which table resides (ie,"DatabaseName")
	 * @param String srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param String srcTable: Source SQL Server table to create extract of (ie."ACCOUNT")
	 * @param String sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
	 * @param String sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
	 * @param String sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param String sfTable": Remote Snowflake table name to compare columns (ie,"ACCOUNT")
	 * @param String rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
	 * @param String colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
	 * @param String textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"")
	 * @param String escapeChar: POSIX control character (ie, "\\")
	 * @param String valueLimterCol: Column or Field Name used to limit dataset (ie, "ProcessDate" or "POSTDATE")
	 * @param String dataTypeCat: Value (ie, int: "20220203" or date: "2/3/2022" or date: "2022/02/03")
	 * @param String sfMaxFromTable: 
	 * @param String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
	 * @param String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
	 * @param String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
	 */
	static public void writeCsvFromSqlServerTableDiff(
			 String filePath, String fileName, long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
			 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
			 String rowDelim, String colDelim, String textQualifier, String escapeChar,
			 String valueLimiterCol, String dataTypeCat, String sfMaxFromTable,
			 String azCredsLoc, String azBlobDir, String sfStage) {
		
		QLog.log("writeCsvFromSqlServerTableDiff");
		QLog.log("filePath: " + filePath);
		QLog.log("fileName: " + fileName);
		QLog.log("maxFileSize: " + maxFileSize);
		QLog.log("srcSqlHost: " + srcSqlHost);
		QLog.log("srcDatabase: " + srcDatabase);
		QLog.log("srcSchema: " + srcSchema);
		QLog.log("srcTable: " + srcTable);
		QLog.log("sfCredsLoc: " + sfCredsLoc);
		QLog.log("sfDatabase: " + sfDatabase);
		QLog.log("sfSchema: " + sfSchema);
		QLog.log("sfTable: " + sfTable);
		QLog.log("rowDelim: " + rowDelim);
		QLog.log("colDelim: " + colDelim);
		QLog.log("textQualifier: " + textQualifier);
		QLog.log("escapeChar: " + escapeChar);
		QLog.log("valueLimiterCol: " + valueLimiterCol);
		QLog.log("dataTypeCat: " + dataTypeCat);
		QLog.log("sfMaxFromTable: " + sfMaxFromTable);
		QLog.log("azCredsLoc: " + azCredsLoc);
		QLog.log("azBlobDir: " + azBlobDir);
		
		if(sfMaxFromTable == null) sfMaxFromTable = sfTable;
		
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		ssMaxValue = getSsMaxValue(srcSqlHost,  srcDatabase, srcSchema, srcTable, valueLimiterCol);

		QLog.log(SqlServerDbScour.sqlMaxValue(srcDatabase, srcSchema, srcTable, valueLimiterCol) + ": " + ssMaxValue);
		
		//make sure time-dimension value was returned on SQL Server side, otherwise skip as there is no data to pull
		if (ssMaxValue != null) {
			
			//ping Snowflake source for most current data
			String sfMaxValue = null;
			sfMaxValue = getSfMaxValue(sfCredsLoc, sfDatabase, sfSchema, sfMaxFromTable, valueLimiterCol);
			
			QLog.log(SnowflakeDiscovery.sqlMaxValue(sfDatabase, sfSchema, sfMaxFromTable, valueLimiterCol) + ": " + sfMaxValue);
			
			//make sure some value was returned, otherwise pull full file
			if(sfMaxValue != null) {
				
				QLog.log(SqlServerDbScour.sqlGreaterThan(srcDatabase, srcSchema, srcTable, valueLimiterCol, sfMaxValue, dataTypeCat));
				
				SqlServerCnString srcCnString = new SqlServerCnString();
				srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
				
				//grab range of values from SQL server that is greater than what is available on Snowflake side
				RowSet segs = SqlServerDbScour.executeSqlRowSet(srcCnString.getCnString(),
						SqlServerDbScour.sqlGreaterThan(srcDatabase, srcSchema, srcTable, valueLimiterCol, sfMaxValue, dataTypeCat));
				try {
					while(segs.next()) {
						
						String valueLimit = segs.getString(valueLimiterCol);
						QLog.log(valueLimit);
						
						//generate differential dataset: generally for Fact tables
						SnowflakeDbScour.writeCsvFromSqlServerTableValueLimit(
								filePath, fileName, maxFileSize,
								srcSqlHost, srcDatabase, srcSchema, srcTable,
								sfCredsLoc, sfDatabase, sfSchema, sfTable,
								rowDelim, colDelim, textQualifier, escapeChar,
								valueLimiterCol, valueLimit, 
								azCredsLoc, azBlobDir, sfStage, false);
						
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					QLog.log("ETL Exception: " + e.toString());
					QLog.log(e);
				}
				
			}
			else {
				
				//generate full dataset: generally for Dim tables
				SnowflakeDbScour.writeCsvFromSqlServerTableFull(
						filePath, fileName,	maxFileSize,				//max file size in bytes
						srcSqlHost,srcDatabase,srcSchema,srcTable,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
						sfCredsLoc,sfDatabase,sfSchema,sfTable,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
						rowDelim,colDelim,textQualifier,escapeChar,
						azCredsLoc, azBlobDir, sfStage);	
				
			}	
		}
	}

    		
	/***
	 * Takes source table and creates a *.csv or *.csv.gz based on a source Sql Server host, database, schema,
	 * and table, compares to specified remote Snowfalke instance, compute, database, schema, and table to
	 * build column ordinal sensitive datasets with optionaly compressing the file via GZip.
	 *
	 * TODO: suspend warehouse after complete
	 * 
	 * @param filePath: Output file location. Include last slash (ie, "C:\\temp\\" which becomes "C:\temp\)
	 * @param fileName: Output file name (ie, "dbo.ACCOUNT")
	 * @param maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
	 * @param srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param srcDatabse: Source Database in which table resides (ie,"DatabaseName")
	 * @param srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param srcTable: Source SQL Server table to create extract of (ie."ACCOUNT")
	 * @param sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
	 * @param sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
	 * @param sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param sfTable": Remote Snowflake table name to compare columns (ie,"ACCOUNT")
	 * @param rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
	 * @param colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
	 * @param textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"")
	 * @param escapeChar: POSIX control character (ie, "\\")
	 * @param valueLimterFCol: Column or Field Name used to limit dataset (ie, "ProcessDate" or "POSTDATE")
	 * @param valueLimit: Value (ie, int: "20220203" or date: "2/3/2022" or date: "2022/02/03")
	 * @param String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
	 * @param String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
	 * @param String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
	 * @param boolean sfForceReload: Performs delete on full table if true
	 */
	static public void writeCsvFromSqlServerTableValueLimit(String filePath, String fileName, long maxFileSize,
							 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
							 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
							 String rowDelim, String colDelim, String textQualifier, String escapeChar,
							 String valueLimiterCol, String valueLimit,
							 String azCredsLoc, String azBlobDir, 
							 String sfStage, boolean sfForceReload) {
		
		QLog.log("writeCsvFromSqlServerTableValueLimit");
		QLog.log("filePath: " + filePath);
		QLog.log("fileName: " + fileName);
		QLog.log("maxFileSize: " + maxFileSize);
		QLog.log("srcSqlHost: " + srcSqlHost);
		QLog.log("srcDatabase: " + srcDatabase);
		QLog.log("srcSchema: " + srcSchema);
		QLog.log("srcTable: " + srcTable);
		QLog.log("sfCredsLoc: " + sfCredsLoc);
		QLog.log("sfDatabase: " + sfDatabase);
		QLog.log("sfSchema: " + sfSchema);
		QLog.log("sfTable: " + sfTable);
		QLog.log("rowDelim: " + rowDelim);
		QLog.log("colDelim: " + colDelim);
		QLog.log("textQualifier: " + textQualifier);
		QLog.log("escapeChar: " + escapeChar);
		QLog.log("valueLimiterCol: " + valueLimiterCol);
		QLog.log("valueLimit: " + valueLimit);
		QLog.log("azCredsLoc: " + azCredsLoc);
		QLog.log("azBlobDir: " + azBlobDir);
		QLog.log("sfStage: " + sfStage);
		QLog.log("sfForceReload: " + sfForceReload);
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		
		//Get Snowflake Schema RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeCnString.getConnection(sfCredsLoc);
		String sqlSfSchema = SnowflakeDiscovery.sqlDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
		RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);
		
        //generate list of column datatype categories
        TreeMap<String, String> datatypeCategories = setDataTypeCategories(ssSchemaRowSet);		
		
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
        //ArrayList<String> ssCols = toCols(ssSchemaRowSet);
        
        //generate select statement for data extract
        String sqlSsTable = SqlServerDiscovery.sqlSelect(srcDatabase, srcSchema, srcTable, cols);
        
        QLog.log(srcSchema + "." + srcTable + " Column Count: " + cols.size());
        
        //if force load is true, delete current table segment to be be reloaded
        if(sfForceReload == true) {
        	QLog.log("Deleting from " + SnowflakeDiscovery.asmSfObj(sfDatabase, sfSchema, sfTable) 
        							  + " @ " + valueLimiterCol +"=" +valueLimit);
        	sfDeleteFrom(sfCredsLoc, sfDatabase, sfSchema, sfTable, valueLimiterCol, valueLimit,
        			 datatypeCategories.get(valueLimiterCol.toUpperCase()));
        }
        
        //get and prep value limiter to pass into sql statement
        String sqlValueLimit = SqlServerDiscovery.sqlValuePrep(valueLimit,
        		datatypeCategories.get(valueLimiterCol.toUpperCase()));
        
        String sqlValueLimitCol = SqlServerDiscovery.sqlObjBracket(valueLimiterCol);
        if(sqlValueLimitCol != null && sqlValueLimitCol  != ""  &&
           sqlValueLimit != null && sqlValueLimit != "")
        	sqlSsTable = sqlSsTable + " WHERE " + sqlValueLimitCol + " = " +  sqlValueLimit;
                                                                                                                 
        QLog.log(sqlSsTable);                                                                                    
        ResultSet ssTable = SqlServerDbScour.executeSqlResultSet(srcCnString.getCnString(), sqlSsTable);
        
        //generate file system friendly name
        String outFileName = fileNamePrep(valueLimit,datatypeCategories.get(valueLimiterCol.toUpperCase()));
        String fullFileName = fileName + "_" + outFileName + ".csv";
        String fullFileNamePath = filePath + fullFileName;
        
        //instantiate file writing objects
		FileWriter writer = null;
		BufferedWriter buffer = null;
		try {
			writer = new FileWriter(fullFileNamePath);
			buffer = new BufferedWriter(writer);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			QLog.log("ETL Exception: " + e1.toString());
			QLog.log(e1);
		}       
		
		//if all went well with file write objects, begin writing to files
		if(buffer != null)  {
			
			long cCnt = 0;	//count calls
	        long aCnt = 0;	//count all records
	        long lCnt = 0;	//count lines for specific file
	        long fCnt = 0;	//
	        long fSize = 0;	//file size of specific file

	        StringBuilder values = new StringBuilder();
	        
			try {
				
				fCnt++;
				
				ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
				
				//start iterating table records and write to extract file
				while(ssTable.next() ) {
					aCnt++;cCnt++;lCnt++;
					
					//if more than one record, add comma separator 
					if(lCnt>1) values.append(rowDelim); 
					
					//iterate columns and grab column values to generate record string
					for(int i = 0; i < cols.size(); i++) {
						if(i > 0) values.append(colDelim);
						
						//grab record values and clean value for Snowflake consumption
						String csvValue = sfCsvValuePrep(
											   ssTable.getString(cols.get(i)),		//value
											   datatypeCategories.get(cols.get(i)),	//value datatype
											   textQualifier,escapeChar);			//add text qualifier and/or escape character
						
						//add to record string
						values.append(csvValue);
					}		
					
					//write to extract file
					buffer.write(values.toString());
					
					//show current row values at some rowcount interval
					if(aCnt%SHOW_ROW_RECORD_COUNT==0 || lCnt==1)
						QLog.log(fileName + " " + cCnt + ": " + values.toString());
					
					//keep track of growing file size 
					fSize = fSize + values.length();
					
					//split file out after size exceeds size limit
					if(fSize >= maxFileSize) {
						buffer.close();
						
						//using a thread pooler allows for the compression of a completed file to occur independently of the
						//SQL pull and can be a time saving if files turn out to be rather large.
						//ExecuteZipAndShip t = new ExecuteZipAndShip(filePath, fullFileName, fullFileName + ".gz", true, azCredsLoc);
						ExecuteZipAndShip t = new ExecuteZipAndShip(
								sfCredsLoc, 			//Snowflake Credentials location
								sfDatabase, 			//Snowflake Database
								sfSchema, 				//Snowflake Schema
								sfTable,				//Snowflake path
								filePath, 				//local file path of csv
								fullFileName, 			//file name of csv
								fullFileName + ".gz", 	//renamed zip file of csv
								true, 					//delete .csv after .csv.gz is complete
								azCredsLoc, 			//Azure credentials location
								azBlobDir,				//Azure blob directory to store zipped file
								sfStage, 				//Snowflake stage name to cnmsume from from Azure to Snowflake
								sfForceReload);			//Snowflake COPY force load directive (needed if file already loaded once)

						exe.submit(t);
						
						lCnt = 0; 	//reset line count counter
						fSize = 0;	//reset file size counter
						fCnt++;		//increment file count counter
						
						fullFileName = fileName + "_" + outFileName + "_" + String.format("%03d", fCnt) + ".csv";
						fullFileNamePath = filePath + fullFileName;
						
						writer = new FileWriter(fullFileNamePath);
						buffer = new BufferedWriter(writer);
						
					}
					
					values.setLength(0);
				}
						
				//close file
				buffer.close();
				
				//using a thread pooler allows for the compression of a completed file to occur independently of the
				//SQL pull and can be time saving if files turn out to be rather large.
				ExecuteZipAndShip t = new ExecuteZipAndShip(
						sfCredsLoc, 			//Snowflake Credentials location
						sfDatabase, 			//Snowflake Database
						sfSchema, 				//Snowflake Schema
						sfTable,				//Snowflake path
						filePath, 				//local file path of csv
						fullFileName, 			//file name of csv
						fullFileName + ".gz", 	//renamed zip file of csv
						true, 					//delete .csv after .csv.gz is complete
						azCredsLoc, 			//Azure credentials location
						azBlobDir,				//Azure blob directory to store zipped file
						sfStage, 				//Snowflake stage name to cnmsume from from Azure to Snowflake
						sfForceReload);			//Snowflake COPY force load directive (needed if file already loaded once)
				
				exe.submit(t);

				exe.shutdown();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
				QLog.log("ETL Exception: " + e.toString());
				QLog.log(e);
			}
			finally {
				//compiler complained for not having try/catch around buffer.close()
				//bad form to have in finally?
				try {
					buffer.close();
				} catch (IOException e) {
					QLog.log("ETL Exception: " + e.toString());
					QLog.log(e);
				} //silently fail
			}
		}
	}
	
	
	/***
	 * 
	 * TODO: suspend warehouse after complete
	 * 
	 * @param filePath: Output file location (ie, "C:\\temp\\")
	 * @param fileName: Output file name (ie, "dbo.ACCOUNT")
	 * @param maxFileSize: Max file size threshold (ie, 30000 is "30Kb" or 4000000000L is "4Gb") 
	 * @param srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param srcDatabse: Source Database in which table resides (ie,"DatabaseName")
	 * @param srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param srcTable: Source SQL Server table to create extract of (ie."ACCOUNT")
	 * @param sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
	 * @param sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
	 * @param sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param sfTable": Remote Snowflake table name to compare columns (ie,"ACCOUNT")
	 * @param rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
	 * @param colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
	 * @param textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"")
	 * @param escapeChar: POSIX control character (ie, "\\")
	 * @param String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
	 * @param String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
	 * @param String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
	 */
	static public void writeCsvFromSqlServerTableFull(String filePath, String fileName, long maxFileSize,
							 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
							 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
							 String rowDelim, String colDelim, String textQualifier, String escapeChar,
							 String azCredsLoc, String azBlobDir, String sfStage) {
		
		
		QLog.log("writeCsvFromSqlServerTableFull");
		QLog.log("filePath: " + filePath);
		QLog.log("fileName: " + fileName);
		QLog.log("maxFileSize: " + maxFileSize);
		QLog.log("srcSqlHost: " + srcSqlHost);
		QLog.log("srcDatabase: " + srcDatabase);
		QLog.log("srcSchema: " + srcSchema);
		QLog.log("srcTable: " + srcTable);
		QLog.log("sfCredsLoc: " + sfCredsLoc);
		QLog.log("sfDatabase: " + sfDatabase);
		QLog.log("sfSchema: " + sfSchema);
		QLog.log("sfTable: " + sfTable);
		QLog.log("rowDelim: " + rowDelim);
		QLog.log("colDelim: " + colDelim);
		QLog.log("textQualifier: " + textQualifier);
		QLog.log("escapeChar: " + escapeChar);
		QLog.log("azCredsLoc: " + azCredsLoc);
		QLog.log("azBlobDir: " + azBlobDir);
		QLog.log("sfStage: " + sfStage);
	
		
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		
		//Get Snowflake Schema RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeCnString.getConnection(sfCredsLoc);
		String sqlSfSchema = SnowflakeDiscovery.sqlDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
		RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);

        //generate list of column datatype categories
        TreeMap<String, String> datatypeCategories = setDataTypeCategories(ssSchemaRowSet);		
		
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
        
        //generate select statement for data extract
        String sqlSsTable = SqlServerDiscovery.sqlSelect(srcDatabase, srcSchema, srcTable, cols);
        
        QLog.log(srcSchema + "." + srcTable + " Column Count: " + cols.size());
		
		//Delete from Snowflake table because full table load twice could load duplicate records
		sfDeleteFrom(sfCredsLoc, sfDatabase, sfSchema, sfTable);
        
        //execute SELECT Table
        QLog.log(sqlSsTable);
        
        ResultSet ssTable = SqlServerDbScour.executeSqlResultSet(srcCnString.getCnString(), sqlSsTable);
        
        //establish file name
        long fCnt = 1;
        String fullFileName = fileName + "_"  + String.format("%03d", fCnt) + ".csv";
        String fullFileNamePath = filePath + fullFileName;
        
        //set up file writing objects
		FileWriter writer = null;
		BufferedWriter buffer = null;
		try {
			writer = new FileWriter(fullFileNamePath);
			buffer = new BufferedWriter(writer);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			QLog.log("ETL Exception: " + e1.toString());
			QLog.log(e1);
		}       
		
		//continue if buffer was successfully created
		if(buffer != null)  {
			
			long cCnt = 0;	//count calls
	        long aCnt = 0;	//count all records
	        long lCnt = 0;	//count lines for specific file
	        long fSize = 0;	//file size of specific file
	        StringBuilder values = new StringBuilder();
	        
			try {
				//spin up thread pool to for ExecuteZipAndShip
				ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
				
				while(ssTable.next() ) {
					aCnt++;cCnt++;lCnt++;
					
					//if more than one record, add comma separator 
					if(lCnt>1) values.append(rowDelim); 
					
					//iterate columns and grab column values
					for(int i = 0; i < cols.size(); i++) {
						if(i > 0) values.append(colDelim);
						
						//wrap and append values
						values.append(
								sfCsvValuePrep(ssTable.getString(cols.get(i)),
											   datatypeCategories.get(cols.get(i)),
											   textQualifier,escapeChar));
					}		
					
					buffer.write(values.toString());
					
					if(aCnt%SHOW_ROW_RECORD_COUNT==0 || lCnt == 1)
							QLog.log(fileName + " " + cCnt + ": " + values.toString());
					
					fSize = fSize + values.length();
					
					//if file size reaches specified max file size, close file and create new file
					if(fSize >= maxFileSize) {
						//close flie
						buffer.close();
						
						ExecuteZipAndShip t = new ExecuteZipAndShip(
								sfCredsLoc, 			//Snowflake Credentials location
								sfDatabase, 			//Snowflake Database
								sfSchema, 				//Snowflake Schema
								sfTable,				//Snowflake path
								filePath, 				//local file path of csv
								fullFileName, 			//file name of csv
								fullFileName + ".gz", 	//renamed zip file of csv
								true, 					//delete .csv after .csv.gz is complete
								azCredsLoc, 			//Azure credentials location
								azBlobDir,				//Azure blob directory to store zipped file
								sfStage, 				//Snowflake stage name to cnmsume from from Azure to Snowflake
								true);					//Snowflake COPY force load directive (needed if file already loaded once)
						
						exe.submit(t);
	
						lCnt = 0; 	//reset line count counter
						fSize = 0;	//reset file size counter
						fCnt++;		//increment file count counter
						
						//handles up to 999 files of the same name
				        fullFileName = fileName + "_"  + String.format("%03d", fCnt) + ".csv";
				        fullFileNamePath = filePath + fullFileName;

						writer = new FileWriter(fullFileNamePath);
						buffer = new BufferedWriter(writer);
					}
					
					values.setLength(0);
				}
				
				//close last file
				buffer.close();
				
				//Asynchronously compress (GZIP) and send to Azure Blob Container 
				ExecuteZipAndShip t = new ExecuteZipAndShip(
						sfCredsLoc, 			//Snowflake Credentials location
						sfDatabase, 			//Snowflake Database
						sfSchema, 				//Snowflake Schema
						sfTable,				//Snowflake path
						filePath, 				//local file path of csv
						fullFileName, 			//file name of csv
						fullFileName + ".gz", 	//renamed zip file of csv
						true, 					//delete .csv after .csv.gz is complete
						azCredsLoc, 			//Azure credentials location
						azBlobDir,				//Azure blob directory to store zipped file
						sfStage, 				//Snowflake stage name to consume from from Azure to Snowflake
						true);					//Snowflake COPY force load directive (needed if file already loaded once)
				
				exe.submit(t);
				exe.shutdown();
				
			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log("ETL Exception: " + e.toString());
				QLog.log(e);
			}
			finally {
				//compiler complained for not having try/catch around buffer.close()
				//bad form to have in finally?
				try {
					buffer.close();
				} catch (IOException e) {
					QLog.log("ETL Exception: " + e.toString());
					QLog.log(e);
				} //silently fail
			}
		}
	}
	
	
	/***
	 * 
	 * 
	 * @param String sfCredsLoc:
	 * @param String sfDatabase:
	 * @param String sfSchema:
	 * @param String sfTable:
	 * @param String valueLimiterCol:
	 * @param String valueLimit:
	 * @param String valueDatatypeCat:
	 */
	static void sfDeleteFrom(String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable, 
			String valueLimiterCol, String valueLimit, String valueDatatypeCat) {
		

		try {
			java.sql.Connection sfCn = null;
			sfCn = SnowflakeCnString.getConnection(sfCredsLoc, sfDatabase, sfSchema);
			Statement sfStatement = sfCn.createStatement();
						
			//String sft = sfSchema.toUpperCase() + "." + sfTable.toUpperCase();
			
			String sft = SnowflakeDiscovery.asmSfObj(sfDatabase, sfSchema, sfTable);
			
			String sql = "DELETE FROM " + sft;
			if(valueLimiterCol != null && valueLimit != null) {
				String sqlValueLimit = SqlServerDiscovery.sqlValuePrep(valueLimit, valueDatatypeCat);
				sql = sql + " WHERE " + valueLimiterCol + " = " + sqlValueLimit;
			}
			
			//QLog.log(sql);
		
			sfStatement.execute(sql);
			sfStatement = sfCn.createStatement();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
	}
	
	
	/***
	 * 
	 * 
	 * @param sfCredsLoc
	 * @param sfDatabase
	 * @param sfSchema
	 * @param sfTable
	 */
	static void sfDeleteFrom(String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable) {
		sfDeleteFrom(sfCredsLoc,sfDatabase,sfSchema,sfTable, null, null, null);
	}
	
	
	/***
	 * 
	 * @param String checkString:
	 * @param ArrayList<String> stringList:
	 * @return String
	 */
	static String containsString(String checkString, ArrayList<String> stringList) {
		String output = null;
		
		if(stringList.contains(checkString))
			output = checkString;
		return output;
	}
	
	
	/***
	 * 
	 * 
	 * @param String checkString:
	 * @param ArrayList<String> stringList:
	 * @return
	 */
	static String containsString(String checkString, ArrayList<String> stringList, boolean ignoreCase) {
		String output = null;
		
		if(ignoreCase == false)
			output = containsString(checkString, stringList);
		else {
			for(int i = 0; i < stringList.size(); i++) {
				if(stringList.get(i).toString().equalsIgnoreCase(checkString) == true) {
					output = checkString;
					i = stringList.size();  //short circuit loop if found
				}
			}
		}
		return output;
	}
	
	
	/***
	 * 
	 * 
	 * @param String sfCredsLoc:
	 * @param String sfDatabase:
	 * @param String sfSchema:
	 * @param String sfTable:
	 * @param String valueLimiterCol:
	 * @return String
	 */
	static String getSfMaxValue(String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable, String valueLimiterCol) {
		String output = null;
		
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeCnString.getConnection(sfCredsLoc);
		String sqlSfMaxValue = SnowflakeDiscovery.sqlMaxValue(sfDatabase, sfSchema, sfTable, valueLimiterCol);
		RowSet rsSfmax = executeRowSetSnowflakeCommand(sqlSfMaxValue, sfCn);
		
		if(rsSfmax != null) {
			try {
				
				rsSfmax.next();
				output = rsSfmax.getString("MaxValue");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log("ETL Exception: " + e.toString());
				QLog.log(e);
			}
		}
		return output;
	}
	
	

	
	/***
	 * Returns the maximum value from a particular DATABASE.SCHEMA.TABLE.COLUMN
	 * 
	 * @param String srcSqlHost: 
	 * @param String srcDatabase:
	 * @param String srcSchema:
	 * @param String srcTable:
	 * @param String valueLimiterCol:
	 * @return String
	 */
	public static String getSsMaxValue(String srcSqlHost, String srcDatabase, String srcSchema, String srcTable, String valueLimiterCol) {
		String output = null;
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		RowSet rsSsMax = SqlServerDbScour.executeSqlRowSet(srcCnString.getCnString(), 
				SqlServerDbScour.sqlMaxValue(srcDatabase, srcSchema, srcTable, valueLimiterCol));
		try {
			rsSsMax.next();
			output = rsSsMax.getString("MaxValue");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	
		return output;
	}

	
	/***
	 *  
	 *
	 * @param String srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param String srcDatabase: Source Database in which table resides (ie,"DatabaseName")
	 * @param String srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param String srcTable: Source Table to compare against
	 * @param String sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C:\\snowflake_creds.json")
	 * @param String sfDatabase: Remote Snowflake database name (ie, "DatabaseName")
	 * @param String sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param String sfTable: Destination table to compare to
	 * @param boolean renameAndCreate:
	 * @return String: return new file name as evidence of success
	 */
	public static String checkTableAndCols(
			 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
			 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
			 String valueLimiterCol, String dataTypeCat,
			 boolean renameAndCreate) {	
		
		String output = null;
		String ntn = null;
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		//String sfObj = sfDatabase + "." + sfSchema + "." + sfTable;
		String sfObj = SnowflakeDiscovery.asmSfObj(sfDatabase,sfSchema,sfTable);
		
		QLog.log("Performing check table on: " + sfObj);
		
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		ArrayList<String> ssCols = toCols(ssSchemaRowSet);
		
		//we have a source table
		if(ssSchemaRowSet != null && ssCols.size() != 0) {
			
			//Get Snowflake Schema RowSet
			java.sql.Connection sfCn = null;
			sfCn = SnowflakeCnString.getConnection(sfCredsLoc);
			String sqlSfSchema = SnowflakeDiscovery.sqlDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
			
			RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);
			ArrayList<String> sfCols = toCols(sfSchemaRowSet);
			
			//we have a destination table
			if(sfSchemaRowSet != null && sfCols.size() != 0) {
				
				QLog.log("ssCols: " + ssCols.size());			
				QLog.log("sfCols: " + sfCols.size());
				//check row counts first
				
				if(ssCols.size() == sfCols.size()) {
					ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
					
					//if the rowsetColcompare returns same size of ssCols and sfCols, they compare
					if(cols.size() == sfCols.size()) {
						QLog.log("They match: " + sfObj);
						output = SnowflakeDiscovery.asmSfObj(null, null, sfTable);
					}
					else {
						QLog.log("Same number of columns, different data types: " + sfObj);						
						if(renameAndCreate == true) {
							
							//renames table, and returns string of new table name if successful
							ntn = renameTable_Processdated(sfCredsLoc,sfDatabase,sfSchema,sfTable,valueLimiterCol,dataTypeCat);
							if(ntn != null && ntn != "") {
								
								//create/copy table from sql server source.
								if(createTableFromSs(sfCredsLoc,srcSqlHost,srcDatabase,srcSchema,srcTable)) {
									output = ntn;  //return new file name as evidence of success
								}								
							}
						}
					}
				}
				else {
					QLog.log("Different number of columns: " + sfObj);
					
					if(renameAndCreate == true) {
						
						//renames table, and returns string of new table name if successful
						ntn = renameTable_Processdated(sfCredsLoc,sfDatabase,sfSchema,sfTable,valueLimiterCol,dataTypeCat);
						if(ntn != null && ntn != "") {
							
							//create/copy table from sql server source.
							if(createTableFromSs(sfCredsLoc,srcSqlHost,srcDatabase,srcSchema,srcTable)) {
								output = ntn;//return new file name as evidence of success
							}
						}
					}
				}	
			}
			else { 
				QLog.log("Table doesn't exist: "  + sfObj);
			
				if(renameAndCreate == true){
					//create/copy table from sql server source.
					if(createTableFromSs(sfCredsLoc,srcSqlHost,srcDatabase,srcSchema,srcTable)) {
						output = SnowflakeDiscovery.asmSfObj(null, null, sfTable);
					}
				}
			}
		}
		
		return output;
	}
	
	
	/***
	 * 
	 * 
	 * @param String sfCredsLoc:
	 * @param String sfDatabase:
	 * @param String sfSchema:
	 * @param String sfTable:
	 * @param String valueLimiterCol:
	 * @param String dataTypeCat:
	 * @return String 
	 */
	public static String renameTable_Processdated(String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
			String valueLimiterCol, String dataTypeCat) {
		String output = null;
		
		String processdate = null; 
		
		//get max differential
		if(valueLimiterCol != null && dataTypeCat != null) {
			//try to get max date differential first
			String sfMaxValue = getSfMaxValue(sfCredsLoc, sfDatabase, sfSchema, sfTable, valueLimiterCol);
			processdate = fileNamePrep(sfMaxValue, dataTypeCat);
		}
			
		//if max differential not available
		if(processdate == null || processdate == "") {
			java.util.Date date = new java.util.Date();
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
			processdate = df.format(date);
		}
		
		String sql = SnowflakeDiscovery.sqlRenameTable(sfDatabase, sfSchema, sfTable, sfTable + "_" + processdate);
		QLog.log(sql);
		
		try {
			
			java.sql.Connection sfCn = SnowflakeCnString.getConnection(sfCredsLoc, sfDatabase);
			Statement statement = sfCn.createStatement();
			statement.executeUpdate(sql);
			statement.close();
			sfCn.close();
			
			output = SnowflakeDiscovery.asmSfObj(null, null, sfTable + "_" + processdate);		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
		return output;
		
	}
	
	/***
	 * Creates a table in Snowflake by referencing a SQL Server Table
	 * 
	 * @param String sfCredsLoc:
	 * @param String srcSqlHost:
	 * @param String srcDatabase:
	 * @param String srcSchema:
	 * @param String srcTable:
	 * @return boolean: Returns true if no errors were encountered in creating the table
	 */
	public static boolean createTableFromSs(String sfCredsLoc, 
			String srcSqlHost, String srcDatabase, String srcSchema, String srcTable) {
		boolean output = false;
		
		//generate CREATE TABLE SQL statement
		String sql = sqlCreateTableFromSs(srcSqlHost,srcDatabase,srcSchema,srcTable);
		QLog.log(sql);
		
		try {
			
			java.sql.Connection sfCn = SnowflakeCnString.getConnection(sfCredsLoc, srcDatabase);
			Statement statement = sfCn.createStatement();
			statement.executeUpdate(sql);
			statement.close();
			sfCn.close();
			
			output = true;			
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
		return output;
	}
	
	
	/***
	 * Writes CREATE TABLE statements to file, and optionally send to Azure Blob Container.
	 * 
	 * @param String extractDir:
	 * @param String srcSqlHost:
	 * @param String srcDatabase:
	 * @param String srcSchema:
	 * @param String srcTable:
	 * @param String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C:\\azure_creds.json")
	 * @param String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
	 */
	public static void writeSqlCreateTable(String extractDir, 
			String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
			String azCredsLoc, String azBlobDir) {
		
		String sqlDir = extractDir + "sql\\";
		new File(sqlDir).mkdirs();
		String sql = sqlCreateTableFromSs(srcSqlHost,srcDatabase,srcSchema,srcTable);
		String fileName = srcSchema + "." + srcTable + ".sql";
		String fullFileNamePath = sqlDir + fileName;
		
        //instantiate file writing objects
		FileWriter writer = null;
		BufferedWriter buffer = null;
		try {
			writer = new FileWriter(fullFileNamePath);
			buffer = new BufferedWriter(writer);
			buffer.write(sql);
			buffer.close();
			
			//send to azure blob storage
			if(azCredsLoc != null) {
				AzureBlob.putBlobFile(azCredsLoc, sqlDir, fileName, azBlobDir + "/sql");
			}
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			QLog.log("ETL Exception: " + e1.toString());
			QLog.log(e1);
		}  
	}
	
	
	/***
	 * 
	 * 00_CREATE.sql
	 * 01_DELETE.sql
	 * 02_COPYINTO.sql
	 * 
	 * @param String extractDir:
	 * @param String fileName:
	 * @param String sql:
	 * @param String azCredsLoc:
	 * @param String azBlobDir:
	 */
	public static void writeSql(String extractDir, String fileName, String sql,
			String azCredsLoc, String azBlobDir) {
		
		String sqlDir = extractDir + "sql\\";
		new File(sqlDir).mkdirs();

		fileName = fileName + ".sql";
		String fullFileNamePath = sqlDir + fileName;
		
        //instantiate file writing objects
		FileWriter writer = null;
		BufferedWriter buffer = null;
		try {
			writer = new FileWriter(fullFileNamePath);
			buffer = new BufferedWriter(writer);
			buffer.write(sql);
			buffer.close();
			
			//send
			if(azCredsLoc != null) {
				AzureBlob.putBlobFile(azCredsLoc, sqlDir, fileName, azBlobDir + "\\sql");
			}
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			QLog.log("ETL Exception: " + e1.toString());
			QLog.log(e1);
		}  
	}
	
	
	/***
	 * Copy table via series of API calls to Snowflake
	 * 
	 * @param String srcSqlHost:
	 * @param String srcDatabse:
	 * @param String srcSchema:
	 * @param String srcTable: 
	 * @param String sfCredsLoc:
	 * @param String sfDatabase:
	 * @param String sfSchema:
	 * @param String sfTable:
	 */
	static public void copyApiTableData(String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
			String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable) {
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabse);
		
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		
		
		//Get Snowflake Schema RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeCnString.getConnection(sfCredsLoc);
		String sqlSfSchema = SnowflakeDiscovery.sqlDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
		RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);
		
		
        //generate list of column datatype categories
        TreeMap<String, String> datatypeCategories = setDataTypeCategories(ssSchemaRowSet);		
		
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
        String sqlSsTable = SqlServerDiscovery.sqlSelect(srcDatabse, srcSchema, srcTable, cols);
        
        QLog.log(sqlSsTable);
        
        ResultSet ssTable = SqlServerDbScour.executeSqlResultSet(srcCnString.getCnString(), sqlSsTable);

        try {
        	sfCn = SnowflakeCnString.getConnection(sfCredsLoc,sfDatabase);
			copyApiTableDataIterate(cols, ssTable, sfCn, sfSchema, sfTable, datatypeCategories);
			sfCn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
        

	}
	
	
	/***
	 * 
	 * 
	 * @param ArrayList<String> colSet:
	 * @param String srcTableResultSet:
	 * @param String sfConnection:
	 * @param String sfSchema:
	 * @param String sfTable:
	 * @param TreeMap<String, String> datatypeCategories:
	 * @throws SQLException
	 */
	static void copyApiTableDataIterate(ArrayList<String> colSet, ResultSet srcTableResultSet, 
			java.sql.Connection sfConnection, String sfSchema, String sfTable, 
			TreeMap<String, String> datatypeCategories) throws SQLException {

		long cCnt = 0;	//count calls
        long aCnt = 0;	//count all records
        long pCnt = 0;	//count packed records
        
        String sqlInsertInto = SnowflakeDiscovery.sqlInsertInto(sfSchema.toUpperCase(), sfTable.toUpperCase(), colSet);
    	
        StringBuilder values = new StringBuilder();
    	StringBuilder sql = new StringBuilder();
    	
    	//Instantiate Thread pool
    	ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
    	
		while(srcTableResultSet.next() ) {
			aCnt++;pCnt++;
			
			//if more than one record, add comma separator 
        	if(pCnt>1) values.append(","); 
        	
        	values.append("(");
        	
        	//iterate columns and grab column values
        	for(int i = 0; i < colSet.size(); i++) {
        		if(i > 0) values.append(",");
        		
        		//wrap and append values
        		values.append(SnowflakeDiscovery.sqlValuePrep(srcTableResultSet.getString(colSet.get(i)), 
        				datatypeCategories.get(colSet.get(i))));
        	}		
        	values.append(")");
			
        	if(sqlInsertInto.length() + values.length() > API_PACK_SIZE) {
        		cCnt++;
        		sql.append(sqlInsertInto + " VALUES " + values.toString());
        		
        		QLog.log("cCnt: " + cCnt+ ", pCnt: " + pCnt + ", len: " + sql.length() + ", aCnt: " + aCnt);
        		
        		//queue in thread pool
        		ExecuteUpdateSnowflakeCommand t = 
        				new ExecuteUpdateSnowflakeCommand(sql.toString(), sfConnection, cCnt);
				exe.submit(t);	

				QLog.log(exe.getTaskCount() + " tasks!");
        		
				//reset values
        		values.setLength(0);
        		pCnt = 0;
        		sql.setLength(0);
        	}
			
		}
		
		sql.append(sqlInsertInto + " VALUES " + values.toString());
		ExecuteUpdateSnowflakeCommand t = new ExecuteUpdateSnowflakeCommand(sql.toString(), sfConnection, cCnt);
		exe.submit(t);	
		
		exe.shutdown();
	}
	
	
	/***
	 * 
	 * @param String sqlCommand:
	 * @param java.sql.Connection sfConnection:
	 * @return
	 */
	public static RowSet executeRowSetSnowflakeCommand(String sqlCommand, java.sql.Connection sfConnection) {
		Statement stmnt = null;
		CachedRowSet output = null;
		try {
			if(sfConnection != null &&
			   sfConnection.isClosed() != true) {
				stmnt = sfConnection.createStatement();
			    ResultSet sfResultSet = stmnt.executeQuery(sqlCommand);
			    		
		        RowSetFactory rsf = RowSetProvider.newFactory();
		        output = rsf.createCachedRowSet();
		        output.populate(sfResultSet);
		        stmnt.close();
		        sfConnection.close();
		        
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
			QLog.log("SQL Error: " + sqlCommand);
		}
		finally {
			try {
				stmnt.close();
			} catch (SQLException e) {
				QLog.log("ETL Exception: " + e.toString());
				QLog.log(e);
			}
		}
		
		return output;
	}
	
	
	/***
	 * Builds a tree map of Column names to Data Type Category f
	 * Key pair is set to upper case
	 * 
	 * @param RowSet schema: a rowset that contains columns "COLUMN_NAME" and "DATA_TYPE_CAT"
	 * 			produced by SnowflakeDiscovery.sqlDatabaseAllInformationShema(database)
	 * @return TreeMap<String, String>:
	 */
	public static TreeMap<String, String> setDataTypeCategories(RowSet schema) {
        TreeMap<String, String> output;
        output = new TreeMap<String, String>();
        try {
			schema.beforeFirst();
			
	        while(schema.next()) {
	        	//QLog.log("COLUMN_NAME: " + schema.getString("COLUMN_NAME").toUpperCase() +
	        	//	  ", DATA_TYPE_CAT: " + schema.getString("DATA_TYPE_CAT"));
	        	
	        	output.put(schema.getString("COLUMN_NAME").toUpperCase(), 
	        			   schema.getString("DATA_TYPE_CAT"));
	        }			
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
        return output;
	}
	

	/***
	 * A method that copies data directly from SQL server to Snowflake for a matching 
	 * DATABASE.SCHEMA.TABLE.  
	 * 
	 * *CAUTION*
	 * Uses Snowflake Webservices credits 
	 * Currently not being used as a production item, but leaving available as experimental
	 * 
	 * snowflakeCreds \
	 *                 --- database, schema, table -> match fields
	 * srcSQLHost     /
	 * 
	 * @param String snowflakeCreds:
	 * @param String srcSqlHost:
	 * @param String database:
	 * @param String schema:
	 * @param String table:
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void copyApiTableData(String snowflakeCreds, String srcSqlHost, 
			String database, String schema, String table) throws SQLException, IOException {

		//set SQL server connection
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , database);

		//get tables for given database
		RowSet ssCols = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), schema, table);
		
		//Get Snowflake RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeCnString.getConnection(snowflakeCreds);
		Statement sfStatement = sfCn.createStatement();
		
		/*
		 * TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE,
		 * DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX,
		 * NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_TYPE, INTERVAL_PRECISION, CHARACTER_SET_CATALOG,
		 * CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME,
		 * DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, SCOPE_CATALOG, SCOPE_SCHEMA,
		 * SCOPE_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER, IS_SELF_REFERENCING, IS_IDENTITY, IDENTITY_GENERATION,
		 * IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE, COMMENT
		 */
	    ResultSet sfColsRs = sfStatement.executeQuery(
	    		SnowflakeDiscovery.sqlDatabaseTableInformationShema(database,schema,table));
		
        RowSetFactory rsf = RowSetProvider.newFactory();
        CachedRowSet sfCols = rsf.createCachedRowSet();
        sfCols.populate(sfColsRs);
        sfStatement.close();
        
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssCols,sfCols);
        
        //load up a treemap to quickly resolve datatypes
        TreeMap<String, String> colDatatypeCat = setDataTypeCategories(ssCols);

        sfCols.close();

        QLog.log(colDatatypeCat.size() + "");
        
        Connection srcCn = DriverManager.getConnection(srcCnString.getCnString()); 
        Statement ssStatment = srcCn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        ResultSet resColumns = ssStatment.executeQuery(
        		SqlServerDiscovery.sqlSelect(database,schema,table, cols));
        
        String sqlInsertInto = SnowflakeDiscovery.sqlInsertInto(schema.toUpperCase(), table.toUpperCase(), cols);
        
        //spin up snowflake connection with database context
		java.sql.Connection cn = null;
		cn = SnowflakeCnString.getConnection(snowflakeCreds,database.toUpperCase());
		Statement statement = cn.createStatement();
        
		long cCnt = 0;	//count calls
        long aCnt = 0;	//count all records
        long pCnt = 0;	//count packed records
        StringBuilder values = new StringBuilder();
        StringBuilder sql = new StringBuilder();
        
        //start grabbing values
        while(resColumns.next()) {
        	aCnt++;pCnt++;
        	
        	if(pCnt>1) values.append(",");        	
        	values.append("(");
        	for(int i = 0; i < cols.size(); i++) {
        		if(i > 0) values.append(",");
        		values.append(SnowflakeDiscovery.sqlValuePrep(resColumns.getString(cols.get(i)), colDatatypeCat.get(cols.get(i))));
        	}		
        	values.append(")");

        	if(sqlInsertInto.length() + values.length() > API_PACK_SIZE) {
        		cCnt++;
        		
        		sql.append(sqlInsertInto + " VALUES  " + values.toString());

        		QLog.log("cCnt: " + cCnt+ ", pCnt: " + pCnt + ", len: " + sql.length() + ", aCnt: " + aCnt);
        		
        		//Synchronous Call
        		//statement.executeUpdate(sql.toString());
        		
        		//Asynchronous Call
        		statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql.toString());
        		
        		values.setLength(0);
        		pCnt = 0;
        		sql.setLength(0);
        	}
        	
        }
        values.append(")");
        sql.append(sqlInsertInto + " VALUES  " + values.toString());
        
        
		//Synchronous Call: waits for round trip before proceeding (Safer and slower; will bomb on failure)
		//statement.executeUpdate(sql.toString());
		
		//Asynchronous Call: doesn't wait for round trip (faster with but offers not guarantees on proper execution)
		statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql.toString());
        
        statement.close();
        cn.close();
	}
	
	
	/***
	 * Prepares a given value for Snowflake consumption via a CSV based on the supplied datatype.
	 * Converts unicode characters to space characters (" ") as Snwoflake complains about trying to 
	 * consume unicode characters via CSV.  Ensure ASCII character range
	 * 
	 * @param String value:
	 * @param String datatypeCat: Looks for "TEXT", "DATETIME", "NUMERIC", or "OTHER"
	 * @param String textQualifier: Character used to wrap around text values, "ie, double quote ", single quote ', etc)
	 * @param String escapeChar: Escape character to be used if given textQualifier is found in TEXT value (ie, backslash \)
	 * @return String
	 */
	public static String sfCsvValuePrep(
			String value, String datatypeCat, String textQualifier, String escapeChar) {
		
		String output = "";
		
		//DATA_TYPE_CAT: TEXT, NUMERIC, DATETIME, OTHER
		if(value == null) output = "";
		else if (datatypeCat.equals("TEXT")) {
			
			//Snowflake likes ASCII only.  (it says UTF-8, but its complaining for characters between 127-255
			for(int i = 0; i < value.length(); i++) {
				if((int)value.charAt(i) > 127) {
					/* Common offenders
					 * 209 -> N
					 * 8211 -> -
					 * 8217 -> '
					 * 8220 -> "
					 * 8221 -> "
					 */
					QLog.log("Found an offending character: " + (int)value.charAt(i) + ", " +  value) ;
					
					value = value.replaceAll(String.valueOf(value.charAt(i)), " ");

				}
			}
			
			//print literal value as opposed to being seen as escape character
			if(escapeChar != null && escapeChar != "")
				value = value.replace(escapeChar, escapeChar + escapeChar);
			
			//if text qualifier is present, escape the text qualifier
			// ie, " -> \"
			if(textQualifier != null && textQualifier != "") {
				if(escapeChar != null && escapeChar != "")
					value = value.replace(textQualifier,escapeChar + textQualifier);
				else 
					value = value.replace(textQualifier, textQualifier + textQualifier);
				output = textQualifier + value + textQualifier;
			}
			else output = value;
			
		}
		else if (datatypeCat.equals("DATETIME")) output = value;
		else if (datatypeCat.equals("NUMERIC")) output = value;
		else if (datatypeCat.equals("OTHER")) output = "";
		
		return output; 
	}
	

	/***
	 * Converts a Postdate String (ie,"12/31/2021") or Processdate (ie,"20221231") or String with special
	 * characters to a file system friendly value to help with file naming.
	 * 
	 * @param String value: File name to prep
	 * @param String datatypeCat: Looks for "TEXT", "DATETIME", "NUMERIC", or "OTHER"
	 * @return String 
	 */
	public static String fileNamePrep(String value, String datatypeCat) {
		
		String output = "";
		
		//DATA_TYPE_CAT: TEXT, NUMERIC, DATETIME, OTHER
		if(value == null) output = "";
		else if (datatypeCat.equalsIgnoreCase("TEXT")) {
			output = value;
			output = output.replace("#", "");output = output.replace(">", "");output = output.replace("<", "");
			output = output.replace(">", "");output = output.replace("\\", "");output = output.replace("/", "");
			output = output.replace("*", "");output = output.replace("!", "");output = output.replace("{", "");
			output = output.replace("}", "");output = output.replace("|", "");output = output.replace(":", "");
			output = output.replace("@", "");output = output.replace("+", "");output = output.replace("&", "");
			output = output.replace("$", "");output = output.replace("-", "");output = output.replace("`", "");
		}
		else if (datatypeCat.equalsIgnoreCase("DATETIME")) {
			java.util.Date date = tryDateParse(value);
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
			output = df.format(date);
		}
		else if (datatypeCat.equalsIgnoreCase("NUMERIC")) output = value;
		else if (datatypeCat.equalsIgnoreCase("OTHER")) output = "";
		
		return output; 
	}
	
	//used for tryDateParse function as date formats to look for
	static List<String> dateFormatStrings = 
			Arrays.asList("yyyy-MM-dd", "yyyyMMdd","MM/dd/yyyy","MM-dd-yyyy","MM/dd/yy","MM-dd-yy");
	
	/***
	 * Tries to parse a String to Date
	 * 
	 * @param dateString
	 * @return java.util.Date
	 */
	static java.util.Date tryDateParse(String dateString) {
	    for (String formatString : dateFormatStrings) {
	        try {
	            return new SimpleDateFormat(formatString).parse(dateString);
	        }
	        catch (java.text.ParseException e) {
				QLog.log("ETL Exception: " + e.toString());
				QLog.log(e);
	        }
	    }
	    return null;
	}

	
	/***
	 * Returns a unique set of column names for a RowSet that contains "COLUMN_NAME" in caps
	 * 
	 * @param RowSet rowSet: A rowset that contains the column name COLUMN_NAME 
	 * @return ArrayList<String>: A list of column names
	 */
	public static ArrayList<String> toCols(RowSet rowSet) {
		ArrayList<String> output = new ArrayList<String>();
		
		try {
			rowSet.beforeFirst();

			while(rowSet.next()) {
				//make sure column doesn't already exist before adding to list
				if(output.contains(rowSet.getString("COLUMN_NAME").toUpperCase()) == false)
					output.add(rowSet.getString("COLUMN_NAME").toUpperCase());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
		return output;
	}

	
	/***
	 * Compares two RowSets and returns a matching set of column names from both rowsets
	 * 
	 * @param RowSet ssRowSet: Sql Server RowSet
	 * @param RowSet sfRowSet: SnowFlake RowSet
	 * @return ArrayList<String> rowsetColCompare: An array list of matched columns
	 * @throws SQLException
	 */
	public static ArrayList<String> rowsetColCompare(RowSet ssRowSet, RowSet sfRowSet) {
		ArrayList<String> output = new ArrayList<String>();
		String ssCol = "";
		String sfCol = "";
		String ssColType = "";
		String sfColType = "";
		
		try {
			sfRowSet.beforeFirst();
			ssRowSet.beforeFirst();
			
			while(sfRowSet.next()) {
				//QLog.log("COLUMN_NAME: " + sfRowSet.getString("COLUMN_NAME").toUpperCase());
				
				sfCol = sfRowSet.getString("COLUMN_NAME").toUpperCase();
				sfColType = sfRowSet.getString("DATA_TYPE_CAT").toUpperCase();
				
				ssRowSet.beforeFirst();
				while(ssRowSet.next() && sfCol.compareToIgnoreCase(ssCol) != 0) {
					ssCol = ssRowSet.getString("COLUMN_NAME").toUpperCase();
					ssColType = ssRowSet.getString("DATA_TYPE_CAT").toUpperCase();
					
					//make sure both column name and data type category match before adding
					if(sfCol.compareToIgnoreCase(ssCol) == 0 && 
					   sfColType.compareToIgnoreCase(ssColType)	 == 0) {
						output.add(sfCol);
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
		return output;
	}
	
	
	/***
	 * Creates a copy of database, schema, and tables from a SQL Server host to a Snowflake instance
	 * 
	 * @param Source SQL Server host name (String)
	 * @param Source SQL Server database of the previously specified host name (String)
	 */
	public static void copyFullSqlServerDatabase(String sfCredsLoc, String srcHost, String srcDatabase) {
		java.sql.Connection sfCn = null;
		
		try {	
			//Create Snowflake Database
			sfCn = SnowflakeCnString.getConnection(sfCredsLoc);
			Statement statement = sfCn.createStatement();
			statement.executeUpdate("CREATE DATABASE " + srcDatabase);
			statement.close();
			sfCn.close();
			
			sfCn = SnowflakeCnString.getConnection(sfCredsLoc, srcDatabase);
			
			QLog.log("Create Database...");
			copySqlServerDatabase(sfCn, srcHost, srcDatabase);

			QLog.log("Create Shemas...");
			copySqlServerAllDatabaseSchemas(sfCn, srcHost, srcDatabase);
			
			QLog.log("Create Tables...");
			copySqlServerAllDatabaseTables(sfCn, srcHost, srcDatabase);
			
			sfCn.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	}
	
	
	/***
	 * 
	 * @param java.sql.Connection snowflakeCn:
	 * @param String srcHost: Source SQL Server host name (String)
	 * @param String srcDatabase: Source SQL Server database of the previously specified host name (String)
	 * @throws SQLException
	 */
	public static void copySqlServerDatabase(java.sql.Connection snowflakeCn, 
			String srcHost, String srcDatabase) throws SQLException {
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcHost, null , srcDatabase);
		//TODO: not finished!
	}
	

	/***
	 * Copies schemas from a source SQL Server host to a Snowflake instance 
	 * 
	 * @param java.sql.Connection snowflakeCn: An established Snowflake connection to execute Snowflake SQL statements (java.sql.Connection)
	 * @param String srcHost: Source SQL Server host name (String)
	 * @param String srcDatabase: Source SQL Server database of the previously specified host name (String)
	 * @throws SQLException
	 */
	public static void copySqlServerAllDatabaseSchemas(java.sql.Connection snowflakeCn, 
			String srcHost, String srcDatabase) throws SQLException {
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcHost, null , srcDatabase);
		
		//get tables for given database
		RowSet schemas = SqlServerDbScour.getSrcSchemas(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName());	
		
		try {
			//object the excutes to snowflake
			Statement statement = snowflakeCn.createStatement();
			
			while(schemas.next()) {
				StringBuilder createStatement = new StringBuilder();
				
				createStatement.append("CREATE SCHEMA " + schemas.getString("TABLE_SCHEMA"));
				
				QLog.log(createStatement.toString());
				statement.executeUpdate(createStatement.toString());
			}
			
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	}
	
	
	/***
	 * Copies schemas from a source SQL Server host to a Snowflake instance by performing a syntax conversion
	 * Creates a series of create statements
	 * Currently supported: Column Names, Data Types, Default Values, Nullable, and Primary Keys 
	 * 
	 * TODO: Method should be refactored moving from test case to 
	 * 
	 * @param java.sql.Connection snowflakeCn: An active Snowflake connection
	 * @param String srcHost: Source SQL Server host name
	 * @param String srcDatabase: Source SQL Server database of the previously specified host name
	 */
	public static void copySqlServerAllDatabaseTables(java.sql.Connection snowflakeCn, 
			String srcHost, String srcDatabase) throws SQLException {
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcHost, null , srcDatabase);
	
		//get tables for given database, schema parameter not specified, so gets all tables for all schemas
		RowSet tables = SqlServerDbScour.getSrcTables(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(),
				null);  	//schema name
		
		try {
			//object the excutes to snowflake
			Statement statement = snowflakeCn.createStatement();
			
			//loop through tables
			while(tables.next()) {
				
				String createTable = sqlCreateTableFromSs(
						srcCnString.getHostName(),
						srcCnString.getDatabaseName(),
						tables.getString("TABLE_SCHEMA"),
						tables.getString("TABLE_NAME"));

				statement.execute(createTable);
			}
			
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	}
	
	
	/***
	 * Connect to an existing Generates a CREATE TABLE for Snowflake
	 * 
	 * Schema columns used from SQL Server
	 * DATA_TYPE_CAT,DATA_TYPE,COLUMN_NAME,NUMERIC_PRECISION,NUMERIC_SCALE,
	 * ORDINAL_POSITION,COLUMN_DEFAULT,CHARACTER_MAXIMUM_LENGTH
	 * 
	 * TODO: Using double quotes make objects case sensitive; give option to use use double quotes or not 
	 * 
	 * @param String srcSqlHost:
	 * @param String srcDatabse:
	 * @param String srcSchema: 
	 * @param String srcTable:
	 * @return String sql: SQL CREATE TABLE Statement
	 */
	public static String sqlCreateTableFromSs(String srcSqlHost, String srcDatabase, String srcSchema, String srcTable) {
		
		//make connection to source SQL Server
		SqlServerCnString srcCnString = new SqlServerCnString();								
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		//get column of current table, ordered by ordinal position
		RowSet cols = SqlServerDbScour.getSrcInformationSchema(
							srcCnString.getCnString(), 
							srcCnString.getDatabaseName(), 
							srcSchema, 
							srcTable);
		
		return SnowflakeDiscovery.sqlCreateTable(cols, srcDatabase,  srcSchema,  srcTable);
	}
	
		
	
}
