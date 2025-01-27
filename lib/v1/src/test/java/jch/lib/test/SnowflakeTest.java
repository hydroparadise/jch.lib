package jch.lib.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.json.simple.*;
import org.json.simple.parser.*;

import jch.lib.log.QLog;
import jch.lib.db.sqlserver.SqlServerCnString;
import jch.lib.db.sqlserver.SqlServerDbScour;
import jch.lib.db.sqlserver.SqlServerDiscovery;
import net.snowflake.client.jdbc.SnowflakeStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//import java.util.Map;


import java.io.FileInputStream;
import java.io.FileOutputStream;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;
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
 * 
 */



public class SnowflakeTest {
	static int API_PACK_SIZE = 1900000;
	static int MAX_CONCURRENCY_LEVEL = 4;
	
	SnowflakeTest() {
		
	}
	
	
	//Adding Feature
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	
	
	
	
	//Current Production
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
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
	 * @param String sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
	 * @param String sfDatabase: Remote Snowflake database name (ei, "DatabaseName"
	 * @param String sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param String rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
	 * @param String colDelim: Output file column/field delimiter(ie, "," or "~" or "\t")
	 * @param String textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"" -> ")
	 * @param String escapeChar: POSIX control character (ie, "\\" -> \)
	 * @param String azCredsLoc: Location of Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
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
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		RowSet ssTables = SqlServerDbScour.getSrcTables(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema);

		
		try {
			ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
			
			while(ssTables.next()) {
				
				
				final String schemaName = ssTables.getString("TABLE_SCHEMA");
				final String tableName = ssTables.getString("TABLE_NAME");
				final String fileName = schemaName + "." + tableName;
				
				//submit method for Async 
				exe.execute(()->  
					SnowflakeTest.writeCsvFromSqlServerTableFull(
							filePath, fileName,	maxFileSize,			//max file size in bytes
							srcSqlHost,srcDatabase,srcSchema,tableName,	//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
							sfCredsLoc,sfDatabase,sfSchema,tableName,	//String sfCredsLoc, sfDatabase,sfSchema, sfTable
							rowDelim,colDelim,textQualifier,escapeChar,	//
							azCredsLoc,azBlobDir,sfStage)							//
				);
			}
			
			exe.shutdown();
		
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
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
	 * 															|--> dbo.Transaction_20201231.csv
	 * 															| 	  |--> dbo.Transaction_20201231.csv.gz --> Azure Blob Container
	 * 															|--> dbo.Transaction_20201230.csv
	 *  														| 	  |--> dbo.Transaction_20201231.csv.gz --> Azure Blob Container
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
	 * @param String azCredsLoc: Location of Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
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
							valueLimiterCol = containsString(colName, timeDimensions);
							dataTypeCat = ssCols.getString("DATA_TYPE_CAT");
						}
					} else {
						valueLimiterCol = containsString(colName, timeDimensions);
						dataTypeCat = ssCols.getString("DATA_TYPE_CAT");
					}
					
				}
				
				//to pass function to ThreadPoolExecutor, variables must be "final"
				final String vlc = valueLimiterCol;
				final String dtc = dataTypeCat;
				
				if(valueLimiterCol != null) {
					
					
					QLog.log(fileName + ": Diff file");
					exe.execute(()->  
						SnowflakeTest.writeCsvFromSqlServerTableDiff(
							filePath, fileName,	maxFileSize,				//max file size in bytes
							srcSqlHost,srcDatabase,srcSchema,tableName,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
							sfCredsLoc,sfDatabase,sfSchema,tableName,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
							rowDelim,colDelim,textQualifier,escapeChar,
							vlc, dtc, azCredsLoc, azBlobDir, sfStage)
					);
				}
				else if (optionalFull == true) {
					
					QLog.log(fileName + ": Full file");
					exe.execute(()->  
						SnowflakeTest.writeCsvFromSqlServerTableFull(
								filePath, fileName,	maxFileSize,				//max file size in bytes
								srcSqlHost,srcDatabase,srcSchema,tableName,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
								sfCredsLoc,sfDatabase,sfSchema,tableName,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
								rowDelim,colDelim,textQualifier,escapeChar,
								azCredsLoc, azBlobDir, sfStage)
					);
				}
			}
			
			exe.shutdown();
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	}
	
	
	/***
	 * Establishes time range to create differential extracts for specific table
	 * 
	 *  											     Max: 12/31/2021
	 * 	SqlServer Hostname -> Database -> dbo -> Transaction(Postdate, TransAmount,.....)
	 * 															|--> dbo.Transaction_20201231.csv
	 * 															| 	  |--> dbo.Transaction_20201231.csv.gz --> Azure Blob Container
	 * 															|--> dbo.Transaction_20201230.csv
	 *  														| 	  |--> dbo.Transaction_20201231.csv.gz --> Azure Blob Container
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
	 * @param String azCredsLoc: Location of the Azure credentials of an Azure Blob Container instance (ie, "C\\azure_creds.json")
	 * @param String azBlobDir: A directory to store extracts for an Azure Blob Container instance (ie, "init" or "20220203" or "test/2020203")
	 * @param String sfStage: The name of the defined stage in Snowflake to consume from Azure (ie, "@stage_azure")
	 */
	static public void writeCsvFromSqlServerTableDiff(String filePath, String fileName, long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
			 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
			 String rowDelim, String colDelim, String textQualifier, String escapeChar,
			 String valueLimiterCol, String dataTypeCat,
			 String azCredsLoc, String azBlobDir, 
			 String sfStage) {
		
		
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		ssMaxValue = getSsMaxValue(srcSqlHost,  srcDatabase, srcSchema, srcTable, valueLimiterCol);

		QLog.log(SqlServerDbScour.sqlMaxValue(srcDatabase, srcSchema, srcTable, valueLimiterCol) + ": " + ssMaxValue);
		
		//make sure time-demen value was returned on SQL Server side, otherwise skip as there is no data to pull
		if (ssMaxValue != null) {
			
			//ping Snowflake source for most current data
			String sfMaxValue = null;
			sfMaxValue = getSfMaxValue(sfCredsLoc, sfDatabase, sfSchema, sfTable, valueLimiterCol);
			
			QLog.log(sqlSfMaxValue(sfDatabase, sfSchema, sfTable, valueLimiterCol) + ": " + sfMaxValue);
			
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
						
						writeCsvFromSqlServerTableValueLimit(filePath, fileName, maxFileSize,
								 srcSqlHost, srcDatabase, srcSchema, srcTable,
								 sfCredsLoc, sfDatabase, sfSchema, sfTable,
								 rowDelim, colDelim, textQualifier, escapeChar,
								 valueLimiterCol, valueLimit, 
								 azCredsLoc, azBlobDir, sfStage, false);
						
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					QLog.log(e.toString(),true);
				}
				
			}
			else {
				
				SnowflakeTest.writeCsvFromSqlServerTableFull(
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
	 * @param boolean sfForceReload: Performs delete on full 
	 */
	static public void writeCsvFromSqlServerTableValueLimit(String filePath, String fileName, long maxFileSize,
							 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
							 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
							 String rowDelim, String colDelim, String textQualifier, String escapeChar,
							 String valueLimiterCol, String valueLimit,
							 String azCredsLoc, String azBlobDir, 
							 String sfStage, boolean sfForceReload) {
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		
		//Get Snowflake Schema RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeTest.getConnection(sfCredsLoc);
		String sqlSfSchema = sqlSfDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
		RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);
		
        //generate list of column datatype categories
        TreeMap<String, String> datatypeCategories = setDataTypeCategories(ssSchemaRowSet);		
		
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
        String sqlSsTable = SqlServerDiscovery.sqlSelect(srcDatabase, srcSchema, srcTable, cols);
        
        QLog.log(srcSchema + "." + srcTable + " Column Count: " + cols.size());
        
        //if force load is true, delete current table segment to be be reloaded
        if(sfForceReload == true) 
        	sfDeleteFrom(sfCredsLoc, sfDatabase, sfSchema, sfTable, valueLimiterCol, valueLimit,
        			 datatypeCategories.get(valueLimiterCol.toUpperCase()));
        

        String sqlValueLimit = SqlServerDiscovery.sqlValuePrep(valueLimit,                                   
        		datatypeCategories.get(valueLimiterCol.toUpperCase()));                                         
        String sqlValueLimitCol = SqlServerDiscovery.sqlObjBracket(valueLimiterCol);                           
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
			QLog.log(e1.toString(),true);
		}       
		
		//if all went well with file write objects, begin writing to files
		if(buffer != null)  {
			
			long cCnt = 0;	//count calls
	        long aCnt = 0;	//count all records
	        long lCnt = 0;	//count of lines in current file
	        long fCnt = 0;	//
	        long fSize = 0;
	        StringBuilder values = new StringBuilder();
	        
			try {
				
				fCnt++;
				
				ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
				
				while(ssTable.next() ) {
					aCnt++;cCnt++;lCnt++;
					
					//if more than one record, add comma separator 
					if(lCnt>1) values.append(rowDelim); 
					
					//iterate columns and grab column values
					for(int i = 0; i < cols.size(); i++) {
						if(i > 0) values.append(colDelim);
						
						//wrap and append values
						String csvValue = sfCsvValuePrep(
											   ssTable.getString(cols.get(i)),
											   datatypeCategories.get(cols.get(i)),
											   textQualifier,escapeChar);
						
						values.append(csvValue);
					}		
					
					buffer.write(values.toString());
					
					if(aCnt%10000==0 || lCnt==1)
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
								sfCredsLoc, sfDatabase, sfSchema, sfTable, 
								filePath, fullFileName, fullFileName + ".gz", true, 
								azCredsLoc, azBlobDir, 
								sfStage, sfForceReload);
						
						exe.submit(t);
						
						lCnt = 0; 	//reset line count counter
						fSize = 0;	//reset file size counter
						fCnt++;		//increment file count counter
						
						fullFileName = filePath + fileName + "_" + outFileName + "_" + String.format("%03d", fCnt) + ".csv";
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
						sfCredsLoc, sfDatabase, sfSchema, sfTable, 
						filePath, fullFileName, fullFileName + ".gz", true, 
						azCredsLoc, azBlobDir, 
						sfStage, sfForceReload);
				exe.submit(t);
				
				exe.shutdown();
			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
			finally {
				//compiler complained for not having try/catch around buffer.close()
				//bad form to have in finally?
				try {
					buffer.close();
				} catch (IOException e) {QLog.log(e.toString(),true);} //silently fail
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
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		
		
		//Get Snowflake Schema RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeTest.getConnection(sfCredsLoc);
		String sqlSfSchema = sqlSfDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
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
			QLog.log(e1.toString(),true);
		}       
		
		//continue if buffer was successfully created
		if(buffer != null)  {
			
			long cCnt = 0;	//count calls
	        long aCnt = 0;	//count all records
	        long lCnt = 0;
	        long fSize = 0;
	        StringBuilder values = new StringBuilder();
	        
			try {
				
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
					
					if(aCnt%10000==0 || lCnt == 1)
							QLog.log(fileName + " " + cCnt + ": " + values.toString());
					
					fSize = fSize + values.length();
					if(fSize >= maxFileSize) {
						buffer.close();
						
						ExecuteZipAndShip t = new ExecuteZipAndShip(
								sfCredsLoc, sfDatabase, sfSchema, sfTable,
								filePath, fullFileName, fullFileName + ".gz", true, azBlobDir, azCredsLoc, 
								sfStage, true);
						exe.submit(t);
	
						lCnt = 0; 	//reset line count counter
						fSize = 0;	//reset file size counter
						fCnt++;		//increment file count counter
						
				        fullFileName = fileName + "_"  + String.format("%03d", fCnt) + ".csv";
				        fullFileNamePath = filePath + fullFileName;

						writer = new FileWriter(fullFileNamePath);
						buffer = new BufferedWriter(writer);
					}
					
					values.setLength(0);
				}
				

				buffer.close();
				//ExecuteCompressGzip t = new ExecuteCompressGzip(fullFileName, fullFileName + ".gz", true);
				//ExecuteZipAndShip t = new ExecuteZipAndShip(filePath, fullFileName, fullFileName + ".gz", true, azCredsLoc);
				ExecuteZipAndShip t = new ExecuteZipAndShip(
						sfCredsLoc, sfDatabase, sfSchema, sfTable,
						filePath, fullFileName, fullFileName + ".gz", true, azCredsLoc, azBlobDir, 
						sfStage, true);
				
				exe.submit(t);
				exe.shutdown();
				
			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
			finally {
				//compiler complained for not having try/catch around buffer.close()
				//bad form to have in finally?
				try {
					buffer.close();
				} catch (IOException e) {QLog.log(e.toString(),true);} //silently fail
			}
		}
	}
	
	
	/***
	 * Deletes all records for a given Snoflake table
	 * 
	 * TODO: suspend warehouse after complete
	 * 
	 * @param sfCredsLoc
	 * @param sfDatabase
	 * @param sfSchema
	 * @param sfTable
	 */
	static void sfDeleteFrom(String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable, 
			String valueLimiterCol, String valueLimit, String valueDatatypeCat) {
		

		try {
			java.sql.Connection sfCn = null;
			sfCn = SnowflakeTest.getConnection(sfCredsLoc, sfDatabase, sfSchema);
			Statement sfStatement = sfCn.createStatement();
						
			String sft = sfSchema.toUpperCase() + "." + sfTable.toUpperCase();

			String sql = "DELETE FROM " + sft;
			if(valueLimiterCol != null && valueLimit != null) {
				String sqlValueLimit = SqlServerDiscovery.sqlValuePrep(valueLimit, valueDatatypeCat);
				sql = sql + " WHERE " + valueLimiterCol + " = " + sqlValueLimit;
			}
			
			QLog.log(sql);
		
			sfStatement.execute(sql);
			sfStatement = sfCn.createStatement();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
		
	}
	
	static void sfDeleteFrom(String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable) {
		sfDeleteFrom(sfCredsLoc,sfDatabase,sfSchema,sfTable, null, null, null);
	}
	
    
	/***
	 * Asynchronously compresses file and sends to an Azure blob instance.
	 * @author harrisonc
	 *
	 */
	static class ExecuteZipAndShip extends Thread {
		/***
		 * Zips and sends to Azure Only
		 * @param filePath
		 * @param sourceFileName
		 * @param targetFileName
		 * @param deleteSource
		 * @param azBlobCredLoc
		 */
		public ExecuteZipAndShip(
				String filePath, String sourceFileName, String targetFileName, boolean deleteSource, 
				String azBlobCredLoc, String azBlobDir) {
			this.sourceFileName = sourceFileName;
			this.targetFileName = targetFileName;
			this.deleteSource = deleteSource;
			this.azBlobCredLoc = azBlobCredLoc;
			this.filePath = filePath;
		}
		
		/***
		 * Zips and sends to azure and snowflake consumes from azur
		 * @param sfCredsLoc
		 * @param sfDatabase
		 * @param sfSchema
		 * @param sfTable
		 * @param valueLimiterCol
		 * @param filePath
		 * @param sourceFileName
		 * @param targetFileName
		 * @param deleteSource
		 * @param azBlobCredLoc
		 * @param sfStage
		 */
		public ExecuteZipAndShip(
				String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,  
				String filePath, String sourceFileName, String targetFileName, boolean deleteSource, 
				String azBlobCredLoc, String azBlobDir,	
				String sfStage, boolean sfForceLoad) {
			
			this.sourceFileName = sourceFileName;
			this.targetFileName = targetFileName;
			this.deleteSource = deleteSource;
			this.azBlobCredLoc = azBlobCredLoc;
			this.filePath = filePath;
			this.sfCredsLoc = sfCredsLoc;
			this.sfDatabase = sfDatabase;
			this.sfSchema = sfSchema;
			this.sfTable = sfTable;
			this.sfForceLoad = sfForceLoad;
			this.sfStage = sfStage;
			this.azBlobDir = azBlobDir;
			
		}
		
		
		@Override
		public void run() {
			String sourceFile = this.filePath + this.sourceFileName;
			String targetFile = this.filePath + this.targetFileName;
			
			try {
				QLog.log("Compressing " + sourceFile);
				
				ExecuteZipAndShip.compressGzip(sourceFile, targetFile, this.deleteSource);
				
				QLog.log(sourceFile + " has been compressed.");
				
				if(this.azBlobCredLoc != null) {
					
					//if Azure blob directory hasn't been specified, send to container root
					if(this.azBlobDir != null && this.azBlobDir != "")
						AzureTest.putBlobFile(
								this.azBlobCredLoc, 
								this.filePath, 
								this.targetFileName, 
								this.azBlobDir);
					else
						AzureTest.putBlobFile(
								this.azBlobCredLoc, 
								this.filePath, 
								this.targetFileName);
					
					QLog.log(targetFile + " has been sent to azure.");
							
					//performs copy from Azure to Snowflake for a 7
					sfCopyFromAzureBlob(azBlobCredLoc, this.targetFileName, this.azBlobDir,
							sfCredsLoc, sfDatabase, sfSchema, sfTable, sfStage, sfForceLoad);

					QLog.log(this.targetFileName + " has been copied into Snowflake.");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
		}
		
	    /***
	     * 
	     * @param source
	     * @param target
	     * @throws IOException
	     */
	    public static void compressGzip(String sourceName, String targetName, boolean deleteSource) throws IOException {
	    	
	        Path source = Paths.get(sourceName);
	        Path target = Paths.get(targetName);
	    	
	        try (GZIPOutputStream gos = new GZIPOutputStream(
	                                      new FileOutputStream(target.toFile()));
	             FileInputStream fis = new FileInputStream(source.toFile())) {

	            // copy file
	            byte[] buffer = new byte[1024];
	            int len;
	            while ((len = fis.read(buffer)) > 0) {
	                gos.write(buffer, 0, len);
	            }
	            
	            fis.close();
	            
	            if(deleteSource == true) {
		            File delFile = new File(sourceName);
		            delFile.delete();            	
	            }

	        }

	    }
		
	    /***
	     * 
	     * @param azBlobCredsLoc
	     * @param blobFileName
	     * @param sfCredsLoc
	     * @param sfDatabase
	     * @param sfSchema
	     * @param sfTable
	     * @param sfStage
	     * @param valueLimiterCol
	     */
	    public static void sfCopyFromAzureBlob(String azBlobCredsLoc, String azBlobFileName,  String azBlobDir,
	    		String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable, String sfStage, boolean forceLoad) {
	    	
	    	
	    	
	    	
			String sql = sqlSfCopyFromStage(azBlobCredsLoc, azBlobFileName, azBlobDir, sfSchema, sfTable, sfStage, forceLoad);
			
			QLog.log(sql);

			try {
				
				java.sql.Connection sfCn = null;
				sfCn = SnowflakeTest.getConnection(sfCredsLoc, sfDatabase, "PUBLIC");
				Statement sfStatement = sfCn.createStatement();
				

				sfStatement.execute(sql);
				sfStatement.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
	    }
	    
		boolean deleteSource;
		String targetFileName;
		String sourceFileName;
		String filePath;
		String azBlobCredLoc;
		String sfCredsLoc; 
		String sfDatabase; 
		String sfSchema; 
		String sfTable;
		boolean sfForceLoad;
		String sfStage;
		String azBlobDir;
	}
	
	
	
	/***
	 * 
	 * @param azBlobCredsLoc
	 * @param blobFileName
	 * @param sfSchema
	 * @param sfTable
	 * @param sfStage
	 * @param valueLimiterCol
	 * @return
	 */
	static String sqlSfCopyFromStage(String azBlobCredsLoc, String azBlobFileName, String azBlobDir, 
			String sfSchema, String sfTable, String sfStage, boolean forceLoad) {
		String output = null;
		   
		StringBuilder sql = new StringBuilder();
		
		String tbl = sfSchema.toUpperCase() + "." + sfTable.toUpperCase();
		
		//if(valueLimiterCol == null) {
		//	sql.append("DELETE FROM " + tbl + "; ");
		//}
		String abfnp = null;
		if(azBlobDir != null && azBlobDir != "") {
			abfnp = azBlobDir + "/" + azBlobFileName;
		} else abfnp = azBlobFileName;
		
		sql.append("COPY INTO " + tbl + " FROM " + sfStage + " PATTERN = \"" + abfnp + "\"");
		
		if(forceLoad == true) {
			sql.append(" FORCE = TRUE");
		}
		
		sql.append(";");
		output = sql.toString();
		

		return output;
	}
	
	/*
	
	COPY INTO DBO.NONACCTNAMEFMHISTORY FROM @stage_db_servSYS000dbo_dev2 PATTERN = "dbo.NONACCTNAMEFMHISTORY_.*.csv.gz";
	-- or --
	DELETE FROM DBO.RESERVEPLANTRANSACTION;
	COPY INTO DBO.RESERVEPLANTRANSACTION FROM @stage_db_servSYS000dbo_dev2 PATTERN = "dbo.RESERVEPLANTRANSACTION_.*.csv.gz" FORCE = TRUE;
	 */
	static String sqlSfCopyFromStage2(String blobFileName, String sfSchema, String sfTable, String sfStage, String valueLimiterCol) {
		String output = null;
		StringBuilder sql = new StringBuilder();
		
		String tbl = sfSchema + "." + sfTable;
		
		if(valueLimiterCol == null) {
			sql.append("DELETE FROM " + tbl + "; ");
		}
		
		sql.append("COPY INTO " + tbl + " FROM " + sfStage + " PATTERN = \"" + blobFileName + "\"");
		
		if(valueLimiterCol == null) {
			sql.append(" FORCE = TRUE");
		}
		
		sql.append(";");
		output = sql.toString();
		
		return output;
	}
	
	/*********************************************************************************************************************************************************************************/
	/***************************************************************************  adding azure connection ****************************************************************************/
	/*********************************************************************************************************************************************************************************/
	
	static public void writeCsvFromSqlServerAllTablesFull(String filePath,long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema,
			 String sfCredsLoc, String sfDatabase, String sfSchema,
			 String rowDelim, String colDelim, String textQualifier, String escapeChar) {
		
		//Get Sql Server Table information in a RowSet, connection opening and closing handled by called functions
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		RowSet ssTables = SqlServerDbScour.getSrcTables(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema);

		
		try {
			ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
			
			while(ssTables.next()) {
				
				
				final String schemaName = ssTables.getString("TABLE_SCHEMA");
				final String tableName = ssTables.getString("TABLE_NAME");
				final String fileName = schemaName + "." + tableName;
				
				exe.execute(()->  
				SnowflakeTest.writeCsvFromSqlServerTableFull(
						filePath, fileName,	maxFileSize,				//max file size in bytes
						srcSqlHost,srcDatabase,srcSchema,tableName,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
						sfCredsLoc,sfDatabase,sfSchema,tableName,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
						rowDelim,colDelim,textQualifier,escapeChar)
				);
			}
			
			exe.shutdown();
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	}
	
	
	/***
	 * Connects to a Sql Server with a Database and Schema context, iterates through all tables and
	 * compares a table against a given Snowflake Database and Schema context with a table of the 
	 * same name, and creates a compressed CSV file of the difference between the two tables.
	 * The file names will contain the specific time slice the file represents in YYYYMMDD format.
	 * 
	 *  											     Max: 12/31/2021
	 * 	SqlServer Hostname -> Database -> dbo -> Transaction(Postdate, TransAmount,.....)
	 * 															|--> dbo.Transaction_20201231.csv
	 * 															| 	  |--> dbo.Transaction_20201231.csv.gz
	 * 															|--> dbo.Transaction_20201230.csv
	 *  														| 	  |--> dbo.Transaction_20201231.csv.gz
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
	 * This method uses a thread pool to concurrently pull tables, with the max concurrency specified by
	 * MAX_CONCURRENCY_LEVEL (ie 4 threads at any give time)
	 * 
	 * @param filePath: Output file location (ie, "C:\\temp\\")
	 * @param maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
	 * @param srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param srcDatabse: Source Database in which table resides (ie,"DatabaseName")
	 * @param srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param sfCredsLoc: Location of Snowflake credentials specified via JSON (ie, "C\\snowflake_creds.json")
	 * @param sfDatabase: Remote Snowflake database name (ei, "DatabaseName")
	 * @param sfSchema: Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param rowDelim: Output file row delimiter (ie,"\r\n" or "\n")
	 * @param colDelim: Output file column or field delimiter(ie, "," or "~" or "\t")
	 * @param textQualifier: String, text, or VARCHAR datatype value qualifier (ie,"\"" -> ")
	 * @param escapeChar: POSIX control character (ie, "\\" -> \)
	 * @param optionalFull: Prints full file it time dimension can't be determined it set to true
	 */
	static public void writeCsvFromSqlServerAllTablesDiff(String filePath,long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema,
			 String sfCredsLoc, String sfDatabase, String sfSchema,
			 String rowDelim, String colDelim, String textQualifier, String escapeChar,
			 ArrayList<String> timeDimensions, boolean strictPK, boolean optionalFull) {

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
					
					//if strickPK is set to true, check the column constraint to see if "PK" is present which
					//is usually indicative of the column be being a primary key
					if(strictPK == true) {
						String constraint = ssCols.getString("CONSTRAINT_NAME");
						if(constraint != null && constraint.toUpperCase().contains("PK")) {
							valueLimiterCol = containsString(colName, timeDimensions);
							dataTypeCat = ssCols.getString("DATA_TYPE_CAT");
						}
					} else {
						valueLimiterCol = containsString(colName, timeDimensions);
						dataTypeCat = ssCols.getString("DATA_TYPE_CAT");
					}
					
				}
				
				//to pass function to ThreadPoolExecutor, variables must be "final"
				final String vlc = valueLimiterCol;
				final String dtc = dataTypeCat;
				
				if(valueLimiterCol != null) {
					QLog.log(fileName + ": Diff file");
					
					exe.execute(()->  
						SnowflakeTest.writeCsvFromSqlServerTableDiff(
							filePath, fileName,	maxFileSize,				//max file size in bytes
							srcSqlHost,srcDatabase,srcSchema,tableName,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
							sfCredsLoc,sfDatabase,sfSchema,tableName,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
							rowDelim,colDelim,textQualifier,escapeChar,
							vlc, dtc)
					);
				}
				else if (optionalFull == true) {
					QLog.log(fileName + ": Full file");
					
					exe.execute(()->  
						SnowflakeTest.writeCsvFromSqlServerTableFull(
								filePath, fileName,	maxFileSize,				//max file size in bytes
								srcSqlHost,srcDatabase,srcSchema,tableName,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
								sfCredsLoc,sfDatabase,sfSchema,tableName,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
								rowDelim,colDelim,textQualifier,escapeChar)
					);
				}
			}
			
			exe.shutdown();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	}
	
	
	/***
	 */
	static public void writeCsvFromSqlServerTableDiff(String filePath, String fileName, long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
			 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
			 String rowDelim, String colDelim, String textQualifier, String escapeChar,
			 String valueLimiterCol, String dataTypeCat) {
		
		
		//make sure SQL server source has data to pull
		String ssMaxValue = null;
		ssMaxValue = getSsMaxValue(srcSqlHost,  srcDatabase, srcSchema, srcTable, valueLimiterCol);
		
		QLog.log(SqlServerDbScour.sqlMaxValue(srcDatabase, srcSchema, srcTable, valueLimiterCol) + ": " + ssMaxValue);
		
		//make sure some value was returned on SQL Server side, otherwise skip
		if (ssMaxValue != null) {
			
			//ping Snowflake source for most current data
			String sfMaxValue = null;
			sfMaxValue = getSfMaxValue(sfCredsLoc, sfDatabase, sfSchema, sfTable, valueLimiterCol);

			QLog.log(sqlSfMaxValue(sfDatabase, sfSchema, sfTable, valueLimiterCol) + ": " + sfMaxValue);
			
			//make sure some value was returned, otherwise pull full file
			if(sfMaxValue != null) {

				QLog.log(SqlServerDbScour.sqlGreaterThan(srcDatabase, srcSchema, srcTable, valueLimiterCol, sfMaxValue, dataTypeCat));
				
				SqlServerCnString srcCnString = new SqlServerCnString();
				srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
				
				RowSet segs = SqlServerDbScour.executeSqlRowSet(srcCnString.getCnString(), 
						SqlServerDbScour.sqlGreaterThan(srcDatabase, srcSchema, srcTable, valueLimiterCol, sfMaxValue, dataTypeCat));
				try {
					while(segs.next()) {
						
						String valueLimit = segs.getString(valueLimiterCol);

						QLog.log(valueLimit);
						
						writeCsvFromSqlServerTableValueLimit(filePath, fileName, maxFileSize,
								 srcSqlHost, srcDatabase, srcSchema, srcTable,
								 sfCredsLoc, sfDatabase, sfSchema, sfTable,
								 rowDelim, colDelim, textQualifier, escapeChar,
								 valueLimiterCol, valueLimit);
			
						
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					QLog.log(e.toString(),true);
				}
				
			}
			else {
				
				SnowflakeTest.writeCsvFromSqlServerTableFull(
						filePath, fileName,	maxFileSize,				//max file size in bytes
						srcSqlHost,srcDatabase,srcSchema,srcTable,		//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
						sfCredsLoc,sfDatabase,sfSchema,sfTable,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
						rowDelim,colDelim,textQualifier,escapeChar);	
				
			}
			
		}

	}
	
	
	
	public ArrayList<String> db_servDimensions = new ArrayList<String>( Arrays.asList("POSTDATE", "PROCESSDATE", "DATE"));
	
	static String containsString(String checkString, ArrayList<String> stringList) {
		String output = null;
		
		if(stringList.contains(checkString.toUpperCase()))
			output = checkString;
		return output;
	}
	
	
	
	/***
	 * 
	 */
	static String getSfMaxValue(String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable, String valueLimiterCol) {
		String output = null;
		
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeTest.getConnection(sfCredsLoc);
		String sqlSfMaxValue = sqlSfMaxValue(sfDatabase, sfSchema, sfTable, valueLimiterCol);
		RowSet rsSfmax = executeRowSetSnowflakeCommand(sqlSfMaxValue, sfCn);
		
		try {
			
			rsSfmax.next();
			output = rsSfmax.getString("MaxValue");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	
		return output;
	}
	
	
	/***
	 * 
	 * @param srcSqlHost
	 * @param srcDatabase
	 * @param srcSchema
	 * @param srcTable
	 * @param valueLimiterCol
	 * @return
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
			QLog.log(e.toString(),true);
		}
	
		return output;
	}

    /***
     * Compresses source to output file in GZIP format
     * 
     * @param source: The file and location of extract to compress (ie,"C:\\temp\\EXTRACT.csv","C:\\temp\\EXTRACT.csv.gz") 
     * @param target: The output file name after compression.
     * @deleteSource: Flag to delete source file after compression.
     * @throws IOException
     */
    public static void compressGzip(String sourceName, String targetName, boolean deleteSource) throws IOException {
    	
        Path source = Paths.get(sourceName);
        Path target = Paths.get(targetName);
    	
        try (GZIPOutputStream gos = new GZIPOutputStream(
                                      new FileOutputStream(target.toFile()));
            FileInputStream fis = new FileInputStream(source.toFile())) {

            // copy file
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                gos.write(buffer, 0, len);
            }
            
            fis.close();
            
            if(deleteSource == true) {
	            File delFile = new File(sourceName);
	            delFile.delete();            	
            }

        }

    }
    

    
	/***
	 * Asynchronously compresses file
	 * @author harrisonc
	 *
	 */
	static class ExecuteCompressGzip extends Thread {
		public ExecuteCompressGzip(String sourceFile, String targetFile, boolean deleteSource) {
			this.sourceFile = sourceFile;
			this.targetFile = targetFile;
			this.deleteSource = deleteSource;
		}
		
		@Override
		public void run() {
			try {
				ExecuteCompressGzip.compressGzip(this.sourceFile, this.targetFile, this.deleteSource);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
		}
		
	    /***
	     * 
	     * @param source
	     * @param target
	     * @throws IOException
	     */
	    public static void compressGzip(String sourceName, String targetName, boolean deleteSource) throws IOException {
	    	
	        Path source = Paths.get(sourceName);
	        Path target = Paths.get(targetName);
	    	
	        try (GZIPOutputStream gos = new GZIPOutputStream(
	                                      new FileOutputStream(target.toFile()));
	             FileInputStream fis = new FileInputStream(source.toFile())) {

	            // copy file
	            byte[] buffer = new byte[1024];
	            int len;
	            while ((len = fis.read(buffer)) > 0) {
	                gos.write(buffer, 0, len);
	            }
	            
	            fis.close();
	            
	            if(deleteSource == true) {
		            File delFile = new File(sourceName);
		            delFile.delete();            	
	            }

	        }

	    }
		
		boolean deleteSource;
		String targetFile;
		String sourceFile;
	}
	
    
	
	/***
	 * Takes source table and creates a *.csv or *.csv.gz based on a source Sql Server host, database, schema,
	 * and table, compares to specified remote Snowfalke instance, compute, database, schema, and table to
	 * build column ordinal sensitive datasets with omptionaly compressing the file via GZip.
	 * 
	 * @param filePath: Output file location (ie, "C:\\temp\\")
	 * @param fileName: Output file name (ie, "dbo.ACCOUNT")
	 * @param maxFileSize: Max file size threshold (ie, 30000 "means 30Kb" or 4000000000L "means 4Gb") 
	 * @param srcSqlHost: Source SQL Server Hostname or IP address (ie, "SQLSERV01" or "192.168.1.102")
	 * @param srcDatabse: Source Database in which table resides (ie,"DatabaseName")
	 * @param srcSchema: Source Database schema table resides (ie, "dbo")
	 * @param srcTable: Source SQL Server table to create extract of (ie."ACCOUNT")
	 * @param sfTable": Remote Snowflake table name to compare columns (ie,"ACCOUNT")
	 */
	static public void writeCsvFromSqlServerTableFull(String filePath, String fileName, long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable, String sfCredsLoc) {
		
		SnowflakeTest.writeCsvFromSqlServerTableFull(
				filePath, fileName, maxFileSize,	
				srcSqlHost,	srcDatabase, srcSchema,	srcTable,		
				sfCredsLoc,	srcDatabase, srcSchema,	srcTable);	
					
	}
	
	
	/***
	 * Takes source table and creates a *.csv or *.csv.gz based on a source Sql Server host, database, schema,
	 * and table, compares to specified remote Snowfalke instance, compute, database, schema, and table to
	 * build column ordinal sensitive datasets with omptionaly compressing the file via GZip.
	 * 
	 * @param filePath: Output file location (ie, "C:\\temp\\")
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
	 */
	static public void writeCsvFromSqlServerTableFull(String filePath, String fileName, long maxFileSize,
			 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
			 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable) {
		SnowflakeTest.writeCsvFromSqlServerTableFull(
				filePath, fileName,	maxFileSize,	//max file size in bytes
				srcSqlHost,	srcDatabase, srcSchema,	srcTable,	//String srcSqlHost, String srcDatabse, String srcSchema, String srcTable,
				sfCredsLoc,	sfDatabase,	sfSchema,  sfTable,		//String sfCredsLoc, sfDatabase,sfSchema, sfTable
				"\r\n",	",","\"","\\");			//rowDelim,colDelim,textQualifier,escapeChar						
	}
	
	
	/***
	 * Takes source table and creates a *.csv or *.csv.gz based on a source Sql Server host, database, schema,
	 * and table, compares to specified remote Snowfalke instance, compute, database, schema, and table to
	 * build column ordinal sensitive datasets with omptionaly compressing the file via GZip.
	 * 
	 * @param filePath: Output file location (ie, "C:\\temp\\")
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
	 */
	static public void writeCsvFromSqlServerTableValueLimit(String filePath, String fileName, long maxFileSize,
							 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
							 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
							 String rowDelim, String colDelim, String textQualifier, String escapeChar,
							 String valueLimiterCol, String valueLimit) {
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		
		//Get Snowflake Schema RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeTest.getConnection(sfCredsLoc);
		String sqlSfSchema = sqlSfDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
		RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);
		
        //generate list of column datatype categories
        TreeMap<String, String> datatypeCategories = setDataTypeCategories(ssSchemaRowSet);		
		
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
        String sqlSsTable = SqlServerDiscovery.sqlSelect(srcDatabase, srcSchema, srcTable, cols);
        
        QLog.log("Column Count: " + cols.size() + ", " + datatypeCategories.size());
        
        String sqlValueLimit = SqlServerDiscovery.sqlValuePrep(valueLimit, datatypeCategories.get(valueLimiterCol.toUpperCase()));
        String sqlValueLimitCol = SqlServerDiscovery.sqlObjBracket(valueLimiterCol);
        sqlSsTable = sqlSsTable + " WHERE " + sqlValueLimitCol + " = " +  sqlValueLimit;

        QLog.log(sqlSsTable);
        
        ResultSet ssTable = SqlServerDbScour.executeSqlResultSet(srcCnString.getCnString(), sqlSsTable);
       
        String outFileName = fileNamePrep(valueLimit,datatypeCategories.get(valueLimiterCol.toUpperCase()));
 		String fullFileName = filePath + fileName + "_" + outFileName + ".csv";
		FileWriter writer = null;
		BufferedWriter buffer = null;
		
		try {
			writer = new FileWriter(fullFileName);
			buffer = new BufferedWriter(writer);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			QLog.log(e1.toString(),true);
		}       
		
		if(buffer != null)  {
			
			long cCnt = 0;	//count calls
	        long aCnt = 0;	//count all records
	        long lCnt = 0;
	        long fCnt = 0;
	        long fSize = 0;
	        StringBuilder values = new StringBuilder();
	        
			try {
				
				fCnt++;
				
				ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
				
				while(ssTable.next() ) {
					aCnt++;cCnt++;lCnt++;
					
					//if more than one record, add comma separator 
					if(lCnt>1) values.append(rowDelim); 
					
					//iterate columns and grab column values
					for(int i = 0; i < cols.size(); i++) {
						if(i > 0) values.append(colDelim);
						
						//wrap and append values
						String csvValue = sfCsvValuePrep(ssTable.getString(cols.get(i)),
											   datatypeCategories.get(cols.get(i)),
											   textQualifier,escapeChar);
						
						values.append(csvValue);
					}		
					
					buffer.write(values.toString());

					if(aCnt%10000==0)
						QLog.log(fileName + " " + cCnt + ": " + values.toString());
					
					//keep track of growing file size 
					fSize = fSize + values.length();
					
					//split file out after size exceeds size limit
					if(fSize >= maxFileSize) {
						buffer.close();
						ExecuteCompressGzip t = new ExecuteCompressGzip(fullFileName, fullFileName + ".gz", true);
						exe.submit(t);
						
						lCnt = 0; 	//reset line count counter
						fSize = 0;	//reset file size counter
						fCnt++;		//increment file count counter
						fullFileName = filePath + fileName + "_" + outFileName + "_" + String.format("%03d", fCnt) + ".csv";
						writer = new FileWriter(fullFileName);
						buffer = new BufferedWriter(writer);
						
					}
					
					values.setLength(0);
				}
						
				//close file
				buffer.close();
				
				//compress file
				ExecuteCompressGzip t = new ExecuteCompressGzip(fullFileName, fullFileName + ".gz", true);
				exe.submit(t);
				
				exe.shutdown();
			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
			finally {
				//compiler complained for not having try/catch around buffer.close()
				//bad form to have in finally?
				try {
					buffer.close();
				} catch (IOException e) {QLog.log(e.toString(),true);} //silently fail
			}
		}
	}
	
	
	/***
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
	 */
	static public void writeCsvFromSqlServerTableFull(String filePath, String fileName, long maxFileSize,
							 String srcSqlHost, String srcDatabase, String srcSchema, String srcTable,
							 String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,
							 String rowDelim, String colDelim, String textQualifier, String escapeChar) {
		
		//Get Sql Server Schema Rowset
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		RowSet ssSchemaRowSet = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), srcSchema, srcTable);
		
		
		//Get Snowflake Schema RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeTest.getConnection(sfCredsLoc);
		String sqlSfSchema = sqlSfDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
		RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);
		
        //generate list of column datatype categories
        TreeMap<String, String> datatypeCategories = setDataTypeCategories(ssSchemaRowSet);		
		
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
        
        //generate select statement for data extract
        String sqlSsTable = SqlServerDiscovery.sqlSelect(srcDatabase, srcSchema, srcTable, cols);
        
        QLog.log("Column Count: " + cols.size());
        
        //execute SELECT Table
        QLog.log(sqlSsTable);
        ResultSet ssTable = SqlServerDbScour.executeSqlResultSet(srcCnString.getCnString(), sqlSsTable);
        
        long fCnt = 1;
        String fullFileName = filePath + fileName  + "_"  + String.format("%03d", fCnt) + ".csv";
		FileWriter writer = null;
		BufferedWriter buffer = null;
		
		try {
			writer = new FileWriter(fullFileName);
			buffer = new BufferedWriter(writer);
			
		} catch (IOException e1) {

			e1.printStackTrace();
			QLog.log(e1.toString(),true);
		}       
		
		//continue if buffer was successfully created
		if(buffer != null)  {
			
			long cCnt = 0;	//count calls
	        long aCnt = 0;	//count all records
	        long lCnt = 0;
	        long fSize = 0;
	        StringBuilder values = new StringBuilder();
	        
			try {
				
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
					
					if(aCnt%10000==0)
						QLog.log(fileName + " " + cCnt + ": " + values.toString());
					
					fSize = fSize + values.length();
					if(fSize >= maxFileSize) {
						buffer.close();
						
						ExecuteCompressGzip t = new ExecuteCompressGzip(fullFileName, fullFileName + ".gz", true);
						exe.submit(t);
	
						
						lCnt = 0; 	//reset line count counter
						fSize = 0;	//reset file size counter
						fCnt++;		//increment file count counter
						fullFileName = filePath + fileName  + "_"  + String.format("%03d", fCnt) + ".csv";
						
						writer = new FileWriter(fullFileName);
						buffer = new BufferedWriter(writer);
					}
					
					values.setLength(0);
				}
				

				buffer.close();
				ExecuteCompressGzip t = new ExecuteCompressGzip(fullFileName, fullFileName + ".gz", true);
				
				exe.submit(t);
				exe.shutdown();
				
			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
			finally {
				//compiler complained for not having try/catch around buffer.close()
				//bad form to have in finally?
				try {
					buffer.close();
				} catch (IOException e) {QLog.log(e.toString(),true);} //silently fail
			}
		}
	}
	
	/***
	 * Copy table via series of API calls to Snowflake
	 * 
	 * @param srcSqlHost
	 * @param srcDatabse
	 * @param srcSchema
	 * @param srcTable
	 * @param sfCredsLoc
	 * @param sfDatabase
	 * @param sfSchema
	 * @param sfTable
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
		sfCn = SnowflakeTest.getConnection(sfCredsLoc);
		String sqlSfSchema = sqlSfDatabaseTableInformationShema(sfDatabase, sfSchema, sfTable);
		RowSet sfSchemaRowSet = executeRowSetSnowflakeCommand(sqlSfSchema, sfCn);
		
		
        //generate list of column datatype categories
        TreeMap<String, String> datatypeCategories = setDataTypeCategories(ssSchemaRowSet);		
		
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssSchemaRowSet,sfSchemaRowSet);
        String sqlSsTable = SqlServerDiscovery.sqlSelect(srcDatabse, srcSchema, srcTable, cols);
        
        QLog.log(sqlSsTable);
        
        ResultSet ssTable = SqlServerDbScour.executeSqlResultSet(srcCnString.getCnString(), sqlSsTable);

        try {
        	sfCn = SnowflakeTest.getConnection(sfCredsLoc,sfDatabase);
			copyApiTableDataIterate(cols, ssTable, sfCn, sfSchema, sfTable, datatypeCategories);
			sfCn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
        

	}
	
	/***
	 * 
	 * @param colSet
	 * @param srcTableResultSet
	 * @param sfConnection
	 * @param sfSchema
	 * @param sfTable
	 * @param datatypeCategories
	 * @throws SQLException
	 */
	static void copyApiTableDataIterate(ArrayList<String> colSet, ResultSet srcTableResultSet, 
			java.sql.Connection sfConnection, String sfSchema, String sfTable, 
			TreeMap<String, String> datatypeCategories) throws SQLException {

		long cCnt = 0;	//count calls
        long aCnt = 0;	//count all records
        long pCnt = 0;	//count packed records
        
        String sqlInsertInto = sqlSfGenerateInsertInto(sfSchema.toUpperCase(), sfTable.toUpperCase(), colSet);
    	
        StringBuilder values = new StringBuilder();
    	StringBuilder sql = new StringBuilder();
    	
    	//Instantiate Thread pooler
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
        		values.append(sfSqlValuePrep(srcTableResultSet.getString(colSet.get(i)), 
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
	
	static class ExecuteUpdateSnowflakeCommand extends Thread {
		public ExecuteUpdateSnowflakeCommand(String sqlCommand, java.sql.Connection sfConnection, long cCnt) {
			this.sqlCommand = sqlCommand;
			this.sfConnection = sfConnection;
			this.cCnt = cCnt;
		}
		
		@Override
		public void run() {
			Statement stmnt = null;
			
			try {
				if(sfConnection != null &&
				   sfConnection.isClosed() != true) {
					stmnt = sfConnection.createStatement();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				stmnt.executeUpdate(sqlCommand);
				QLog.log(cCnt + " executed!");
			} catch (SQLException e) {
				QLog.log(cCnt + " exception!");
				e.printStackTrace();
				QLog.log(e.toString(),true);
			}
			
			try {
				stmnt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		String sqlCommand;
		java.sql.Connection sfConnection;
		long cCnt;
	}
	
	/***
	 * 
	 * @param sqlCommand
	 * @param sfConnection
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
			QLog.log(e.toString(),true);
		}
		finally {
			try {
				stmnt.close();
			} catch (SQLException e) {QLog.log(e.toString(),true);}
		}
		
		return output;
	}
	
	/***
	 * Builds a tree map of Column names to Data Type Category f
	 * @param shema
	 * @return
	 */
	public static TreeMap<String, String> setDataTypeCategories(RowSet shema) {
        TreeMap<String, String> output;
        output = new TreeMap<String, String>();
        try {
			shema.beforeFirst();
			
	        while(shema.next()) {
	        	output.put(shema.getString("COLUMN_NAME").toUpperCase(), 
	        			   shema.getString("DATA_TYPE_CAT"));
	        }			
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
        return output;
	}
	
	
	/* Snowflake Script examples
	 
		CREATE TABLE TESTINS (
		    INSDATE1 DATE,
		    INSDATE2 DATETIME,
		    INSCHAR1 VARCHAR,
		    INTINT INT
		)
		
		//Passed
		INSERT INTO TESTINS (INSDATE1,INSDATE2,INSCHAR1,INTINT) 
		VALUES ('1/1/2021','1/1/2021','Test insert',23)
		
		//Passed
		INSERT INTO TESTINS (INSDATE1,INSDATE2,INSCHAR1,INTINT) 
		VALUES ('1/1/2021','2013-05-08T23:39:20.123',$$Test insert$$,23)
	 */

	
	/*
	 * 
	 * snowflakeCreds \
	 *                 --- database, schema, table -> match fields
	 * srcSQLHost     /
	 * 
	 */
	public static void copyApiTableData(String snowflakeCreds, String srcSqlHost, 
			String database, String schema, String table) throws SQLException, IOException {

		//Get Sql Server RowSet
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , database);

		//get tables for given database
		RowSet ssCols = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), srcCnString.getDatabaseName(), schema, table);
		
		
		//Get Snowflake RowSet
		java.sql.Connection sfCn = null;
		sfCn = SnowflakeTest.getConnection(snowflakeCreds);
		Statement sfStatement = sfCn.createStatement();
		
	    ResultSet sfColsRs = sfStatement.executeQuery(
	    		sqlSfDatabaseTableInformationShema(database,schema,table));
		
        RowSetFactory rsf = RowSetProvider.newFactory();
        CachedRowSet sfCols = rsf.createCachedRowSet();
        sfCols.populate(sfColsRs);
        sfStatement.close();
        
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssCols,sfCols);
        
        //load up a treemap to quickly reslove datatypes

        TreeMap<String, String> colDatatypeCat = new TreeMap<String, String>();
        ssCols.beforeFirst();
        while(ssCols.next()) {
        	colDatatypeCat.put(ssCols.getString("COLUMN_NAME").toUpperCase(), 
        			           ssCols.getString("DATA_TYPE_CAT"));
        }
        
        sfCols.close();

        QLog.log(colDatatypeCat.size() + "");
        
        Connection srcCn = DriverManager.getConnection(srcCnString.getCnString()); 
        Statement ssStatment = srcCn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        ResultSet resColumns = ssStatment.executeQuery(
        		SqlServerDiscovery.sqlSelect(database,schema,table, cols));
        
        String sqlInsertInto = sqlSfGenerateInsertInto(schema.toUpperCase(), table.toUpperCase(), cols);
        
        
        //spin up snowflake connection with database context
		java.sql.Connection cn = null;
		cn = SnowflakeTest.getConnection(snowflakeCreds,database.toUpperCase());
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
        		values.append(sfSqlValuePrep(resColumns.getString(cols.get(i)), colDatatypeCat.get(cols.get(i))));
        	}		
        	values.append(")");

        	if(sqlInsertInto.length() + values.length() > API_PACK_SIZE) {
        		cCnt++;
        		
        		sql.append(sqlInsertInto + " VALUES  " + values.toString());

        		QLog.log("cCnt: " + cCnt+ ", pCnt: " + pCnt + ", len: " + sql.length() + ", aCnt: " + aCnt);
        		
        		//Synchronous Call
        		//statement.executeUpdate(sql.toString());
        		
        		//Asynchronous Call
        		//TODO: method no longer works
        		//statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql.toString());
        		
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
        //TODO: method no longer works
		//statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql.toString());
        
        statement.close();
        cn.close();
	}
	
	
	/***
	 * Prepares a given value for Snoflake consumption via a CSV based on the supplied datatype.
	 * Converts unicode characters to space characters (" ").
	 * 
	 * @param value
	 * @param datatypeCat
	 * @param textQualifier
	 * @param escapeChar
	 * @return String
	 */
	public static String sfCsvValuePrep(
			String value, String datatypeCat, String textQualifier, String escapeChar) {
		
		String output = "";
		
		//DATA_TYPE_CAT: TEXT, NUMERIC, DATETIME, OTHER
		if(value == null) output = "";
		else if (datatypeCat.equals("TEXT")) {
			
			//Snowflake likes ASCII only.  (it says UTF-8, but its complaining for characters
			//between 127-255
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
			if(textQualifier != null && textQualifier != "") {
				if(escapeChar != null && escapeChar != "")
					value = value.replace(textQualifier,escapeChar + textQualifier );
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
	 * @param value
	 * @param datatypeCat
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
	
	
	static List<String> dateFormatStrings = 
			Arrays.asList("yyyy-MM-dd", "yyyyMMdd","MM/dd/yyyy","MM-dd-yyyy","MM/dd/yy","MM-dd-yy");
	
	/***
	 * Tries to parse a String to Date
	 * @param dateString
	 * @return
	 */
	static java.util.Date tryDateParse(String dateString) {
	    for (String formatString : dateFormatStrings) {
	        try {
	            return new SimpleDateFormat(formatString).parse(dateString);
	        }
	        catch (java.text.ParseException e) {QLog.log(e.toString(),true);}
	    }
	    return null;
	}

	
	/***
	 * 
	 * 
	 * @param schema
	 * @param table
	 * @param cols
	 * @return SQL Sting
	 */
	public static String sqlSfGenerateInsertInto(String schema, String table,ArrayList<String> cols) {
		StringBuilder output = new StringBuilder();
		
		output.append("INSERT INTO \"" + schema.toUpperCase() + "\".\"" + table.toUpperCase() + "\" (");
		for(int i = 0; i < cols.size(); i++) {
			if(i > 0) output.append(",");
			output.append("\"" + cols.get(i) + "\"");
		}
		output.append(")");
		
		return output.toString();
	}
	

	
	
	/***
	 * Compares two RowSets and returns a matching set of column names from both rowsets
	 * 
	 * @param RowSet ssRowSet: Sql Server RowSet
	 * @param RowSet sfRowSet: SnowFlake RowSet
	 * @return
	 * @throws SQLException
	 */
	public static ArrayList<String> rowsetColCompare(RowSet ssRowSet, RowSet sfRowSet) {
		ArrayList<String> output = new ArrayList<String>();
		String ssCol = "";
		String sfCol = "";
		
		try {
			while(sfRowSet.next()) {
				sfCol = sfRowSet.getString("COLUMN_NAME").toUpperCase();
				
				ssRowSet.beforeFirst();
				while(ssRowSet.next() && sfCol.compareToIgnoreCase(ssCol) != 0) {
					ssCol = ssRowSet.getString("COLUMN_NAME").toUpperCase();
					
					if(sfCol.compareToIgnoreCase(ssCol) == 0) {
						output.add(sfCol);
					}
				}
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.toString(),true);
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
			sfCn = SnowflakeTest.getConnection(sfCredsLoc);
			Statement statement = sfCn.createStatement();
			statement.executeUpdate("CREATE DATABASE " + srcDatabase);
			statement.close();
			sfCn.close();
			
			sfCn = SnowflakeTest.getConnection(sfCredsLoc, srcDatabase);
			
			
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
			QLog.log(e.toString(),true);
		}
	}
	
	
	public static void copySqlServerDatabase(java.sql.Connection snowflakeCn, 
			String srcHost, String srcDatabase) throws SQLException {
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcHost, null , srcDatabase);
		
	}
	

	/***
	 * Copies schemas from a source SQL Server host to a Snowflake instance 
	 * 
	 * @param An established Snowflake connection to execute Snowflake SQL statements (java.sql.Connection)
	 * @param Source SQL Server host name (String)
	 * @param Source SQL Server database of the previously specified host name (String)
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
			QLog.log(e.toString(),true);
		}
	}
	
	
	/***
	 * Copies schemas from a source SQL Server host to a Snowflake instance by performing a syntax conversion
	 * Creates a series of create statements
	 * Currently supported: Column Names, Data Types, Default Values, Nullable, and Primary Keys 
	 * 
	 * TODO: Method should be refactored moving from test case to 
	 * 
	 * @param Source SQL Server host name (String)
	 * @param Source SQL Server database of the previously specified host name (String)
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
				
				String createTable = sqlCreateSfTable(
						srcCnString.getHostName(),
						srcCnString.getDatabaseName(),
						tables.getString("TABLE_SCHEMA"),
						tables.getString("TABLE_NAME"));

				statement.execute(createTable);
			}
			
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	}
	
	
	
	//Various Connections Methods
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	/***
	 * snowflake_creds.json
	 * <start file>
	 	{
			"user":"user", 
			"password":"secret", 
			"account":"accountNumber", 
			"db":"optional database", 
			"schema":"optional shema"
			"cnstring":"jdbc:snowflake://snowflake_url"
		}
	 * <end file>
	 */
	
	
	/***
	 * Creates and returns a Snowflake connection based on JSON credential path.
	 * 
	 * @param credentialPath String: (ie, "C:\\temp\\creds.json")
	 * @return Snowflake connected java.sql.Connection:
	 * @throws SQLException
	 */
	public static Connection getConnection(String credentialPath)  {
		java.sql.Connection output = null;
		
	    try {
	    	Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex){
	    	System.err.println("Driver not found");
	    	QLog.log("Driver not found");
	    }
	    
	    //use JSON object to pull sensitive information instead of hardcoding
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(credentialPath));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	    
	    // build connection properties
	    Properties properties = new Properties();
	    properties.put("user", jsonObj.get("user"));     // replace "" with your username
	    properties.put("password", jsonObj.get("password")); // replace "" with your password
	    properties.put("account", jsonObj.get("account"));  // replace "" with your account name

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    // use the default connection string if it is not set in environment
	    if(connectStr == null) {
	    	// replace accountName with your account name
	    	connectStr = (String)jsonObj.get("cnstring"); 
	    }
	    
	    try {
			output = DriverManager.getConnection(connectStr, properties);
		} catch (SQLException e) {

			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	    
	    return output;
	}
	
	/***
	 * 
	 * @param credentialPath
	 * @param database
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection(String credentialPath, String database) {
		java.sql.Connection output = null;
		
		try {
	    	Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex){
	    	System.err.println("Driver not found");
	    	QLog.log("Driver not found");
	    }
	    
	    //use JSON object to pull sensitive information instead of hardcoding
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(credentialPath));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	    
	    // build connection properties
	    Properties properties = new Properties();
	    properties.put("user", jsonObj.get("user"));     // replace "" with your username
	    properties.put("password", jsonObj.get("password")); // replace "" with your password
	    properties.put("account", jsonObj.get("account"));  // replace "" with your account name
	    properties.put("db", database);       // replace "" with target database name
	    //properties.put("tracing", "on");

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    // use the default connection string if it is not set in environment
	    if(connectStr == null) {
	    	// replace accountName with your account name
	    	connectStr = (String)jsonObj.get("cnstring"); 
	    }
	    
	    try {
			output = DriverManager.getConnection(connectStr, properties);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	    
	    return output;
	}
	
	/***
	 * 
	 * @param credentialPath
	 * @param database
	 * @param schema
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection(String credentialPath, String database, String schema) {
		java.sql.Connection output = null;
		
		try {
	    	Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex){
	    	System.err.println("Driver not found");
	    }
	    
	    //use JSON object to pull sensitive information instead of hardcoding
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(credentialPath));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			e.printStackTrace();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    // build connection properties
	    Properties properties = new Properties();
	    properties.put("user", jsonObj.get("user"));     // replace "" with your username
	    properties.put("password", jsonObj.get("password")); // replace "" with your password
	    properties.put("account", jsonObj.get("account"));  // replace "" with your account name
	    properties.put("db", database);       // replace "" with target database name
	    properties.put("schema", schema);   // replace "" with target schema name
	    
	    //properties.put("tracing", "on");

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    // use the default connection string if it is not set in environment
	    if(connectStr == null) {
	    	// replace accountName with your account name
	    	connectStr = (String)jsonObj.get("cnstring"); 
	    }
	    
	    try {
			output = DriverManager.getConnection(connectStr, properties);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	    
	    return output;
	}
	

	//Dynamically Generated SQL Statements
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	
	/***
	 * Snowflake database schema information
	 * 
	 * Fields:
	 * TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE,
	 * DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX,
	 * NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_TYPE, INTERVAL_PRECISION, CHARACTER_SET_CATALOG,
	 * CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME,
	 * DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, SCOPE_CATALOG, SCOPE_SCHEMA,
	 * SCOPE_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER, IS_SELF_REFERENCING, IS_IDENTITY, IDENTITY_GENERATION,
	 * IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE, COMMENT
	 * 
	 * @param database
	 * @return sqlString
	 */
	static String sqlSfDatabaseAllInformationShema(String database) {
		String output = "SELECT * FROM \"" + database.toUpperCase() + "\".INFORMATION_SCHEMA.COLUMNS "+
						"ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";
		return output;
	}
	
	
	static String sqlSfDatabaseTableInformationShema(String database, String schema, String table) {
		String output = "SELECT * FROM \"" + database.toUpperCase() + "\".INFORMATION_SCHEMA.COLUMNS "
						+ "WHERE TABLE_SCHEMA = '" + schema.toUpperCase() 
						+ "' AND TABLE_NAME = '" + table.toUpperCase() + "' "
						+ "ORDER BY ORDINAL_POSITION";
		return output;
	}

	
	/***
	 * 
	 * @param database
	 * @param schema
	 * @param table
	 * @param col
	 * @return
	 */
	public static String sqlSfMaxValue(String database, String schema, String table, String col) {
		String output = null;
		
		if(database != null && schema != null && table != null)  {
			
			output = "SELECT MAX(\"" + col.toUpperCase() + "\") MaxValue FROM \"" 
				   + database.toUpperCase() + "\".\""
				   + schema.toUpperCase() + "\".\""
				   + table.toUpperCase() + "\"";

		}
		
		return output;
	}
	
	
	/***
	 * 
	 * @param value
	 * @param datatypeCat
	 * @return
	 */
	public static String sfSqlValuePrep(String value, String datatypeCat ) {
		String output = "";
		
		//DATA_TYPE_CAT: TEXT, NUMERIC, DATETIME, OTHER
		if(value == null) output = "null";
		else if (datatypeCat.equalsIgnoreCase("TEXT")) output = "$$" + value + "$$";
		else if (datatypeCat.equalsIgnoreCase("DATETIME")) output = "'" + value + "'";
		else if (datatypeCat.equalsIgnoreCase("NUMERIC")) output = value;
		else if (datatypeCat.equalsIgnoreCase("OTHER")) output = "null";
		
		return output; 
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
	 * @param srcSqlHost String:
	 * @param srcDatabse String:
	 * @param srcSchema String: 
	 * @param srcTable String:
	 * @return
	 */
	public static String sqlCreateSfTable(String srcSqlHost, String srcDatabase, String srcSchema, String srcTable) {
		
		//make connection to source SQL Server
		SqlServerCnString srcCnString = new SqlServerCnString();								
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , srcDatabase);
		
		//get column of current table, ordered by ordinal position
		RowSet cols = SqlServerDbScour.getSrcInformationSchema(
							srcCnString.getCnString(), 
							srcCnString.getDatabaseName(), 
							srcSchema, 
							srcTable);
					
		//check for unconventional characters in table name: if true, wrap with double quotes for Snowflake literals
		if(srcTable.contains("[") == true || 
		   srcTable.contains("]") == true ||	
		   srcTable.contains(".") == true || 
		   srcTable.contains(" ") == true) {
			
			srcTable = "\"" + srcTable + "\"";
			
			
		}
		
		//create statement
		StringBuilder createStatement = new StringBuilder();
		createStatement.append("CREATE TABLE " + srcSchema + "." + srcTable + " (\n");
		
		//used collect primary keys along the way (getSrcInfromationSchema is sorted by ordinal)
		ArrayList<String> primaryKeys = new ArrayList<String>();
		
		//ensure 1 column per ordinal position.  
		int ordinal = 0, prevOrdinal = 0;
						
		
		int colCnt = 0;
		
		try {
			
			//iterate through columns
			while(cols.next()) {
				
				ordinal = cols.getInt("ORDINAL_POSITION");
				
				//ensures column names won't be repeated if a constraint causes column to appear more than once
				if(ordinal != prevOrdinal ) {
					
					colCnt++;
				
					//add comma to previous and newline to previous field, skip first line
					if(colCnt > 1) {
						createStatement.append(",\n");
					}
					
					//column name
					String colName = cols.getString("COLUMN_NAME");
					//check if colname doesnt have special characters or reserved word
					if(colName.contains(" ") == true ||
					   colName.toLowerCase().equals("row")) {
						colName = "\"" + colName + "\"";
					}
					createStatement.append("\t" + colName);
					
					//convert data type to Snowflake compatible
					String datatype = convertSsToSfDataType(
							cols.getString("DATA_TYPE_CAT"),
							cols.getString("DATA_TYPE"),
							cols.getInt("NUMERIC_PRECISION"),
							cols.getInt("NUMERIC_SCALE"),
							cols.getInt("CHARACTER_MAXIMUM_LENGTH"),
							cols.getString("COLUMN_DEFAULT"));
					createStatement.append("\t" + datatype);
					
					//check default
					if(cols.getString("COLUMN_DEFAULT") != null) {
						String colDefault = cols.getString("COLUMN_DEFAULT");
						
						//make sure there aren't any SQL Server specific commands going across
						if(colDefault.toLowerCase().contains("newsequentialid()") == false &&
						   colDefault.toLowerCase().contains("user_name()") == false &&
						   colDefault.toLowerCase().contains("app_name()") == false	) {
							createStatement.append("\tDEFAULT " + colDefault);
						}
					}
					
					//is field nullable?
					String nullable = isNullable(cols.getString("IS_NULLABLE"));
					createStatement.append("\t" + nullable);
					
					//if constraint is not null, it is likely a primary key?
					if(cols.getString("CONSTRAINT_NAME") != null &&
					   cols.getString("CONSTRAINT_NAME").toUpperCase().contains("PK")) {
						primaryKeys.add(cols.getString("COLUMN_NAME"));
					}
				}
				
				prevOrdinal = cols.getInt("ORDINAL_POSITION");
			}
		} catch (SQLException e) {

			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
			
		//add primary keys
		if(primaryKeys.size() > 0) {
			createStatement.append(",\n");
			createStatement.append("\tPRIMARY KEY(");
			for(int i = 0; primaryKeys.size() > i; i++) {
				if(i > 0) createStatement.append(",");
				createStatement.append(primaryKeys.get(i));
			}
			createStatement.append(")\n");
		}
		
		//cap off create statement
		createStatement.append(")\n");
		
		return createStatement.toString();
	}
	
	

	/***
	 * 
	 * @param isNullable
	 * @return
	 */
	static String isNullable(String isNullable) {
		String output = "";
		
		if(isNullable.equalsIgnoreCase("NO")) output = "NOT NULL";
		else if(isNullable.equalsIgnoreCase("YES")) output = "NULL";
		else output = "NULL";
		
		return output;
	}
	
	/***
	 * 	Converts SQL Server data type to Snowflake datatype
	 * 
	 * Data Type Categories (dataTypeCategory)
	 * 	DATE	smalldatetime,date,datetime,datetime2,time
	 *	NUMERIC	smallint,money,int,numeric,bigint,decimal,bit,float,tinyint,real
	 *	TEXT	varchar,char,nvarchar,nchar,uniqueidentifier,text
	 *	OTHER	varbinary
	 *	
	 * @param dataTypeCategory String:
	 * @param dataType String:
	 * @param numericPrecision int:
	 * @param numericScale int:
	 * @param charMaxLength int:
	 * @param colDefault String:
	 * @return
	 */
	static String convertSsToSfDataType(String dataTypeCategory, String dataType, int numericPrecision, int numericScale, 
							int charMaxLength, String colDefault) {

		String output = null;
		switch(dataTypeCategory) {
			case "TEXT":
				if(dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("nvarchar")
					 || dataType.equalsIgnoreCase("text")) {
					
					if(charMaxLength < 1) output = "VARCHAR";
					else output = "VARCHAR(" + charMaxLength + ")";
				}
				if(dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("nchar")) {
					output = "CHAR(" + charMaxLength + ")";
				}
				if(dataType.equalsIgnoreCase("uniqueidentifier")) {
					output = "VARCHAR(" + 50 + ")";
				}
				break;
			case "NUMERIC":
				if(    dataType.equalsIgnoreCase("bigint")  || dataType.equalsIgnoreCase("decimal")
					|| dataType.equalsIgnoreCase("numeric") || dataType.equalsIgnoreCase("int") 
					|| dataType.equalsIgnoreCase("money")   || dataType.equalsIgnoreCase("smallint")
					|| dataType.equalsIgnoreCase("tinyint") || dataType.equalsIgnoreCase("decimal")
					|| dataType.equalsIgnoreCase("real")) {
						output = "NUMBER(" + numericPrecision + "," + numericScale + ")";
				}
				if(dataType.equalsIgnoreCase("bit")) {
					output = "NUMBER(1,0)";
				}
				if(dataType.equalsIgnoreCase("float")) {
						output = "FLOAT";
				}
				break;
			case "DATETIME":
				if(dataType.equalsIgnoreCase("date")) {
					output = "DATE";
					
					//getdate() in Snowflake will break DATE type, so make DATETIME instead
					if(colDefault != null &&
					   colDefault.toLowerCase().contains("getdate()") == true) output = "DATETIME";
				}
				if(dataType.equalsIgnoreCase("smalldatetime") || dataType.equalsIgnoreCase("datetime")
						|| dataType.equalsIgnoreCase("datetime2")) {
					output = "DATETIME";
				}
				if(dataType.equalsIgnoreCase("time")) {
					output = "TIME";
				}
				break;
			case "OTHER":
				if(dataType.equalsIgnoreCase("varbinary") || dataType.equalsIgnoreCase("binary")) {
					output = "BINARY";
				}
				if(dataType.equalsIgnoreCase("float")) {
					output = "FLOAT";
				}
				break;
		}
		return output;
	}
	
	/***
	 * Create database
	 * @param databaseName
	 * @return
	 */
	public static String sqlSfCreateDatabase(String databaseName) {
		String output =  null;
		output = "CREATE DATABASE " + databaseName;
		return output;
	}
	
	
	
	
	//In development
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/***
	 * TODO: Completer later
	 * Fields: TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION,IS_UPDATABLE
	 * 
	 * 
	 * @param snowflakeCn
	 * @param srcHost
	 * @param srcDatabase
	 * @throws SQLException
	 */
	public static void copySqlServerAllViews(java.sql.Connection snowflakeCn, String srcHost, String srcDatabase) {
		try {
			copySqlServerAllViews(snowflakeCn, srcHost, srcDatabase, null, null, 0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	/***
	 * TODO: Complete later
	 * Fields: TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION,IS_UPDATABLE
	 * 
	 * 
	 * @param snowflakeCn
	 * @param srcHost
	 * @param srcDatabase
	 * @throws SQLException
	 */
	public static void copySqlServerAllViews(java.sql.Connection snowflakeCn, String srcHost, String srcDatabase,
			String srcSchema, String srcTable, Integer recLvl)  throws SQLException {
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcHost, null , srcDatabase);
				
		recLvl++;
		//first run
		if(srcSchema == null && srcTable == null) {
			
			QLog.log("Start recursion");
			//get tables for given database 
			RowSet viewDefs = SqlServerDbScour.getSrcViewDefinitions(
					srcCnString.getCnString(), 			//Source host to get InformationSchema
					srcCnString.getDatabaseName());
			
			while(viewDefs.next()) {
				String viewSchema = viewDefs.getString("TABLE_SCHEMA");
				String viewName = viewDefs.getString("TABLE_NAME");
				
				copySqlServerAllViews(snowflakeCn, srcHost, srcDatabase, viewSchema, viewName, recLvl);
				
			}
			
			
		}
		//recursive run
		else {

			String sql = SqlServerDiscovery.sqlParentObjects(srcDatabase, srcSchema, srcTable, true);
			QLog.log(recLvl + ": " + srcSchema + "." + srcTable + " --> " + srcCnString.getCnString() + ";" +sql);
			
			RowSet parents = SqlServerDbScour.executeSqlRowSet(srcCnString.getCnString(), sql);
			String v1 = srcHost + "." + srcDatabase +  "." + srcSchema + "." + srcTable;
			if(parents.next() == false) {
				//do the thing
				
				
				QLog.log(recLvl + ": Do the thing 1 :" + v1);
								
			} else {
				
				do {
					//recurse again					
					String viewServer = parents.getString("referenced_server_name");
					String viewDatabase = parents.getString("referenced_database_name");
					String viewSchema = parents.getString("referenced_schema_name");
					String viewName = parents.getString("referenced_entity_name");
					
					String v2 =  viewServer+ "," + viewDatabase + "," + viewSchema + "," + viewName;
					
					QLog.log("Lets see the variables: " + v2);	
					
					if(viewServer == null) {
						QLog.log("viewServer is null, using srcHost, " + srcHost);
						viewServer = srcHost;
					}
					
					if(viewDatabase == null) {
						QLog.log("viewDatabase is null, using srcHost, " + srcDatabase);
						viewDatabase = srcDatabase;
					}
					
					
					
					copySqlServerAllViews(snowflakeCn, viewServer, viewDatabase, viewSchema, viewName, recLvl);
					
				}
				while(parents.next() == true);
				
				//do the thing
				QLog.log(recLvl + ": Do the thing 2 :" + v1);
			}
			
				
		}
		
		recLvl--;

	}
	

	/*
	 * TODO: complete later
	 */
	public static void sqlConvertViewDefinition(String sql) {
		/*
		 * ] -> "
		 * [ -> "
		 * TOP 2O -> LIMIT 20
		 * CIELING -> CIEL
		 * STDEV -> STDDEV
		 */
		
	}
		
	
	
	
	//Isolated test cases
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	/***
	 * Attempts to print account table schema as a create statement acceptable snowflake
	 * PASS
	 */
	public static void createAccountTableTest() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity("server", null , "database");
	
		/*
			SELECT T.TABLE_TYPE,T.TABLE_CATALOG,T.TABLE_SCHEMA,T.TABLE_NAME,C.COLUMN_NAME,CU.CONSTRAINT_NAME,
				C.DATA_TYPE,C.ORDINAL_POSITION,C.COLUMN_DEFAULT,C.IS_NULLABLE,C.CHARACTER_MAXIMUM_LENGTH,C.CHARACTER_OCTET_LENGTH,
				C.NUMERIC_PRECISION,C.NUMERIC_PRECISION_RADIX,C.NUMERIC_SCALE,C.DATETIME_PRECISION, 
		 		C.CHARACTER_SET_NAME,C.COLLATION_CATALOG,C.COLLATION_SCHEMA,C.COLLATION_NAME,C.DOMAIN_CATALOG, 
		  		C.DOMAIN_SCHEMA,C.DOMAIN_NAME,C.CHARACTER_SET_CATALOG,C.CHARACTER_SET_SCHEMA,  
		 		CASE WHEN DATA_TYPE IN ('varchar','nvarchar','text','char','nchar','ntext','xml','uniqueidentifier') THEN 'TEXT' 
		 			WHEN DATA_TYPE IN ('smallint','int','money','numeric','decimal','bigint','float','real','tinyint','bit') THEN 'NUMERIC' 
		 			WHEN DATA_TYPE IN ('smalldatetime','date','datetime','datetime2','time') THEN 'DATETIME' 
		  			ELSE 'OTHER' 
		 		END DATA_TYPE_CAT  
		 		FROM " + databaseName + ".INFORMATION_SCHEMA.TABLES T  
		 		JOIN " + databaseName + ".INFORMATION_SCHEMA.COLUMNS C ON 
		 			T.TABLE_CATALOG = C.TABLE_CATALOG AND T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME 
		 		LEFT JOIN " + databaseName + ".INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CU ON 
		 		CU.TABLE_CATALOG = C.TABLE_CATALOG AND CU.TABLE_SCHEMA = C.TABLE_SCHEMA AND  
		 			CU.TABLE_NAME = C.TABLE_NAME AND CU.COLUMN_NAME = C.COLUMN_NAME
		*/
		
		RowSet infSchema = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(), 		//Source database to get InformationSchema
				true);								//Grab only user tables
		try {
			String tableName = "";
			int line = 0;
			
			ArrayList<String> pks = new ArrayList<String>();
			
			while(infSchema.next()) {
				String str = new String();
				
				tableName = infSchema.getString("TABLE_SCHEMA") + "." + infSchema.getString("TABLE_NAME");
				
				if(tableName.compareTo("dbo.ACCOUNT") ==0) {
					
					line++;
					if(line == 1) {
						str = "CREATE TABLE " + tableName+ " (";
						QLog.log(str);
					}
					
					//TODO: why was this here?
					if(line >= 2) {
						QLog.log(",");
					}
					
					if(infSchema.getString("CONSTRAINT_NAME") != null) pks.add(infSchema.getString("COLUMN_NAME"));
					
					//pass over relevant SQL Server schema info to return a Snowflake friendly datatype
					String datatype = convertSsToSfDataType(
										 infSchema.getString("DATA_TYPE_CAT"),
										 infSchema.getString("DATA_TYPE"),
										 infSchema.getInt("NUMERIC_PRECISION"),
										 infSchema.getInt("NUMERIC_SCALE"),
										 infSchema.getInt("CHARACTER_MAXIMUM_LENGTH"),
										 infSchema.getString("COLUMN_DEFAULT") );
					
					String nullable = isNullable(infSchema.getString("IS_NULLABLE"));
					
					str = "\t" + infSchema.getString("COLUMN_NAME") + 
						  "\t" + datatype +
						  "\t" + nullable;

					QLog.log(str);
				}
			}
			if(pks.size() > 0) {
				
				
				String pk = "\tPRIMARY KEY(";
				for(int i = 0; i < pks.size(); i++) {
					if(i > 0) pk = pk + ",";
					pk = pk + pks.get(i);
				}
				pk = pk + ")";
				
				QLog.log(pk);
				
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
		
	}
	
	
		
	/***
	 * 
	 * @return
	 * @throws SQLException
	 */
	private static Connection getConnectionTest() throws SQLException {
	    try  {
	    	Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex) {
	    	System.err.println("Driver not found");
	    	QLog.log(ex.toString(),true);
	    }
	    
	    //use JSON object to pull sensitive information instead of hardcoding
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of("H:\\snowflake_creds.json"));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
	    
		/* JSON Example
			{
				"user":"username", 
				"password":"secret", 
				"account":"abc123", 
				"db":"databaseName", 
				"schema":"dbo"
			}
		 */
		
	    // build connection properties
	    Properties properties = new Properties();
	    properties.put("user", jsonObj.get("user"));     // replace "" with your username
	    properties.put("password", jsonObj.get("password")); // replace "" with your password
	    properties.put("account", jsonObj.get("account"));  // replace "" with your account name
	    properties.put("db", jsonObj.get("db"));       // replace "" with target database name
	    properties.put("schema", jsonObj.get("schema"));   // replace "" with target schema name
	    //properties.put("tracing", "on");

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    
	    // use the default connection string if it is not set in environment
	    if(connectStr == null) {
	    	// replace accountName with your account name
	    	connectStr = (String)jsonObj.get("cnstring"); 
	    }
	    
	    return DriverManager.getConnection(connectStr, properties); 
	}
	
	
	
	
	
	
	/****
	 * Snowflake demo script pulled from website
	 * https://docs.snowflake.com/en/user-guide/jdbc-configure.html
	 * @throws Exception
	 */
	public static void snowflakeDriverTest() throws Exception {
	    // get connection
	    System.out.println("Create Snowflake JDBC connection");
	    Connection connection = getConnectionTest();
	    System.out.println("Done creating JDBC connection");
	    
	    // create statement
	    System.out.println("Create JDBC statement");
	    Statement statement = connection.createStatement();
	    System.out.println("Done creating JDBC statement");
	    
	    // create a table
	    System.out.println("Create demo table");
	    statement.executeUpdate("create or replace table demo(C1 STRING)");
	    //statement.close();
	    System.out.println("Done creating demo table");
	    
	    // insert a row
	    System.out.println("Insert 'hello world'");
	    statement.executeUpdate("insert into demo values ('hello world')");
	    System.out.println("Done inserting 'hello world'");
	    
	    // query the data
	    System.out.println("Query demo");
	    ResultSet resultSet = statement.executeQuery("SELECT * FROM demo");
	    System.out.println("Metadata:");
	    System.out.println("================================");
	    
	    // fetch metadata
	    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
	    System.out.println("Number of columns=" +       resultSetMetaData.getColumnCount());
	    for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++)
	    {
	    	System.out.println("Column " + colIdx + ": type=" + resultSetMetaData.getColumnTypeName(colIdx+1));
	    }
	    
	    // fetch data
	    System.out.println("nData:");
	    System.out.println("================================");
	    int rowIdx = 0;
	    while(resultSet.next())
	    {
	    	System.out.println("row " + rowIdx + ", column 0: " + resultSet.getString(1));
	    }
	    statement.close();
	}
	
	
}
