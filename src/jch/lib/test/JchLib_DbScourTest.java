package jch.lib.test;

/***
 * Used to connect to, search, and gather stats about a database.  Mainly used to move data or
 * reverse-engineer a database.
 * 
 * Primary targeted technologies
 * 	SQL-Server
 * 	MySQL
 * 	HyperSQL (HSQLDB)
 * 
 * @author harrisonc
 *
 */



import java.sql.*;
import javax.sql.*;

import jch.lib.common.QLog;
import jch.lib.db.sqlserver.*;

import java.util.Scanner;


public class JchLib_DbScourTest {
	JchLib_DbScourTest(){
		//constructor		
	}

	
	
	
	
	public static void cfsSearch() {
		QLog.log("Starting Search:");
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("arcu", null , "CFSConnectors");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		
		QLog.log("Source: " + srcCnString.getCnString());
		QLog.log("Destination: " + destCnString.getCnString());
		
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		String searchTerm = "";
		String dataTypeCategory = "TEXT";
		
		//String searchTerm = "20";
		//String dataTypeCategory = "NUMERIC";
		
		QLog.log("Search Term: " + searchTerm);
		QLog.log("Data Type Catagory: " + dataTypeCategory);
		
		
		QLog.log("Running dbsDestination.getDestVwTblStats");
		
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		QLog.log("Done!");
		
		try {
			
			while(tblStats.next()) {
				System.out.println(tblStats.getString("TableSchema") + "." + tblStats.getString("TableName"));
			}
			tblStats.beforeFirst();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		QLog.log("Press Key to continue:");
		Scanner myObj = new Scanner(System.in);
		String getline = myObj.nextLine();
		
		
		QLog.log("Running dbsDestination.insertColSearchResults");
		dbsDestination.insertColSearchResults(
				searchTerm,
				dataTypeCategory,
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//The collection of tables to search through
			
	}
	
	public static void cfsSchema() {
		
		QLog.log("Getting meta data...");
		
		//instantiated jdbc connections string generators
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		//store source and destination host information to generate jdbc connection string
		srcCnString.setCnStringIntegratedSecurity("arcu", null , "CFSConnectors");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		//SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//grab column information from source database
		//source connection string, source database, source schema, include views along with tables
		RowSet infSchema = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(), 		//Source database to get InformationSchema
				false);								//Grab only user tables
		
		//updates destination table with information schema based on source connection string
		dbsDestination.updateDestInformationSchema(
				srcCnString.getCnString(), 
				destCnString.getCnString(), 
				destCnString.getDatabaseName(), 
				"dbo", 
				infSchema);
	}
	
	
	public static void cfsColumnStats() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("arcu", null , "CFSConnectors");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//Get a list of tables as each table will get its own thread up to the thread pool size
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		
		dbsDestination.updateDestinationColStats(
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//used to produce each thread
		
		
	}
	
	public static void imageCenterColumnStats() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("dc1sql.firstmarkcu.org", null , "ImageCenter");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//Get a list of tables as each table will get its own thread up to the thread pool size
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		
		dbsDestination.updateDestinationColStats(
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//used to produce each thread
		
	}
	
	public static void imageCenterSchema() {
		
		QLog.log("Getting meta data...");
		
		//instantiated jdbc connections string generators
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		//store source and destination host information to generate jdbc connection string
		srcCnString.setCnStringIntegratedSecurity("dc1sql.firstmarkcu.org", null , "ImageCenter");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		//SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//grab column information from source database
		//source connection string, source database, source schema, include views along with tables
		RowSet infSchema = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(), 		//Source database to get InformationSchema
				false);								//Grab only user tables
		
		//updates destination table with information schema based on source connection string
		dbsDestination.updateDestInformationSchema(
				srcCnString.getCnString(), 
				destCnString.getCnString(), 
				destCnString.getDatabaseName(), 
				"dbo", 
				infSchema);
	}
	
	
	
	public static void imageCenterSearch() {
		QLog.log("Starting Search:");
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("dc1sql.firstmarkcu.org", null , "CUAnalyticsServer");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		
		QLog.log("Source: " + srcCnString.getCnString());
		QLog.log("Destination: " + destCnString.getCnString());
		
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		String searchTerm = "00028df0-c2d3-4043-a7a1-e3a373117f67";
		String dataTypeCategory = "TEXT";
		
		//String searchTerm = "20";
		//String dataTypeCategory = "NUMERIC";
		
		QLog.log("Search Term: " + searchTerm);
		QLog.log("Data Type Catagory: " + dataTypeCategory);
		
		
		QLog.log("Running dbsDestination.getDestVwTblStats");
		
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		QLog.log("Done!");
		
		try {
			
			while(tblStats.next()) {
				System.out.println(tblStats.getString("TableSchema") + "." + tblStats.getString("TableName"));
			}
			tblStats.beforeFirst();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		QLog.log("Press Key to continue:");
		Scanner myObj = new Scanner(System.in);
		String getline = myObj.nextLine();
		
		
		QLog.log("Running dbsDestination.insertColSearchResults");
		dbsDestination.insertColSearchResults(
				searchTerm,
				dataTypeCategory,
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//The collection of tables to search through
			
	}
	
	
	
	/*
	 spotfireSchema()
	 spotfireSearch() 
	 
	 */
	public static void spotfireSchema() {
		
		QLog.log("Getting meta data...");
		
		//instantiated jdbc connections string generators
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		//store source and destination host information to generate jdbc connection string
		srcCnString.setCnStringIntegratedSecurity("dc1sql.firstmarkcu.org", null , "CUAnalyticsServer");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//grab column information from source database
		//source connection string, source database, source schema, include views along with tables
		RowSet infSchema = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(), 		//Source database to get InformationSchema
				false);								//Grab only user tables
		
		//updates destination table with information schema based on source connection string
		dbsDestination.updateDestInformationSchema(
				srcCnString.getCnString(), 
				destCnString.getCnString(), 
				destCnString.getDatabaseName(), 
				"dbo", 
				infSchema);
	}
	
	
	
	public static void spotfireSearch() {
		QLog.log("Starting Search:");
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("dc1sql.firstmarkcu.org", null , "CUAnalyticsServer");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		
		QLog.log("Source: " + srcCnString.getCnString());
		QLog.log("Destination: " + destCnString.getCnString());
		
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		String searchTerm = "ldap";
		String dataTypeCategory = "TEXT";
		
		//String searchTerm = "20";
		//String dataTypeCategory = "NUMERIC";
		
		QLog.log("Search Term: " + searchTerm);
		QLog.log("Data Type Catagory: " + dataTypeCategory);
		
		
		QLog.log("Running dbsDestination.getDestVwTblStats");
		
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		QLog.log("Done!");
		
		try {
			
			while(tblStats.next()) {
				System.out.println(tblStats.getString("TableSchema") + "." + tblStats.getString("TableName"));
			}
			tblStats.beforeFirst();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		QLog.log("Press Key to continue:");
		Scanner myObj = new Scanner(System.in);
		String getline = myObj.nextLine();
		
		
		QLog.log("Running dbsDestination.insertColSearchResults");
		dbsDestination.insertColSearchResults(
				searchTerm,
				dataTypeCategory,
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//The collection of tables to search through
			
	}
	
	
	
	public static void mlSchema() {
		
		//instantiated jdbc connections string generators
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		//store source and destination host information to generate jdbc connection string
		srcCnString.setCnStringIntegratedSecurity("arcu.firstmarkcu.org", null , "CFSConnectors");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//grab column information from source database
		//source connection string, source database, source schema, include views along with tables
		RowSet infSchema = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(), 		//Source database to get InformationSchema
				false);								//Grab only user tables
		
		//updates destination table with information schema based on source connection string
		dbsDestination.updateDestInformationSchema(
				srcCnString.getCnString(), 
				destCnString.getCnString(), 
				destCnString.getDatabaseName(), 
				"dbo", 
				infSchema);
	}
	
	
	public static void mlSearch() {
		QLog.log("Starting Search:");
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("arcu.firstmarkcu.org", null , "CFSConnectors");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		
		QLog.log("Source: " + srcCnString.getCnString());
		QLog.log("Destination: " + destCnString.getCnString());
		
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		String searchTerm = "GATEWAY";
		String dataTypeCategory = "TEXT";
		
		//String searchTerm = "20";
		//String dataTypeCategory = "NUMERIC";
		
		QLog.log("Search Term: " + searchTerm);
		QLog.log("Data Type Catagory: " + dataTypeCategory);
		
		
		QLog.log("Running dbsDestination.getDestVwTblStats");
		
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		QLog.log("Done!");
		
		try {
			
			while(tblStats.next()) {
				System.out.println(tblStats.getString("TableSchema") + "." + tblStats.getString("TableName"));
			}
			tblStats.beforeFirst();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		QLog.log("Press Key to continue:");
		Scanner myObj = new Scanner(System.in);
		String getline = myObj.nextLine();
		
		
		QLog.log("Running dbsDestination.insertColSearchResults");
		dbsDestination.insertColSearchResults(
				searchTerm,
				dataTypeCategory,
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//The collection of tables to search through
			
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	
	//searches databa
	public static void akcelerantSearch() {
		//akcelerantSearch("$1879.71","TEXT");
		//akcelerantSearch("112336","NUMERIC");
		//akcelerantSearch("6729","NUMERIC");
		akcelerantSearch("83200.doc","TEXT");
		
		
	}
	
	public static void akcelerantSearch(String searchTerm, String dataTypeCategory) {
		//instantiate connection string generator objects
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		//set hostname, sql server instances name, and database
		srcCnString.setCnStringIntegratedSecurity("VM-TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("VM-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//String searchTerm = "20";
		//String dataTypeCategory = "NUMERIC";
		
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		
		
		dbsDestination.insertColSearchResults(
				searchTerm,
				dataTypeCategory,
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//The collection of tables to search through
			
	}
	
	
	/***
	 * Search values and store results
	 * PASSED
	 */
	public static void testSearchText() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		String searchTerm = "Tom";
		String dataTypeCategory = "TEXT";
		
		//String searchTerm = "20";
		//String dataTypeCategory = "NUMERIC";
		
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		
		try {
			
			while(tblStats.next()) {
				System.out.println(tblStats.getString("TableSchema") + "." + tblStats.getString("TableName"));
			}
			tblStats.beforeFirst();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Scanner myObj = new Scanner(System.in);
		String getline = myObj.nextLine();
		
		dbsDestination.insertColSearchResults(
				searchTerm,
				dataTypeCategory,
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//The collection of tables to search through
			
	}
	
	/***
	 * Gets basic column statistics and updates ColStats table
	 * PASSED
	 */
	public static void testUpdateColumnStats() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("VM-TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//Get a list of tables as each table will get its own thread up to the thread pool size
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		
		dbsDestination.updateDestinationColStats(
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//used to produce each thread
		
	}
	
	
	/***
	 * Gets record counts of tables and updates TblStats table
	 * PASSED
	 */
	public static void testUpdateRecordCounts() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("VM-TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//Get a list of tables as each table will get its own thread up to the thread pool size
		RowSet tblStats = dbsDestination.getDestVwTblStats(
				srcCnString.getCnString(),		//Grab table list based source connection string (filter value)
				destCnString.getCnString(), 	//Used to make connection where vwTblStats resides
				destCnString.getDatabaseName(), //Used to make connection where vwTblStats resides
				"dbo");							//Used to make connection where vwTblStats resides
		
		dbsDestination.updateDestinationTableStats(
				srcCnString.getCnString(), 		//
				destCnString.getCnString(), 	//
				destCnString.getDatabaseName(), //
				"dbo", 							//
				tblStats);						//
		
		
	}
	
	/***
	 * Connects to source database and populates destination information schema table
	 * PASSED
	 */
	public static void testGetBasicStats() {
		//instantiated jdbc connections string generators
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		//store source and destination host information to generate jdbc connection string
		srcCnString.setCnStringIntegratedSecurity("VM-TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//grab column information from source database
		//source connection string, source database, source schema, include views along with tables
		RowSet infSchema = SqlServerDbScour.getSrcInformationSchema(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(), 		//Source database to get InformationSchema
				false);								//Grab only user tables
		
		//updates destination table with information schema based on source connection string
		dbsDestination.updateDestInformationSchema(
				srcCnString.getCnString(), 
				destCnString.getCnString(), 
				destCnString.getDatabaseName(), 
				"dbo", 
				infSchema);
		
		
	}
	
	
	/***
	 * Test generation of connection string, drop DbScour objects, and then create DbScour Objects
	 * PASSED
	 */
	public static void testDbScourCreateCnStrings() {
		SqlServerCnString destCn = new SqlServerCnString();
		
		//hostname:port, sql server instance name, database name
		System.out.println("Using the following connection string:");
		System.out.println(destCn.setCnStringIntegratedSecurity("devanalytics:1433", null , "dev01"));
		
		System.out.println("Dropping all objects (will fail if they do not exist):");
		if(SqlServerDbScour.dropDbScourOjbects(destCn.getCnString(),destCn.getDatabaseName(),"dbo")) 
			{System.out.println("Success!");}
		else {System.out.println("Failed!");}		

		System.out.println("Creating all objects:");
		if(SqlServerDbScour.createDbScourOjbects(destCn.getCnString(),destCn.getDatabaseName(),"dbo")) 
			{System.out.println("Success!");}
		else {System.out.println("Failed!");}	
	}
	
	
	/**
	 * Test the creation of destination tables and views.
	 */
	public static void testDbSourCreateObjects() {
		
		String cnString = "jdbc:sqlserver://devanalytics;databaseName=dev01;integratedSecurity=true";
		if(SqlServerDbScour.createDbScourOjbects(cnString,"dev01","dbo")) {
			System.out.println("Success!");
		}
		else {
			System.out.println("Failed!");
		}
	}
	
	/***
	 * just a general connection test
	 */
	public static void testSQL(){
		  
        Connection conn = null;
 
        try {
        	/*
	            String dbURL = "jdbc:sqlserver://host;authenticationScheme=NTLM;integratedSecurity=true";
	            String user = "sa";
	            String pass = "secret";
            */
        	
        	//Windows
        	//be sure to copy mssql-jdbc_auth-9.4.0.x64.dll to %PATH% directory
        	//cmd: echo %PATH%
        	String dbURL = "jdbc:sqlserver://devanalytics;integratedSecurity=true";
            conn = DriverManager.getConnection(dbURL);
            if (conn != null) {
                DatabaseMetaData dm = (DatabaseMetaData) conn.getMetaData();
                System.out.println("Driver name: " + dm.getDriverName());
                System.out.println("Driver version: " + dm.getDriverVersion());
                System.out.println("Product name: " + dm.getDatabaseProductName());
                System.out.println("Product version: " + dm.getDatabaseProductVersion());
            }
            
            //String sqlAllDatabase = SqlServerDiscovery.sqlAllUserDatabases();
            String sqlAllTables = SqlServerDiscovery.sqlDbTableViewColumns("dev01");
            System.out.println("Output:");
            
            Statement sta = conn.createStatement();
            ResultSet res = sta.executeQuery(sqlAllTables);
            
            while (res.next()) {
            	//String databaseName = res.getString("name");
            	//System.out.println("\t" + databaseName);
            	String colName = res.getString("COLUMN_NAME");
            	System.out.println(colName);
            	
            }
 
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        
        
        //System.out.println(SqlServerDiscovery.sqlPrint(SqlServerDbScour.sqlCreateTableInformationSchema()));
    }
	
}
