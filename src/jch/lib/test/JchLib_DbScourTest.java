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
import jch.lib.db.sqlserver.*;

public class JchLib_DbScourTest {
	JchLib_DbScourTest(){
		//constructor

		
	}
	
	
	public static void search(String searchTerm, String dataTypeCategory) {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("VM-TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
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
		
		srcCnString.setCnStringIntegratedSecurity("VM-TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		String searchTerm = "Jerry";
		String dataTypeCategory = "TEXT";
		
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
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		//grab column information from source database
		//source connection string, source database, source schema, include views along with tables
		RowSet infSchema = dbsSource.getSrcInformationSchema(
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
		System.out.println(destCn.setCnStringIntegratedSecurity("vm-devanalytics:1433", null , "dev01"));
		
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
		
		String cnString = "jdbc:sqlserver://vm-devanalytics;databaseName=dev01;integratedSecurity=true";
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
        	String dbURL = "jdbc:sqlserver://vm-devanalytics;integratedSecurity=true";
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
