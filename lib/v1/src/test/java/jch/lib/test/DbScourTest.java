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

import jch.lib.log.QLog;

import java.util.Scanner;


public class DbScourTest {
	DbScourTest(){
		//constructor		
	}

	
	
	
	
	public static void sqlSearch() {
		QLog.log("Starting Search:");
		
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("server", null , "db");
		destCnString.setCnStringIntegratedSecurity("vm-sever", null , "db");
		
		
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
		srcCnString.setCnStringIntegratedSecurity("server", null , "db");
		destCnString.setCnStringIntegratedSecurity("vm-server", null , "db");
		
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
	
	
	public static void dbsColumnStats() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("server", null , "db");
		destCnString.setCnStringIntegratedSecurity("vm-server", null , "db");
		
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
	
	public static void dbsColumnStats() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();

		srcCnString.setCnStringIntegratedSecurity("server", null , "db");
		destCnString.setCnStringIntegratedSecurity("vm-server", null , "db");

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

	

}
