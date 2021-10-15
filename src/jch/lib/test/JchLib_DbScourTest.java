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
	
	
	public static void testGetBasicStats() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		SqlServerCnString destCnString = new SqlServerCnString();
		
		srcCnString.setCnStringIntegratedSecurity("VM-TEMENOS", null , "Akcelerant");
		destCnString.setCnStringIntegratedSecurity("vm-devanalytics", null , "dev01");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		SqlServerDbScour dbsDestination = new SqlServerDbScour();
		
		RowSet infSchema = dbsSource.getSrcInformationSchema(srcCnString.getCnString(), srcCnString.getDatabaseName());
		
		//updates destination table with information schema based on source connection string
		dbsDestination.updateDestInformationSchema(srcCnString.getCnString(), 
				destCnString.getCnString(), destCnString.getDatabaseName(), "dbo", infSchema);
		
	}
	
	
	/***
	 * Test generation of connection string, drop DbScour objects, and then create DbScour Objects
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
	            String dbURL = "jdbc:sqlserver://gcarcu;authenticationScheme=NTLM;integratedSecurity=true;domain=teachers";
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
