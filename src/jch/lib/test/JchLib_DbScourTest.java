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
import jch.lib.db.sqlserver.*;

public class JchLib_DbScourTest {
	JchLib_DbScourTest(){
		//constructor

		
	}
	
	/***
	 * 
	 */
	public static void testDbScourCreateCnStrings() {
		SqlServerCnString cn = new SqlServerCnString();
		
		//hostname:port, sql server instance name, database name
		System.out.println(cn.setCnStringIntegratedSecurity("vm-devanalytics:1433", null , "dev01"));
		if(SqlServerDbScour.createDbScourOjbects(cn.getCnString())) {
			System.out.println("Success!");
		}
		else {
			System.out.println("Failed!");
		}
	}
	
	
	/**
	 * Test the creation of destination tables and views.
	 */
	public static void testDbSourCreateObjects() {
		
		String cnString = "jdbc:sqlserver://vm-devanalytics;databaseName=dev01;integratedSecurity=true";
		if(SqlServerDbScour.createDbScourOjbects(cnString)) {
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
            String sqlAllTables = SqlServerDiscovery.sqlAllTableViewColumns("dev01");
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
        
        
        System.out.println(SqlServerDbScour.sqlPrint(SqlServerDbScour.sqlCreateTableInformationSchema()));
    }
	
	
	/**
		SELECT  sch.name AS schName,
                 lst.type AS tblType,
                 lst.name AS tblName, 
                 lsc.name AS colName,
                 lsc.is_identity AS isIdentity,
                 lsc.is_nullable AS isNullable,
                 typ.name AS typeName,
                 lsc.precision AS precision,
                 lsc.max_length AS maxLength 
         FROM (
               SELECT name, object_id, principal_id, schema_id, parent_object_id, type, type_desc
               FROM master.sys.tables
               UNION ALL
               SELECT name, object_id, principal_id, schema_id, parent_object_id, type, type_desc
               FROM master.sys.views
               ) AS lst JOIN [" & svrName & "].[" & dbName & "].[sys].[columns] lsc ON lst.OBJECT_ID=lsc.object_id
             LEFT JOIN [" & svrName & "].[" & dbName & "].[sys].[types] AS typ ON lsc.system_type_id = typ.system_type_id
             LEFT JOIN [" & svrName & "].[" & dbName & "].[sys].[schemas] AS sch ON sch.schema_id = lst.schema_id
             WHERE typ.name <> 'sysname'
	 */
}
