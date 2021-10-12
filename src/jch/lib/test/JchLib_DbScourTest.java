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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JchLib_DbScourTest {
	JchLib_DbScourTest(){
		//constructor
		
	}
	
	
	public static void testSQL(){
		  
        Connection conn = null;
 
        try {
        	/*
            String dbURL = "jdbc:sqlserver://gcarcu080119;authenticationScheme=NTLM;integratedSecurity=true;domain=teachers";
            String user = "sa";
            String pass = "secret";
            */
        	
        	//Windows
        	//be sure to copy mssql-jdbc_auth-9.4.0.x64.dll to %PATH% directory
        	//cmd: echo %PATH%
        	String dbURL = "jdbc:sqlserver://gcarcu080119;integratedSecurity=true";
            conn = DriverManager.getConnection(dbURL);
            if (conn != null) {
                DatabaseMetaData dm = (DatabaseMetaData) conn.getMetaData();
                System.out.println("Driver name: " + dm.getDriverName());
                System.out.println("Driver version: " + dm.getDriverVersion());
                System.out.println("Product name: " + dm.getDatabaseProductName());
                System.out.println("Product version: " + dm.getDatabaseProductVersion());
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
