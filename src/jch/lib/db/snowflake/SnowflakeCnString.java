package jch.lib.db.snowflake;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jch.lib.common.QLog;

public class SnowflakeCnString {
	

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
	    	QLog.log(ex.toString(),true);
	    	QLog.log(ex,true);
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
			QLog.log(e,true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
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
			QLog.log(e,true);
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
	    	QLog.log(ex,true);
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
			QLog.log(e,true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
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
			QLog.log(e.toString(),true);
			QLog.log(e,true);
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
	    	QLog.log(ex.toString(),true);
	    	QLog.log(ex,true);
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
			QLog.log(e,true);
			
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
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
			QLog.log(e.toString(),true);
			QLog.log(e,true);
		}
	    
	    return output;
	}
	
}
