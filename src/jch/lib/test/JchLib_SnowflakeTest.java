package jch.lib.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.RowSet;

import org.json.simple.*;
import org.json.simple.parser.*;

import jch.lib.db.sqlserver.SqlServerCnString;
import jch.lib.db.sqlserver.SqlServerDbScour;

import java.util.ArrayList;

public class JchLib_SnowflakeTest {
	
	/***
	 * host
	 * 	database
	 * 	 schemas
	 *    tables
	 *     columns
	 * 
	 * 
	 * @param host
	 * @param database
	 */
	public static void createAllTablesDatabase(String host, String database) {
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(host, null , database);
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		
		//get tables for given database
		RowSet tables = dbsSource.getSrcTables(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName());
		
		try {
			//loop through tables
			while(tables.next()) {
				StringBuilder createStatement = new StringBuilder();
				
				
				System.out.println(tables.getString("TABLE_NAME"));
				
				//get column of current table
				RowSet cols = dbsSource.getSrcInformationSchema(
						srcCnString.getCnString(), 
						srcCnString.getDatabaseName(), 
						tables.getString("TABLE_SCHEMA"), 
						tables.getString("TABLE_NAME"));
				
				
				
				//create statement
				createStatement.append("CREATE TABLE " + tables.getString("TABLE_SCHEMA") + "." +  tables.getString("TABLE_NAME") + " (\n");
				
				//used collect primary keys along the way
				ArrayList<String> primaryKeys = new ArrayList<String>();
				
				//iterate through columns
				int colCnt = 0;
				while(cols.next()) {
					colCnt++;
					
					//add comma to previous and newline to previous field, skip first line
					if(colCnt > 1) {
						createStatement.append(",\n");
					}
					
					//column name
					createStatement.append("\t" + cols.getString("COLUMN_NAME"));
					
					//convert data type to snowflake compatible
					String datatype = convertDataType(
							cols.getString("DATA_TYPE_CAT"),
							cols.getString("DATA_TYPE"),
							cols.getInt("NUMERIC_PRECISION"),
							cols.getInt("NUMERIC_SCALE"),
							cols.getInt("CHARACTER_MAXIMUM_LENGTH"));
					createStatement.append("\t" + datatype);
					
					//is field nullable
					String nullable = isNullable(cols.getString("IS_NULLABLE"));
					createStatement.append("\t" + nullable);
					
					//if constraint is not null, it is likely a primary key
					if(cols.getString("CONSTRAINT_NAME") != null) {
						primaryKeys.add(cols.getString("COLUMN_NAME"));
					}
				}
					
				//add primary keys
				if(primaryKeys.size() > 0) {
					createStatement.append(",\n");
					createStatement.append("\tPRIMARY KEYS(");
					for(int i = 0; primaryKeys.size() > i; i++) {
						if(i > 0) createStatement.append(",");
						createStatement.append(primaryKeys.get(i));
					}
					createStatement.append(")\n");
				}
				
				//cap off create statement
				createStatement.append(")\n");
				//defaults
				
				
				
				System.out.println(createStatement.toString());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/***
	 * Attempts to print account table schema as a create statement acceptable snowflake
	 * PASS
	 */
	public static void createAccountTableTest() {
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity("gcarcu080119", null , "ARCUSYM000");
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		
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
		
		
		RowSet infSchema = dbsSource.getSrcInformationSchema(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName(), 		//Source database to get InformationSchema
				true);								//Grab only user tables
		try {
			String tableName = "";
			String prevTableName = "";
			int line = 0;
			
			ArrayList<String> pks = new ArrayList<String>();
			
			while(infSchema.next()) {
				String str = new String();
				
				tableName = infSchema.getString("TABLE_SCHEMA") + "." + infSchema.getString("TABLE_NAME");
				
				
				
				
				if(tableName.compareTo("dbo.ACCOUNT") ==0) {
					
					line++;
					if(line == 1) {
						str = "CREATE TABLE " + tableName+ " (";
						System.out.println(str);
					}
					
					if(line >= 2) {
						System.out.println(",");
					}
					
					if(infSchema.getString("CONSTRAINT_NAME") != null) pks.add(infSchema.getString("COLUMN_NAME"));
					
					
					String datatype = convertDataType(
										 infSchema.getString("DATA_TYPE_CAT"),
										 infSchema.getString("DATA_TYPE"),
										 infSchema.getInt("NUMERIC_PRECISION"),
										 infSchema.getInt("NUMERIC_SCALE"),
										 infSchema.getInt("CHARACTER_MAXIMUM_LENGTH"));
					
					String nullable = isNullable(infSchema.getString("IS_NULLABLE"));
					
					str = "\t" + infSchema.getString("COLUMN_NAME") + 
						  "\t" + datatype +
						  "\t" + nullable;

					System.out.print(str);
				}
				
				prevTableName = tableName;
			}
			if(pks.size() > 0) {
				System.out.println(",");
				
				String pk = "\tPRIMARY KEY(";
				for(int i = 0; i < pks.size(); i++) {
					if(i > 0) pk = pk + ",";
					pk = pk + pks.get(i);
				}
				pk = pk + ")";
				
				System.out.println(pk);
				
			}
			
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	static String isNullable(String isNullable) {
		String output = "";
		
		if(isNullable.equalsIgnoreCase("NO")) output = "NOT NULL";
		else if(isNullable.equalsIgnoreCase("YES")) output = "NULL";
		else output = "NULL";
		
		return output;
	}
	
	
	/***
	 * Converts SQL Server data type to Snowflake datatype
	 * @param dataType
	 * @return
	 */
	static String convertDataType(String dataTypeCategory, String dataType, int numericPrecision, int numericScale, 
			int charMaxLength) {
		/*
			DATE	smalldatetime
			DATE	date
			DATE	datetime
			DATE	datetime2
			DATE	time
			NUMERIC	smallint
			NUMERIC	money
			NUMERIC	int
			NUMERIC	numeric
			NUMERIC	bigint
			NUMERIC	decimal
			NUMERIC	bit
			NUMERIC	float
			NUMERIC	tinyint
			NUMERIC	real
			OTHER	varbinary
			TEXT	varchar
			TEXT	char
			TEXT	nvarchar
			TEXT	nchar
			TEXT	uniqueidentifier
			TEXT	text
		*/
		String output = null;
		switch(dataTypeCategory) {
			case "TEXT":
				if(dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("nvarchar")
					 || dataType.equalsIgnoreCase("text")) {
					output = "VARCHAR(" + charMaxLength + ")";
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
	
	public static void snowflakeDriverTest() throws Exception {
	    // get connection
	    System.out.println("Create Snowflake JDBC connection");
	    Connection connection = getConnection();
	    System.out.println("Done creating JDBC connection");
	    
	    // create statement
	    System.out.println("Create JDBC statement");
	    Statement statement = connection.createStatement();
	    System.out.println("Done creating JDBC statement");
	    
	    // create a table
	    System.out.println("Create demo table");
	    statement.executeUpdate("create or replace table demo(C1 STRING)");
	    //statement.close();
	    System.out.println("Done creating demo tablen");
	    
	    // insert a row
	    System.out.println("Insert 'hello world'");
	    statement.executeUpdate("insert into demo values ('hello world')");
	    //statement.close();
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
	
	private static Connection getConnection() throws SQLException {
	    try
	    {
	    	Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex)
	    {
	    	System.err.println("Driver not found");
	    }
	    
	    
	    
	    
	    //use JSON object to pull sensitive information instead of hardcoding
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of("H:\\snowflake_creds.json"));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
}
