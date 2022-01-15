package jch.lib.test;

import java.io.BufferedWriter;
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
import java.util.Properties;
import java.util.TreeMap;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.json.simple.*;
import org.json.simple.parser.*;

import jch.lib.db.sqlserver.SqlServerCnString;
import jch.lib.db.sqlserver.SqlServerDbScour;
import jch.lib.db.sqlserver.SqlServerDiscovery;
import net.snowflake.client.jdbc.SnowflakeStatement;

import java.util.ArrayList;
import java.util.Map;

/*
 * Snowflake concurrent statements
 * MAX_CONCURRENCY_LEVEL = 8
 */



public class JchLib_SnowflakeTest {
	static int PACK_SIZE = 1900000;
	
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
		VALUES ('1/1/2021','2013-05-08T23:39:20.123','Test insert',23)
	 */

	
	/*
	 * 
	 * snowflakeCreds \
	 *                 --- database, schema, table -> match fields
	 * srcSQLHost     /
	 * 
	 */
	public static void copyTableData(String snowflakeCreds, String srcSqlHost, 
		String database, String schema, String table) throws SQLException, IOException {
		
		StringBuilder intoColumns;
		StringBuilder intoFields;
		
		//Get Sql Server RowSet
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcSqlHost, null , database);
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		//get tables for given database
		RowSet ssCols = dbsSource.getSrcInformationSchema(
				srcCnString.getCnString(), 
				srcCnString.getDatabaseName(), 
				schema, 
				table);
		
		
		//Get Snowflake RowSet
		java.sql.Connection sfCn = null;
		sfCn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json");
		Statement sfStatement = sfCn.createStatement();
		
		//System.out.println(sqlSfDatabaseTableInformationShema(database,schema,table));
	    ResultSet sfColsRs = sfStatement.executeQuery(
	    		sqlSfDatabaseTableInformationShema(database,schema,table));
		
        RowSetFactory rsf = RowSetProvider.newFactory();
        CachedRowSet sfCols = rsf.createCachedRowSet();
        sfCols.populate(sfColsRs);
        sfStatement.close();
        //;
        
        //generate list of matching columns between Sql Server and Snowflake tables
        ArrayList<String> cols = rowsetColCompare(ssCols,sfCols);
        
        //load up a treemap to quickly reslove datatypes

        TreeMap<String, String> colDatatypeCat = new TreeMap<String, String>();
        ssCols.beforeFirst();
        while(ssCols.next()) {
        	//System.out.println(ssCols.getString("COLUMN_NAME") + ", " + ssCols.getString("DATA_TYPE_CAT"));
        	colDatatypeCat.put(ssCols.getString("COLUMN_NAME").toUpperCase(), 
        			           ssCols.getString("DATA_TYPE_CAT"));
        }
        
        sfCols.close();
        //System.out.println(cols.size());
        //System.out.println(ssGenerateSelect(database,schema,table, cols));
        System.out.println(colDatatypeCat.size());
        
        
        Connection srcCn = DriverManager.getConnection(srcCnString.getCnString()); 
        Statement ssStatment = srcCn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        ResultSet resColumns = ssStatment.executeQuery(ssGenerateSelect(database,schema,table, cols));
        
        String sqlInsertInto = sfGenerateInsertInto(schema.toUpperCase(), table.toUpperCase(), cols);
        
        
        //spin up snowflake connection with database context
		java.sql.Connection cn = null;
		cn = JchLib_SnowflakeTest.getConnection(snowflakeCreds,database.toUpperCase());
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

        	//System.out.println(values.length() + " vs " + values.toString().length());
        	if(sqlInsertInto.length() + values.length() > PACK_SIZE) {
        		cCnt++;
        		
        		sql.append(sqlInsertInto + " VALUES  " + values.toString());
        		System.out.println("cCnt: " + cCnt+ ", pCnt: " + pCnt + ", len: " + sql.length() + ", aCnt: " + aCnt);
        		
        		//System.out.println(sql.toString());
        	    //BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\temp\\out.txt"));
        	    //writer.write(sql.toString());
        	    //writer.close();
        		
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
        
        
		//Synchronous Call
		//statement.executeUpdate(sql.toString());
		
		//Asynchronous Call
		statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql.toString());
        
        statement.close();
        cn.close();
	}
	
	
	public static String sfSqlValuePrep(String value, String datatypeCat ) {
		String output = "";
		
		//DATA_TYPE_CAT: TEXT, NUMERIC, DATETIME, OTHER
		if(value == null) output = "null";
		else if (datatypeCat.equals("TEXT")) output = "$$" + value + "$$";
		else if (datatypeCat.equals("DATETIME")) output = "'" + value + "'";
		else if (datatypeCat.equals("NUMERIC")) output = value;
		else if (datatypeCat.equals("OTHER")) output = "null";
		
		return output; 
	}
	
	public static String sfGenerateInsertInto(String schema, String table,ArrayList<String> cols) {
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
	 * 
	 * @param database
	 * @param schema
	 * @param table
	 * @param cols
	 * @return
	 */
	public static String ssGenerateSelect(String database, String schema, String table, 
			ArrayList<String> cols) {
		String output = null;
		StringBuilder colList = new StringBuilder();
		
		if(database != null && schema != null && 
			table != null && cols != null && cols.size() > 0) {
			
			for(int i = 0; i < cols.size(); i++) {
				if(i > 0) colList.append(",");

				colList.append(SqlServerDiscovery.sqlObjBracket(cols.get(i)));

			}
			
			output = "SELECT " + colList.toString() + " FROM " 
				   + SqlServerDiscovery.sqlObjBracket(database) + "."
				   + SqlServerDiscovery.sqlObjBracket(schema) + "."
				   + SqlServerDiscovery.sqlObjBracket(table)				   ;
		}
		
		return output;
	}
	
	
	/***
	 * 
	 * @param ssRowSet
	 * @param sfRowSet
	 * @return
	 * @throws SQLException
	 */
	public static ArrayList<String> rowsetColCompare(RowSet ssRowSet, RowSet sfRowSet) throws SQLException {
		ArrayList<String> output = new ArrayList<String>();
		String ssCol = "";
		String sfCol = "";
		
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
		
		return output;
	}
	
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
	 * @return
	 */
	String sqlSfDatabaseAllInformationShema(String database) {
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
	
	//main
	
	//java.sql.Connection cn = null;
	//Statement statement = cn.createStatement();
	
	//Pass
	//cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json", "FMCUANALYTICSTEST", "dbo");
	
	//Pass
	//cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json", "FMCUANALYTICSTEST");
	//pass
	//statement.executeUpdate("CREATE SCHEMA arcu");
	//Pass
	//statement.executeUpdate("create or replace table arcu.demo(C1 STRING)");
	
	//Pass
	//cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json");
	//Pass
	//statement.executeUpdate("CREATE DATABASE test");
	
	
	
	//Passed!
	//JchLib_SnowflakeTest.createDatabase("gcarcu080119","ARCUSYM000");
	
	//Passed!
	//JchLib_SnowflakeTest.createDatabase("gcarcu080119","FMCUAnalytics");
	
	//Passed!
	//JchLib_SnowflakeTest.createDatabase("gcarcu080119","CFSConnectors");
	
	//JchLib_SnowflakeTest.createAllDatabaseSchemas("gcarcu080119","ARCUSYM000");
	//JchLib_SnowflakeTest.createAllTablesDatabase("gcarcu080119","ARCUSYM000");
	//JchLib_SnowflakeTest.createAccountTableTest();
	//JchLib_SnowflakeTest.snowflakeDriverTest();
	
	/***
	 * Copies database schema and tables from a SQL Server host up to Snowflake instance.
	 * 
	 * host
	 * 	database
	 * 	 schemas
	 *    tables
	 *     columns
	 */
	public static void copyTableSchemas() {
		createDatabase("gcarcu080119","ARCUSYM000");
		createDatabase("gcarcu080119","FMCUAnalytics");
		createDatabase("gcarcu080119","CFSConnectors");
	}
	
	/***
	 * Creates a copy of database, schema, and tables from a SQL Server host to a Snowflake instance
	 * 
	 * @param Source SQL Server host name (String)
	 * @param Source SQL Server database of the previously specified host name (String)
	 */
	public static void createDatabase(String srcHost, String srcDatabase) {
		java.sql.Connection cn = null;
		
		
		try {	
			//Create Snowflake Databas
			cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json");
			Statement statement = cn.createStatement();
			statement.executeUpdate("CREATE DATABASE " + srcDatabase);
			statement.close();
			cn.close();
			
			cn = JchLib_SnowflakeTest.getConnection("H:\\snowflake_creds.json", srcDatabase);
			
			//Create Schemas and Tables
			System.out.println("Create Shemas...");
			createAllDatabaseSchemas(cn, srcHost, srcDatabase);
			
			System.out.println("Create Tables...");
			createAllDatabaseTables(cn, srcHost, srcDatabase);
			
			cn.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/***
	 * Copies schemas from a source SQL Server host to a Snowflake instance 
	 * 
	 * @param An established Snowflake connection to execute Snowflake SQL statements (java.sql.Connection)
	 * @param Source SQL Server host name (String)
	 * @param Source SQL Server database of the previously specified host name (String)
	 * @throws SQLException
	 */
	public static void createAllDatabaseSchemas(java.sql.Connection snowflakeCn, String srcHost, String srcDatabase)  throws SQLException {
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcHost, null , srcDatabase);
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		
		//get tables for given database
		RowSet schemas = dbsSource.getSrcSchemas(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName());	
		
		try {
			//object the excutes to snowflake
			Statement statement = snowflakeCn.createStatement();
			
			while(schemas.next()) {
				StringBuilder createStatement = new StringBuilder();
				
				createStatement.append("CREATE SCHEMA " + schemas.getString("TABLE_SCHEMA"));
				
				System.out.println(createStatement.toString());
				statement.executeUpdate(createStatement.toString());
			}
			
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/***
	 * Copies schemas from a source SQL Server host to a Snowflake instance by performing a syntax conversion
	 * Creates a series of create statements
	 * Currently supported: Column Names, Data Types, Default Values, Nullable, and Primary Keys 
	 *  
	 * @param Source SQL Server host name (String)
	 * @param Source SQL Server database of the previously specified host name (String)
	 */
	public static void createAllDatabaseTables(java.sql.Connection snowflakeCn, String srcHost, String srcDatabase) throws SQLException {
		SqlServerCnString srcCnString = new SqlServerCnString();
		srcCnString.setCnStringIntegratedSecurity(srcHost, null , srcDatabase);
		
		SqlServerDbScour dbsSource = new SqlServerDbScour();
		
		//get tables for given database
		RowSet tables = dbsSource.getSrcTables(
				srcCnString.getCnString(), 			//Source host to get InformationSchema
				srcCnString.getDatabaseName());
		
		try {
			//object the excutes to snowflake
			Statement statement = snowflakeCn.createStatement();
			
			//loop through tables
			while(tables.next()) {
				StringBuilder createStatement = new StringBuilder();
				
				//get column of current table
				RowSet cols = dbsSource.getSrcInformationSchema(
						srcCnString.getCnString(), 
						srcCnString.getDatabaseName(), 
						tables.getString("TABLE_SCHEMA"), 
						tables.getString("TABLE_NAME"));
							
				//get table name
				String tblName = tables.getString("TABLE_NAME");
				//check for weird characters: if true, wrap with double quotes (snowflake likes for literals)
				if(tblName.contains("[") == true || 
				   tblName.contains("]") == true ||	
			       tblName.contains(".") == true || 
			       tblName.contains(" ") == true) {
					tblName = "\"" + tblName + "\"";
				}
				//create statement
				createStatement.append("CREATE TABLE " + tables.getString("TABLE_SCHEMA") + "." + tblName + " (\n");
				
				//used collect primary keys along the way (getSrcInfromationSchema is sorted by ordinal)
				ArrayList<String> primaryKeys = new ArrayList<String>();
				
				//ensure 1 column per ordinal position.  
				int ordinal = 0, prevOrdinal = 0;
								
				//iterate through columns
				int colCnt = 0;
				while(cols.next()) {

					
					ordinal = cols.getInt("ORDINAL_POSITION");
					
					//ensures column names won't be repeated
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
						
						//convert data type to snowflake compatible
						String datatype = convertDataType(
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
						
						//is field nullable
						String nullable = isNullable(cols.getString("IS_NULLABLE"));
						createStatement.append("\t" + nullable);
						
						//if constraint is not null, it is likely a primary key
						if(cols.getString("CONSTRAINT_NAME") != null) {
							primaryKeys.add(cols.getString("COLUMN_NAME"));
						}
					}
					
					prevOrdinal = cols.getInt("ORDINAL_POSITION");
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
				
				System.out.println(createStatement.toString());
				statement.execute(createStatement.toString());
			}
			
			statement.close();
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
										 infSchema.getInt("CHARACTER_MAXIMUM_LENGTH"),
										 infSchema.getString("COLUMN_DEFAULT") );
					
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
	 * Converts SQL Server data type to Snowflake datatype
	 * @param dataType
	 * @return
	 */
	static String convertDataType(String dataTypeCategory, String dataType, int numericPrecision, int numericScale, 
			int charMaxLength, String colDefault) {
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
	 * 
	 * @param credentialPath
	 * @return 
	 * @throws SQLException
	 */
	public static Connection getConnection(String credentialPath) throws SQLException  {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	    
	    return DriverManager.getConnection(connectStr, properties);
	}
	
	/***
	 * 
	 * @param credentialPath
	 * @param database
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection(String credentialPath, String database) throws SQLException {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	    
	    return DriverManager.getConnection(connectStr, properties);
	}
	
	/***
	 * 
	 * @param credentialPath
	 * @param database
	 * @param schema
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection(String credentialPath, String database, String schema) throws SQLException {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
	    System.out.println(connectStr);
	    
	    return DriverManager.getConnection(connectStr, properties);
	}
	
	
	/***
	 * 
	 * @return
	 * @throws SQLException
	 */
	private static Connection getConnectionTest() throws SQLException {
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
