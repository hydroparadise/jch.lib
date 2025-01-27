package jch.lib.cloud.snowflake;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.sql.RowSet;

import jch.lib.log.QLog;

/***
 * A collection of methods that generate SQL statements for execution within a Snowflake environment
 * 
 * @author harrisonc
 *
 */
public class SnowflakeDiscovery {
	
	
	/***
	 * Create database
	 * @param databaseName
	 * @return
	 */
	public static String sqlCreateDatabase(String databaseName) {
		String output =  null;
		output = "CREATE DATABASE " + databaseName;
		return output;
	}
	
	
	/***
	 * 
	 * @param databaseName
	 * @param schemaName
	 * @param oldTableName
	 * @param newTableName
	 * @return
	 */
	public static String sqlRenameTable(String databaseName, String schemaName, String oldTableName, String newTableName) {
		String output = null;
		
		
		String otn = asmSfObj(databaseName, schemaName, oldTableName);
		String ntn = asmSfObj(databaseName, schemaName, newTableName);
		
		
		if(otn != null && otn != "" && 
		   ntn != null && ntn != "") {
			
			output = "ALTER TABLE " + otn + " RENAME TO " + ntn + ";";
		}
		
		return output;
	}
	
	
	/***
	 * 
	 * @param azBlobCredsLoc
	 * @param azBlobFileName
	 * @param azBlobDir
	 * @param sfSchema
	 * @param sfTable
	 * @param sfStage
	 * @param forceLoad
	 * @return
	 */
	public static String sqlCopyFromStage(String azBlobFileName, String azBlobDir, 
			String sfSchema, String sfTable, String sfStage, boolean forceLoad) {
		String output = null;
		   
		StringBuilder sql = new StringBuilder();
		//String tbl = sfSchema.toUpperCase() + "." + sfTable.toUpperCase();
		String tbl = asmSfObj(null,sfSchema,sfTable);
		String abfnp = null;
		
		
		
		//make sure azure blob directory is not blank
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
	
	
	/***
	 * 
	 * @param cols
	 * @param srcDatabase
	 * @param srcSchema
	 * @param srcTable
	 * @return SQL CREATE TABLE
	 */
	public static String sqlCreateTable(RowSet cols, String srcDatabase, String srcSchema, String srcTable) {

		//check for unconventional characters in table name: if true, wrap with double quotes for Snowflake literals
		String tbl = asmSfObj( srcDatabase,  srcSchema,  srcTable);
		
		//create statement
		StringBuilder createStatement = new StringBuilder();
		createStatement.append("CREATE TABLE " + tbl + " (\n");
		
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
						
						boolean printDefault = true;
						
						//make sure there aren't any SQL Server specific commands going across
						if(colDefault.toLowerCase().contains("newsequentialid()") == true) printDefault = false; 
						if(colDefault.toLowerCase().contains("nuser_name()") == true) printDefault = false;
						if(colDefault.toLowerCase().contains("app_name()") == true) printDefault = false;
						
						//make sure data type and defualt is consistent
						if(colDefault.toLowerCase().contains("((0))") == true &&
						   cols.getString("DATA_TYPE_CAT") != "NUMERIC") printDefault = false;
						
						
						if(printDefault == true)
							createStatement.append("\tDEFAULT " + colDefault);
						
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
			QLog.log("ETL Exception: " + e.toString(),true);
			QLog.log(e,true);
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
	 * @return NULL or NOT NULL
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
	 * 
	 * @param database
	 * @param schema
	 * @param table
	 * @param col
	 * @return
	 */
	public static String sqlMaxValue(String database, String schema, String table, String col) {
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
	public static String sqlDatabaseAllInformationShema(String database) {
		String output = "SELECT " 
				      + "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, "
				      + "DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, "
				      + "NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_TYPE, INTERVAL_PRECISION, CHARACTER_SET_CATALOG, "
				      + "CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME, "
				      + "DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, SCOPE_CATALOG, SCOPE_SCHEMA, "
				      + "SCOPE_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER, IS_SELF_REFERENCING, IS_IDENTITY, IDENTITY_GENERATION, "
				      + "IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE, COMMENT, "
				  	  + "	CASE WHEN DATA_TYPE IN ('TEXT') THEN 'TEXT'   "
					  + "		 WHEN DATA_TYPE IN ('NUMBER','FLOAT','BOOLEAN') THEN 'NUMERIC' "  
					  + "		 WHEN DATA_TYPE IN ('DATE','TIME') THEN  'DATETIME' "
				      + "        WHEN CHARINDEX('TIMESTAMP',DATA_TYPE,1) > 0 THEN 'DATETIME' "
					  + "		ELSE 'OTHER'  "
					  + "	END DATA_TYPE_CAT  "
	                  + "FROM \"" + database.toUpperCase() + "\".INFORMATION_SCHEMA.COLUMNS "
					  + "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";
		return output;
	}
	
	
	/***
	 * Snowflake database schema information for a specific table
	 * 
	 * @param database Remote Snowflake database name (ei, "DatabaseName")
	 * @param schema Remote Snowflake schema name to compare columns(ie,"DBO")
	 * @param table Remote Snowflake table name to compare columns (ie,"ACCOUNT")
	 * @return
	 */
	public static String sqlDatabaseTableInformationShema(String database, String schema, String table) {
		String output = "SELECT " 
			      + "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, "
			      + "DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, "
			      + "NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_TYPE, INTERVAL_PRECISION, CHARACTER_SET_CATALOG, "
			      + "CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME, "
			      + "DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, SCOPE_CATALOG, SCOPE_SCHEMA, "
			      + "SCOPE_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER, IS_SELF_REFERENCING, IS_IDENTITY, IDENTITY_GENERATION, "
			      + "IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE, COMMENT, "
			  	  + "	CASE WHEN DATA_TYPE IN ('TEXT') THEN 'TEXT'   "
				  + "		 WHEN DATA_TYPE IN ('NUMBER','FLOAT','BOOLEAN') THEN 'NUMERIC' "  
				  + "		 WHEN DATA_TYPE IN ('DATE','TIME') THEN  'DATETIME' "
			      + "        WHEN CHARINDEX('TIMESTAMP',DATA_TYPE,1) > 0 THEN 'DATETIME' "
				  + "		ELSE 'OTHER'  "
				  + "	END DATA_TYPE_CAT  "
			      + "FROM \"" + database.toUpperCase() + "\".INFORMATION_SCHEMA.COLUMNS "
				  + "WHERE TABLE_SCHEMA = '" + schema.toUpperCase() 
				  + "' AND TABLE_NAME = '" + table.toUpperCase() + "' "
				  + "ORDER BY ORDINAL_POSITION";
		return output;
	}


	/***
	 * 
	 * @param value
	 * @param datatypeCat
	 * @return
	 */
	public static String sqlValuePrep(String value, String datatypeCat ) {
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
	 * 
	 * 
	 * @param schema
	 * @param table
	 * @param cols
	 * @return SQL Sting
	 */
	public static String sqlInsertInto(String schema, String table,ArrayList<String> cols) {
		StringBuilder output = new StringBuilder();
		
		output.append("INSERT INTO \"" + schema.toUpperCase() + "\".\"" + table.toUpperCase() + "\" (");
		for(int i = 0; i < cols.size(); i++) {
			if(i > 0) output.append(",");
			output.append("\"" + cols.get(i) + "\"");
		}
		output.append(")");
		
		return output.toString();
	}
	
	
	/****
	 * 
	 * @param sfDatabase
	 * @param sfSchema
	 * @param sfTable
	 * @return
	 */
	public static String asmSfObj(String sfDatabase, String sfSchema, String sfTable) {
		String output = null;
		

		if(sfTable != null && sfTable != "") {
			output = wrapQuotes(sfTable);
			if(sfSchema != null && sfSchema != "") {
				output = wrapQuotes(sfSchema) + "." + output;
				if(sfDatabase != null && sfDatabase != "") {
					output = wrapQuotes(sfDatabase) + "." + output;
				}
			}
			
		}

		return output;
	}
	
	/***
	 * 
	 * @param sfObject
	 * @return
	 */
	public static String wrapQuotes(String sfObject) {
		String output = sfObject;
		
		if(sfObject != null) { 
			if(sfObject.contains("[") == true || 
					sfObject.contains("]") == true ||	
							sfObject.contains(".") == true || 
									sfObject.contains(" ") == true) {
						
				output = "\"" + sfObject + "\"";		
			}
		}
				
		return output;
	}
	
	
}
