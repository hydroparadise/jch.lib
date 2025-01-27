package jch.lib.cloud.gcp.bigquery;

import java.util.ArrayList;

import jch.lib.log.QLog;

public class BigQueryDiscovery {

	
	/***
	 * 
	 * https://developers.google.com/drive/api/guides/manage-downloads
	 * @param bucketName
	 * @param bucketPath
	 * @param baseFilename
	 * @param fileExtension
	 * @param format
	 * @param includeHeader
	 * @param colDelim
	 * @param sql
	 * @return
	 */
	public static String sqlExportDataShards(String bucketName, String bucketPath, String baseFilename, String fileExtension, 
									   String format, boolean includeHeader, String colDelim, String sql) {
		
		String output = "EXPORT DATA OPTIONS (";
		String uri = "gs://" + bucketName + bucketPath + baseFilename + "_*." + fileExtension;
		output += "uri='" + uri + "'";
		
		if(format != null && !format.equalsIgnoreCase(""))
			output += ",format='" + format + "'";
		
		output += ",overwrite=true";
		
		if(includeHeader == true)
			output +=  ",header=true";
		else output += ",header=false";
		
		if(colDelim != null && colDelim.equalsIgnoreCase(""));
			output += ",field_delimiter='" + colDelim + "'";
		
		if(sql != null && !sql.equalsIgnoreCase(""))
			output += ") AS (" + sql + ");";
		else output = null;
		
		return output;
	}
	
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
	 * @param databaseName
	 * @param schemaName
	 * @return
	 */
	public static String sqlDatasetAllTablesSchema(String databaseName, String schemaName) {
		String ds = "`" + databaseName + "`.`" + schemaName + "`";
		
		String output = "SELECT "
				      + "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_INSERTABLE_INTO, IS_TYPED, CREATION_TIME, "
				      + "BASE_TABLE_CATALOG, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, SNAPSHOT_TIME_MS, DDL, DEFAULT_COLLATION_NAME, "
				      + "UPSERT_STREAM_APPLY_WATERMARK "
				      + "FROM " + ds + ".INFORMATION_SCHEMA.TABLES "
				      + "ORDER BY TABLE_NAME";
		
		return output;
	}
	
	/***
	 * database schema information
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
	 * database schema information for a specific table
	 * 
	 * @param database Remote database name (ei, "DatabaseName")
	 * @param schema Remote schema name to compare columns(ie,"DBO")
	 * @param table Remote table name to compare columns (ie,"ACCOUNT")
	 * @return
	 */
	public static String sqlDatabaseTableInformationShema(String database, String schema, String table) {
		String output = "SELECT \n" 
			      + "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, \n"
			      + "DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, \n"
			      + "NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_TYPE, INTERVAL_PRECISION, CHARACTER_SET_CATALOG, \n"
			      + "CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME, \n"
			      + "DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, SCOPE_CATALOG, SCOPE_SCHEMA, \n"
			      + "SCOPE_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER, IS_SELF_REFERENCING, IS_IDENTITY, IDENTITY_GENERATION, \n"
			      + "IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE, COMMENT, \n"
			  	  + "	CASE WHEN DATA_TYPE IN ('TEXT') THEN 'TEXT'   \n"
				  + "		 WHEN DATA_TYPE IN ('NUMBER','FLOAT','BOOLEAN') THEN 'NUMERIC' \n"  
				  + "		 WHEN DATA_TYPE IN ('DATE','TIME') THEN  'DATETIME' \n"
			      + "        WHEN CHARINDEX('TIMESTAMP',DATA_TYPE,1) > 0 THEN 'DATETIME' \n"
				  + "		ELSE 'OTHER'  \n"
				  + "	END DATA_TYPE_CAT  \n"
			      + "FROM " + database + "\".INFORMATION_SCHEMA.COLUMNS \n"
				  + "WHERE TABLE_SCHEMA = '" + schema + "' \n"
				  + "	AND TABLE_NAME = '" + table + "' \n"
				  + "ORDER BY ORDINAL_POSITION";
		return output;
	}

	
	/***
	 * database schema information
	 * 
	 * Fields:
	 * 
	 * @param database
	 * @return sqlString
	 */
	public static String sqlAllColumnInformationShema(String database) {
		String output = "SELECT \n" 
				      + "\tTABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, \n"
				      + "\tDATA_TYPE, IS_GENERATED, GENERATION_EXPRESSION, IS_STORED, IS_HIDDEN, IS_UPDATABLE, \n"
				      + "\tIS_SYSTEM_DEFINED, IS_PARTITIONING_COLUMN, CLUSTERING_ORDINAL_POSITION, COLLATION_NAME, \n"
				      + "\tCOLUMN_DEFAULT, ROUNDING_MODE \n"
	                  + "FROM " + database + ".INFORMATION_SCHEMA.COLUMNS \n"
					  + "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION \n";
		return output;
	}
	
	/***
	 * 
	 * @param database
	 * @param tableName
	 * @return
	 */
	public static String sqlColumnInformationShema(String database, String tableName) {
		String output = "SELECT \n" 
				      + "\tTABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, \n"
				      + "\tDATA_TYPE, IS_GENERATED, GENERATION_EXPRESSION, IS_STORED, IS_HIDDEN, IS_UPDATABLE, \n"
				      + "\tIS_SYSTEM_DEFINED, IS_PARTITIONING_COLUMN, CLUSTERING_ORDINAL_POSITION, COLLATION_NAME, \n"
				      + "\tCOLUMN_DEFAULT, ROUNDING_MODE \n"
	                  + "FROM " + database + ".INFORMATION_SCHEMA.COLUMNS \n"
	                  + "WHERE TABLE_NAME = '" + tableName +"' \n"
					  + "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION \n";
		return output;
	}
	
	/***
	 * 
	 * @param projectID
	 * @return
	 */
	public static String sqlAllDatasetInformationShema(String projectID) {
		String output = "SELECT " 
				      + "\tCATALOG_NAME, SCHEMA_NAME, CREATION_TIME, LAST_MODIFIED_TIME, LOCATION, DDL\n"
				      + "\tDEFAULT_COLLATION_NAME\n"
	                  + "FROM " + projectID + ".INFORMATION_SCHEMA.SCHEMATA \n"
					  + "ORDER BY SCHEMA_NAME \n";
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
