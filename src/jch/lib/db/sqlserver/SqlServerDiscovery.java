package jch.lib.db.sqlserver;

import java.sql.*;


/***
 * 
 * 
 * INFORMATION_SCHEMA Tables
	SELECT * FROM INFORMATION_SCHEMA.COLUMNS
	SELECT * FROM INFORMATION_SCHEMA.TABLES
	SELECT * FROM INFORMATION_SCHEMA.VIEWS
	SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
	SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
	SELECT * FROM INFORMATION_SCHEMA.ROUTINES
	SELECT * FROM INFORMATION_SCHEMA.ROUTINE_COLUMNS
	SELECT * FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
	SELECT * FROM INFORMATION_SCHEMA.COLUMN_DOMAIN_USAGE
	SELECT * FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES
	SELECT * FROM INFORMATION_SCHEMA.DOMAIN_CONSTRAINTS
	SELECT * FROM INFORMATION_SCHEMA.DOMAINS
	SELECT * FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES
	SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
	SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
 * @author harrisonc
 *
 */
public class SqlServerDiscovery {
	
	/**
	 * SQLServer Version 12.0.
	 * 
	 * Fields
	 * name,database_id,source_database_id,owner_sid,create_date,compatibility_level,collation_name,user_access,user_access_desc,
	 * is_read_only,is_auto_close_on,is_auto_shrink_on,state,state_desc,is_in_standby,is_cleanly_shutdown,is_supplemental_logging_enabled,
	 * snapshot_isolation_state,snapshot_isolation_state_desc,is_read_committed_snapshot_on,recovery_model,recovery_model_desc,
	 * page_verify_option,page_verify_option_desc,is_auto_create_stats_on,is_auto_create_stats_incremental_on,is_auto_update_stats_on,
	 * is_auto_update_stats_async_on,is_ansi_null_default_on,is_ansi_nulls_on,is_ansi_padding_on,is_ansi_warnings_on,is_arithabort_on,
	 * is_concat_null_yields_null_on,is_numeric_roundabort_on,is_quoted_identifier_on,is_recursive_triggers_on,is_cursor_close_on_commit_on,
	 * is_local_cursor_default,is_fulltext_enabled,is_trustworthy_on,is_db_chaining_on,is_parameterization_forced,
	 * is_master_key_encrypted_by_server,is_query_store_on,is_published,is_subscribed,is_merge_published,is_distributor,is_sync_with_backup,
	 * service_broker_guid,is_broker_enabled,log_reuse_wait,log_reuse_wait_desc,is_date_correlation_on,is_cdc_enabled,is_encrypted,
	 * is_honor_broker_priority_on,replica_id,group_database_id,resource_pool_id,default_language_lcid,
	 * default_language_name,default_fulltext_language_lcid,default_fulltext_language_name,is_nested_triggers_on,is_transform_noise_words_on,
	 * two_digit_year_cutoff,containment,containment_desc,target_recovery_time_in_seconds,delayed_durability,delayed_durability_desc,
	 * is_memory_optimized_elevate_to_snapshot_on
	 * @return String
	 */
	
	/**
	 * Grabs all available databases for a SQL server instance.
	 * @return SQL String
	 */
	public static String sqlAllDatabases() {
		return "SELECT * FROM sys.databases";
	}
	
	/**
	 * Grabs all available databases for a SQL server instance excluding system databases (usual master, tempdb, model, msdb)
	 * @return SQL String
	 */
	public static String sqlAllUserDatabases() {
		return 
			  "SELECT * FROM sys.databases  "
			+ "WHERE name NOT IN ('master','tempdb','model','msdb')";
	}
	

	/***
	 * All tables and columns unsorted. Used as a based for others
	 * Data Type Categories: TEXT, NUMERIC, DATETIME, OTHER
	 * @param databaseName String
	 * @return SQL String
	 */
	static String sqlAllTableViewColumnsBase(String databaseName) {
		String output = null;
		databaseName = sqlObjBracket(databaseName);
		
		if(databaseName != null) {
			
			output = 
				   "SELECT T.TABLE_TYPE,T.TABLE_CATALOG,T.TABLE_SCHEMA,T.TABLE_NAME,C.COLUMN_NAME,CU.CONSTRAINT_NAME,  "
				 + "	C.DATA_TYPE,C.ORDINAL_POSITION,C.COLUMN_DEFAULT,C.IS_NULLABLE,C.CHARACTER_MAXIMUM_LENGTH,C.CHARACTER_OCTET_LENGTH,  "
				 + "	C.NUMERIC_PRECISION,C.NUMERIC_PRECISION_RADIX,C.NUMERIC_SCALE,C.DATETIME_PRECISION,  "
				 + " 	C.CHARACTER_SET_NAME,C.COLLATION_CATALOG,C.COLLATION_SCHEMA,C.COLLATION_NAME,C.DOMAIN_CATALOG,  "
				 + " 	C.DOMAIN_SCHEMA,C.DOMAIN_NAME,C.CHARACTER_SET_CATALOG,C.CHARACTER_SET_SCHEMA,   "
				 + " 		CASE WHEN DATA_TYPE IN ('varchar','nvarchar','text','char','nchar') THEN 'TEXT'  " 
				 + " 	    WHEN DATA_TYPE IN ('smallint','int','money','numeric','decimal','bigint','float','uniqueidentifier','real','tinyint','bit') THEN 'NUMERIC'  "
				 + " 		WHEN DATA_TYPE IN ('smalldatetime','date','datetime','datetime2','time') THEN 'DATETIME'  "
				 + " 		ELSE 'OTHER' END DATA_TYPE_CAT  "
				 + "FROM " + databaseName + ".INFORMATION_SCHEMA.TABLES T  "
				 + "	JOIN " + databaseName + ".INFORMATION_SCHEMA.COLUMNS C ON  "
				 + "		T.TABLE_CATALOG = C.TABLE_CATALOG AND T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME  "
				 + "	LEFT JOIN " + databaseName + ".INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CU ON  "
				 + "		CU.TABLE_CATALOG = C.TABLE_CATALOG AND CU.TABLE_SCHEMA = C.TABLE_SCHEMA AND  "
				 + "		CU.TABLE_NAME = C.TABLE_NAME AND CU.COLUMN_NAME = C.COLUMN_NAME";
				 //+ "ORDER BY T.TABLE_TYPE,T.TABLE_CATALOG,T.TABLE_NAME,T.TABLE_SCHEMA,C.ORDINAL_POSITION";

		}
		return output;
	}
	
	/*** 
	 * 
	 * @param databaseName String
	 * @return SQL String
	 */
	public static String sqlAllTableViewColumns(String databaseName) {
		String output = null;
		output = sqlAllTableViewColumnsBase(databaseName);
		if(output != null) {
			output = output + "  ORDER BY T.TABLE_TYPE,T.TABLE_CATALOG,T.TABLE_NAME,T.TABLE_SCHEMA,C.ORDINAL_POSITION";
		}
		return output;
	}
	
	
	/*** 
	 * 
	 * @param databaseName String
	 * @return SQL String
	 */
	public static String sqlAllTableColumns(String databaseName) {
		String output = null;
		output = sqlAllTableViewColumnsBase(databaseName);
		if(output != null) {
			output = output + "  WHERE T.TABLE_TYPE = 'BASE TABLE'"
					+ "  ORDER BY T.TABLE_TYPE,T.TABLE_CATALOG,T.TABLE_NAME,T.TABLE_SCHEMA,C.ORDINAL_POSITION";
		}
		return output;
	}
	
	/***
	 * 
	 * @param databaseName String
	 * @return SQL String
	 */
	public static String sqlAllViewColumns(String databaseName) {
		String output = null;
		output = sqlAllTableViewColumnsBase(databaseName);
		if(output != null) {
			output = output + "  WHERE T.TABLE_TYPE = 'VIEW'"
					+ "  ORDER BY T.TABLE_TYPE,T.TABLE_CATALOG,T.TABLE_NAME,T.TABLE_SCHEMA,C.ORDINAL_POSITION";
		}
		return output;
	}

	/***
	 * Puts brackets around an object name in case of spaces in name.
	 * @param objectName String
	 * @return String
	 */
	public static String sqlObjBracket(String objectName) {
		if(objectName != null && objectName.length() > 0) {
			return "[" + objectName + "]";
		}
		return null;
		
	}
	
}