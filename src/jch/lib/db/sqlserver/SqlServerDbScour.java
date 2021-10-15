package jch.lib.db.sqlserver;
import java.sql.*;
import javax.sql.*;
import javax.sql.rowset.*;

/***
 * SqlServerDbSour can be used to get basic statics about a database and to search specific values.
 * The strategy is to make all "destructive" operations as static methods so that instance objects
 * aren't inadvertently ran.
 * 
 * @author harrisonc
 *
 */
public class SqlServerDbScour {
	
	int threadPoolSize = 10;
	
	/***
	 * 
	 * @param Source Connection String (Used as a value in table, include databaseName recommended)
	 * @param Destination Connection String (used to perform update in table)
	 * @param Destination Database Name String (database to store results, won't assume catalog name in destCnString) 
	 * @param Destination Schema String (table schema in case not default "dbo")
	 * @param Information Schema RowSet (CachedRowSet)
	 * @return
	 */
	public boolean updateDestInformationSchema(String srcCnString,
			String destCnString, String destDbName, String schema, RowSet infSch) {
		boolean success = false;
		Connection cn = null;  //connection
		try {
			
			
            cn = DriverManager.getConnection(destCnString);            
            Statement sta = cn.createStatement();
            
            destDbName = SqlServerDiscovery.sqlObjBracket(destDbName);
    		if(schema == null || schema.length() == 0) schema = "dbo";
    		else schema = SqlServerDiscovery.sqlObjBracket(schema);
    		
    		//makes sure we're on the right database before attempting any record modifications
            sta.execute("USE " + destDbName);
            
            //remove records based on the source connection string to reupdate to account for any changes.
            //System.out.println("DELETE FROM " + schema + ".InformationSchema WHERE CnString = " + SqlServerDiscovery.sqlStringClean(srcCnString));
            sta.execute("DELETE FROM " + schema + ".InformationSchema WHERE CnString = " + SqlServerDiscovery.sqlStringClean(srcCnString));
            
            //Begin RowSet Iteration
			while(infSch.next()) {


				//Grab each value from specific row on RowSet
				String tableType = infSch.getString("TABLE_TYPE");							//TableType
				String tableCatalog = infSch.getString("TABLE_CATALOG");					//TableCatalog
				String tableSchema = infSch.getString("TABLE_SCHEMA");						//TableSchema
				String tableName = infSch.getString("TABLE_NAME");							//TableName
				String columnName = infSch.getString("COLUMN_NAME");						//ColumnName
				String constraintName = infSch.getString("CONSTRAINT_NAME");				//ConstraintName
				String dataType = infSch.getString("DATA_TYPE");							//DataType
				Integer ordinalPosition = infSch.getInt("ORDINAL_POSITION");				//OrdinalPosition
				String columnDefault = infSch.getString("COLUMN_DEFAULT");					//ColumnDefault
				String isNullable = infSch.getString("IS_NULLABLE");						//IsNullable
				Integer characterMaximumLength = infSch.getInt("CHARACTER_MAXIMUM_LENGTH");	//CharacterMaximumLength
				Integer characterOctetLength = infSch.getInt("CHARACTER_OCTET_LENGTH");	 	//CharacterOctetLength
				Integer numericPrecision = infSch.getInt("NUMERIC_PRECISION");				//NumericPrecision
				Integer numericPrecisionRadix = infSch.getInt("NUMERIC_PRECISION_RADIX");	//NumericPrecisionRadix
				Integer numericScale = infSch.getInt("NUMERIC_SCALE");						//NumericScale
				Integer dateTimePrecision = infSch.getInt("DATETIME_PRECISION");			//DateTimePrecision
				String characterSetName = infSch.getString("CHARACTER_SET_NAME");			//CharacterSetName
				String collationCatalog = infSch.getString("COLLATION_CATALOG");			//CollationCatalog
				String collationSchema = infSch.getString("COLLATION_SCHEMA");				//CollationSchema
				String collationName = infSch.getString("COLLATION_NAME");					//CollationName
				String domainCatalog = infSch.getString("DOMAIN_CATALOG");					//DomainCatalog
				String domainSchema = infSch.getString("DOMAIN_SCHEMA");					//DomainSchema
				String domainName = infSch.getString("DOMAIN_NAME");						//DomainName
				String characterSetCatalog = infSch.getString("CHARACTER_SET_CATALOG");		//CharacterSetCatalog
				String characterSetSchema = infSch.getString("CHARACTER_SET_SCHEMA");		//CharacterSetSchema
				String dataTypeCat = infSch.getString("DATA_TYPE_CAT");						//DataTypeCategory
				
				//Build INSERT string to be executed
				StringBuilder sqlInsert = new StringBuilder("INSERT INTO " + schema + ".InformationSchema (");
				sqlInsert.append("CnString,TableType,TableCatalog,TableSchema,TableName,ColumnName,ConstraintName,DataType,OrdinalPosition,"
						       + "ColumnDefault,IsNullable,CharacterMaximumLength,CharacterOctetLength,NumericPrecision,NumericPrecisionRadix,"
						       + "NumericScale,DateTimePrecision,CharacterSetName,CollationCatalog,CollationSchema,CollationName,DomainCatalog,"
						       + "DomainSchema,DomainName,CharacterSetCatalog,CharacterSetSchema,DataTypeCategory)  VALUES(");

				sqlInsert.append(SqlServerDiscovery.sqlStringClean(srcCnString) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(tableType) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(tableCatalog) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(tableSchema) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(tableName) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(columnName) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(constraintName) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(dataType) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlIntClean(ordinalPosition) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(columnDefault) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(isNullable) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlIntClean(characterMaximumLength) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlIntClean(characterOctetLength) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlIntClean(numericPrecision) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlIntClean(numericPrecisionRadix) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlIntClean(numericScale) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlIntClean(dateTimePrecision) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(characterSetName) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(collationCatalog) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(collationSchema) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(collationName) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(domainCatalog) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(domainSchema) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(domainName) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(characterSetCatalog) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(characterSetSchema) + ", ");
				sqlInsert.append(SqlServerDiscovery.sqlStringClean(dataTypeCat) + ")");
				
				//System.out.println(sqlInsert.toString());
				sta.executeUpdate(sqlInsert.toString());
			}
		}
		catch (SQLException ex) {ex.printStackTrace();}
		
		return success;
	}
	
	public RowSet getSrcInformationSchema(String srcCnString, String srcDbName) {
        //String sqlAllDatabase = SqlServerDiscovery.sqlAllUserDatabases();

		Connection cn = null;  //connection
		CachedRowSet rs = null;
		
		//Open connection, run drop sql statements
        try {
        	
            cn = DriverManager.getConnection(srcCnString);         
            String sql = SqlServerDiscovery.sqlDbTableViewColumns(srcDbName);
            
	        Statement sta = cn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
	        ResultSet res = sta.executeQuery(sql);
	        
	        RowSetFactory rsf = RowSetProvider.newFactory();
	        rs = rsf.createCachedRowSet();
	        rs.populate(res);

        } 
        catch (SQLException ex) {ex.printStackTrace();} 
        finally {
	        try {
	            if (cn != null && !cn.isClosed()) {cn.close();}
	        } 
	        catch (SQLException ex) {ex.printStackTrace();}
        }
        return rs; 
	}
	

	
	/***
	 * Creates table to store results from INFORMATION_SCHEMA from 
	 * SqlSeverDiscovery.sqlAllTableViewColumnsBase() and it's derivatives:
	 * (sqlAllTableViewColumns,sqlAllTableColumns,sqlAllViewColumns)
	 * @param Connection String
	 * @param Database Name String
	 * @param Schema String
	 * @return True if successful, False if unsuccesful
	 */
	public static boolean createDbScourOjbects(String destCnString, String destDbName, String schema) {
		Connection cn = null;  //connection
		boolean success = false;
        try {
            cn = DriverManager.getConnection(destCnString);            
            Statement sta = cn.createStatement();
            destDbName = SqlServerDiscovery.sqlObjBracket(destDbName);
            sta.execute("USE " + destDbName);
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableInformationSchema(schema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableTblStats(schema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColStats(schema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColSearchResults(schema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColStatsExt(schema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateViewTblStats(schema));
            success = true;
        } 
        
        catch (SQLException ex) {
        	
            ex.printStackTrace();
            return false;
        } 
        finally {
	        try {
	            if (cn != null && !cn.isClosed()) {cn.close();}
	        } 
	        catch (SQLException ex) {ex.printStackTrace();}
        }
        return success;
	}

	/***
	 * Drops all database objects related to DB scour.
	 * @param Connection String
	 * @param Database Name String
	 * @param Schema String
	 * @return
	 */
	public static boolean dropDbScourOjbects(String destCnString, String destDbName, String schema) {
		Connection cn = null;  //connection
		boolean success = false;
		
		//Open connection, run drop sql statements
        try {
            cn = DriverManager.getConnection(destCnString);            
            Statement sta = cn.createStatement();
            destDbName = SqlServerDiscovery.sqlObjBracket(destDbName);
            sta.execute("USE " + destDbName);
            sta.executeUpdate(SqlServerDbScour.sqlDropAllObjects(schema));
            success = true;
        } 
        catch (SQLException ex) {ex.printStackTrace();} 
        finally {
	        try {
	            if (cn != null && !cn.isClosed()) {cn.close();}
	        } 
	        catch (SQLException ex) {ex.printStackTrace();}
        }
        return success;
	}
	
	/***
	 * Returns SQL that creates table to store results from INFORMATION_SCHEMA from 
	 * SqlSeverDiscovery.sqlAllTableViewColumnsBase() and it's derivatives:
	 * (sqlAllTableViewColumns,sqlAllTableColumns,sqlAllViewColumns)
	 * @param Connection String
	 * @param Database Name String
	 * @param Schema String
	 * @return SQL String
	 */
	public static final String sqlCreateTableInformationSchema(String schema) {
		if(schema == null || schema.length() == 0) schema = "dbo";
		else schema = SqlServerDiscovery.sqlObjBracket(schema);
		String output = 
			"CREATE TABLE InformationSchema (  "
		  + "		CnString VARCHAR(512),  "
		  + "		TableType VARCHAR(255),  "
		  + "		TableCatalog VARCHAR(255),  "
		  + "		TableSchema VARCHAR(255),  "
		  + "		TableName VARCHAR(255),  "
		  + "		ColumnName VARCHAR(255),  "
		  + "		ConstraintName VARCHAR(255),  "
		  + "		DataType VARCHAR(255),  "
		  + "		OrdinalPosition INT,  "
		  + "		ColumnDefault VARCHAR(255),  "
		  + "		IsNullable VARCHAR(255),  "
		  + "		CharacterMaximumLength INT,  "
		  + "		CharacterOctetLength INT,  "
		  + "		NumericPrecision INT,  "
		  + "		NumericPrecisionRadix INT,  "
		  + "		NumericScale INT,  "
		  + "		DateTimePrecision INT,  "
		  + "		CharacterSetName VARCHAR(255),  "
		  + "		CollationCatalog VARCHAR(255),  "
		  + "		CollationSchema VARCHAR(255),  "
		  + "		CollationName VARCHAR(255),  "
		  + "		DomainCatalog VARCHAR(255),  "
		  + "		DomainSchema VARCHAR(255),  "
		  + "		DomainName VARCHAR(255),  "
		  + "		CharacterSetCatalog VARCHAR(255),  "
		  + "		CharacterSetSchema VARCHAR(255),  "
		  + "		DataTypeCategory VARCHAR(255),  "
		  + "		MeasureDate DATETIME DEFAULT GetDate()"
		  + ")";
		
		return output;
	}
	
	/***
	 * Creates table to store basic table statistics.
	 * 
	 * @return SQL String
	 */
	public static final String sqlCreateTableTblStats(String schema) {
		if(schema == null || schema.length() == 0) schema = "dbo";
		else schema = SqlServerDiscovery.sqlObjBracket(schema);
		String output = 
			"CREATE TABLE TblStats (  "
		  + "		CnString VARCHAR(512) NOT NULL,  "
		  + "		DbName VARCHAR(256) NOT NULL,  "
		  + "		SchName VARCHAR(256) NULL,  "
		  + "		TblName VARCHAR(256) NOT NULL,  "
		  + "		RecCount BIGINT NULL,  "
		  + "		ColCount INT NULL,  "
		  + "		QueryTimeSeconds INT NULL,  "
		  + "		MeasureDate DATETIME DEFAULT GETDATE()  "
		  + ")";
		return output;
	}
	
	
	/***
	 * Creates table to store basic column statistics.
	 * @return SQL String
	 */
	public static final String sqlCreateTableColStats(String schema) {
		if(schema == null || schema.length() == 0) schema = "dbo";
		else schema = SqlServerDiscovery.sqlObjBracket(schema);
		String output = 
			"CREATE TABLE ColStats (  "
		  + "		CnString VARCHAR(512) NOT NULL,  "
		  + "		DbName VARCHAR(256) NOT NULL,  "
		  + "		SchName VARCHAR(256) NULL,  "
		  + "		TblName VARCHAR(256) NOT NULL,  "
		  + "		ColName VARCHAR(256) NOT NULL,  "
		  + "		NullCount BIGINT NULL,  "
		  + "		BlankCount BIGINT NULL,  "
		  + "		UniqueCount BIGINT NULL,  "
		  + "		QueryTimeSeconds INT NULL,  "
		  + "		MeasureDate DATETIME DEFAULT GETDATE()  "
		  + ")";
				
		return output;		
	}
	
	/***
	 * Creates table to store search results based on a searched upon value.
	 * @return SQL String
	 */
	public static final  String sqlCreateTableColSearchResults(String schema) {
		if(schema == null || schema.length() == 0) schema = "dbo";
		else schema = SqlServerDiscovery.sqlObjBracket(schema);
		String output =
			"CREATE TABLE ColSearchResults (  "
		  + "		CnString VARCHAR(512) NOT NULL,  "
		  + "		DbName VARCHAR(256) NOT NULL,  "
		  + "		SchName VARCHAR(256) NULL,  "
		  + "		TblName VARCHAR(256) NOT NULL,  "
		  + "		ColName VARCHAR(256) NOT NULL,  "
		  + "		SearchTerm VARCHAR(MAX) NOT NULL,  "
		  + "		ResultValue VARCHAR(MAX) NULL,  "
		  + "		ResultCount BIGINT NULL,  "
		  + "		MeasureDate DATETIME DEFAULT GETDATE()  "
		  + ")";
		
		return output;
	}
	
	
	/***
	 * Creates table to store extended column statistics.
	 * @return SQL String
	 */
	public static final String sqlCreateTableColStatsExt(String schema) {
		if(schema == null || schema.length() == 0) schema = "dbo";
		else schema = SqlServerDiscovery.sqlObjBracket(schema);
		String output = 
			"CREATE TABLE " + schema + ".ColStatsExt (  "
		  + "		CnString VARCHAR(512) NOT NULL,  "
		  + "		DbName VARCHAR(256) NOT NULL,  "
		  + "		SchName VARCHAR(256) NULL,  "
		  + "		TblName VARCHAR(256) NOT NULL,  "
		  + "		ColName VARCHAR(256) NOT NULL,  "
		  + "		TextMin VARCHAR(MAX) NULL,  "
		  + "		Text25P VARCHAR(MAX) NULL,  "
		  + "		Text50P VARCHAR(MAX) NULL,  "
		  + "		Text75P VARCHAR(MAX) NULL,  "
		  + "		TextMax VARCHAR(MAX) NULL,  "
		  + "		TextMinLen BIGINT NULL,  "
		  + "		TextMaxLen BIGINT NULL,  "
		  + "		NumericMin FLOAT(53) NULL,  "
		  + "		NumericMax FLOAT(53) NULL,  "
		  + "		Numeric25P FLOAT(53) NULL,  "
		  + "		Numeric50P FLOAT(53) NULL,  "
		  + "		Numeric75P FLOAT(53) NULL,  "
		  + "		DateTimeMin DATETIME NULL,  "
		  + "		DateTime25P DATETIME NULL,  "
		  + "		DateTime50P DATETIME NULL,  "
		  + "		DateTime75P DATETIME NULL,  "
		  + "		DateTimeMax DATETIME NULL,  "
		  + "		MeasureDate DATETIME DEFAULT GETDATE()  "
		  + ")";
		
		return output;
	}
	
	/***
	 * Creates a view to help populate some of the values for TablStats derived from 
	 * the InformationSchema table
	 * @return SQL String
	 */
	public static final  String sqlCreateViewTblStats(String schema) {
		if(schema == null || schema.length() == 0) schema = "dbo";
		else schema = SqlServerDiscovery.sqlObjBracket(schema);
		String output = 
		    "CREATE VIEW " + schema + ".vwTblStats AS  "
		  + "SELECT CnString,  "
		  + "		TableCatalog,  "
		  + "		TableSchema,  "
		  + " 		TableName,  "
		  + " 		COUNT(*) RecCount  "
		  + "FROM InformationSchema  "
		  + "GROUP BY CnString, TableCatalog, TableSchema, TableName";
				
		return output;
	}
	
	

	/***
	 * Drops all objects associated with DB Scour
	 * @return SQL String
	 */
	public static String sqlDropAllObjects(String schema) {
		if(schema == null || schema.length() == 0) schema = "dbo";
		else schema = SqlServerDiscovery.sqlObjBracket(schema);
		String output =
			"DROP TABLE " + schema + ".InformationSchema  "
		  + "DROP TABLE " + schema + ".TblStats  "
		  + "DROP TABLE " + schema + ".ColStats  "
		  + "DROP TABLE " + schema + ".ColSearchResults  "
		  + "DROP TABLE " + schema + ".ColStatsExt  "
		  + "DROP VIEW " + schema + ".vwTblStats  ";
		return output;
	}
	
	/***
	 * 
	 * @param SQL String
	 * @return SQL String
	 */


}
