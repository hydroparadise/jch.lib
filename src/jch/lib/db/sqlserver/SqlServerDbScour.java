package jch.lib.db.sqlserver;
import java.sql.*;

/***
 * SqlServerDbSour can be used to get basic statics about a database and to search specific values.
 * 
 * @author harrisonc
 *
 */
public class SqlServerDbScour {
	

	

	
	int threadPoolSize = 10;
	
	/***
	 * 
	 * @param destCnString
	 * @return True if successful, False if unsuccesful
	 */
	public static boolean createDbScourOjbects(String destCnString) {
		Connection cn = null;  //connection
		
        try {
            cn = DriverManager.getConnection(destCnString);            
            Statement sta = cn.createStatement();
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableInformationSchema());
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableTblStats());
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColStats());
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColSearchResults());
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColStatsExt());
            sta.executeUpdate(SqlServerDbScour.sqlCreateViewTblStats());
            
            return true;
 
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (cn != null && !cn.isClosed()) {
                    cn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return false;
	}

	
	/***
	 * Creates table to store results from INFORMATION_SCHEMA from 
	 * SqlSeverDiscovery.sqlAllTableViewColumnsBase() and it's derivatives:
	 * (sqlAllTableViewColumns,sqlAllTableColumns,sqlAllViewColumns)
	 * @return SQL String
	 */
	public static final String sqlCreateTableInformationSchema() {
		String output = 
			"CREATE TABLE InformationSchema (  "
		  + "		CN_STRING VARCHAR(512),  "
		  + "		TABLE_TYPE VARCHAR(255),  "
		  + "		TABLE_CATALOG VARCHAR(255),  "
		  + "		TABLE_SCHEMA VARCHAR(255),  "
		  + "		TABLE_NAME VARCHAR(255),  "
		  + "		COLUMN_NAME VARCHAR(255),  "
		  + "		CONSTRAINT_NAME VARCHAR(255),  "
		  + "		DATA_TYPE VARCHAR(255),  "
		  + "		ORDINAL_POSITION INT,  "
		  + "		COLUMN_DEFAULT VARCHAR(255),  "
		  + "		IS_NULLABLE VARCHAR(255),  "
		  + "		CHARACTER_MAXIMUM_LENGTH INT,  "
		  + "		CHARACTER_OCTET_LENGTH INT,  "
		  + "		NUMERIC_PRECISION INT,  "
		  + "		NUMERIC_PRECISION_RADIX INT,  "
		  + "		NUMERIC_SCALE INT,  "
		  + "		DATETIME_PRECISION VARCHAR(255),  "
		  + "		CHARACTER_SET_NAME VARCHAR(255),  "
		  + "		COLLATION_CATALOG VARCHAR(255),  "
		  + "		COLLATION_SCHEMA VARCHAR(255),  "
		  + "		COLLATION_NAME VARCHAR(255),  "
		  + "		DOMAIN_CATALOG VARCHAR(255),  "
		  + "		DOMAIN_SCHEMA VARCHAR(255),  "
		  + "		DOMAIN_NAME VARCHAR(255),  "
		  + "		CHARACTER_SET_CATALOG VARCHAR(255),  "
		  + "		CHARACTER_SET_SCHEMA VARCHAR(255),  "
		  + "		DATA_TYPE_CAT VARCHAR(255)  "
		  + ")";
		
		return output;
	}
	
	/***
	 * Creates table to store basic table statistics.
	 * @return SQL String
	 */
	public static final String sqlCreateTableTblStats() {
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
	public static final String sqlCreateTableColStats() {
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
	public static final  String sqlCreateTableColSearchResults() {
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
	public static final String sqlCreateTableColStatsExt() {
		String output = 
			"CREATE TABLE ColStatsExt (  "
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
	public static final  String sqlCreateViewTblStats() {
		String output = 
		    "CREATE VIEW vwTblStats AS  "
		  + "SELECT CN_STRING CnString,  "
		  + "		TABLE_CATALOG DbName,  "
		  + "		TABLE_SCHEMA SchName,  "
		  + " 	TABLE_NAME TblName,  "
		  + " 	COUNT(*) RecCount  "
		  + "FROM InformationSchema  "
		  + "GROUP BY CN_STRING, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME";
				
		return output;
	}
	
	

	/***
	 * Drops all objects associated with DB Scour
	 * @return SQL String
	 */
	public static String sqlDropAllObjects() {
		String output =
			"DROP TABLE InformationSchema  "
		  + "DROP TABLE TblStats  "
		  + "DROP TABLE ColStats  "
		  + "DROP TABLE ColSearchResults  "
		  + "DROP TABLE ColStatsExt  "
		  + "DROP VIEW vwTblStats  ";
		return output;
	}
	
	/***
	 * 
	 * @param SQL String
	 * @return SQL String
	 */
	public static String sqlPrint(String dbScourSql) {
		return dbScourSql.replace("  ", "\n");
	}

}
