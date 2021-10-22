package jch.lib.db.sqlserver;
import java.sql.*;
import java.util.concurrent.*;
import javax.sql.*;
import javax.sql.rowset.*;

import com.sun.prism.impl.Disposer.Record;

/***
 * SqlServerDbSour can be used to get basic statics about a database and to search specific values.
 * The strategy is to make all "destructive" operations as static methods so that instance objects
 * aren't inadvertently ran.
 * 
 * @author harrisonc
 *
 */
public class SqlServerDbScour {
	
	int threadPoolSize = 3;
	
	
	
	/***
	 * 
	 * @param srcCnString
	 * @param destCnString
	 * @param destDbName
	 * @param destSchema
	 * @param tblStats
	 * @return
	 */
	public boolean updateDestinationColStats(String srcCnString, String destCnString, 
			String destDbName, String destSchema, RowSet tblStats) {
		
		boolean success = true;
		Connection destCn = null;
		ThreadPoolExecutor exe = null;
		
		try {
    		if(destSchema == null || destSchema.length() == 0) destSchema = "dbo";
    		else destSchema = SqlServerDiscovery.sqlObjBracket(destSchema);
    		       	
        	//Instantiate Thread pooler
        	exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);
        	
        	//Open Db Connection
        	destCn = DriverManager.getConnection(destCnString);            
        	Statement sta = destCn.createStatement();    		
    		    		
        	//makes sure we're on the right database before attempting any record modifications
        	sta.execute("USE " + destDbName);
        	//remove records based on the source connection string to reupdate to account for any changes.
        	sta.execute("DELETE FROM " + destSchema + ".ColStats WHERE CnString = " + SqlServerDiscovery.sqlStringClean(srcCnString));
        	
            //Begin RowSet Iteration
			while(tblStats.next()) {
				
				//Get fields from RowSet row
				TblStatsRecord tsr = new TblStatsRecord(
						tblStats.getString("CnString"),
						tblStats.getString("TableCatalog"),
						tblStats.getString("TableSchema"),						
						tblStats.getString("TableName"),
						tblStats.getInt("FieldCount")
				);
				
				UpdateColStatsThread t = new UpdateColStatsThread(
						 srcCnString,
						 destCnString,  
						 destDbName,  
						 destSchema, 
						 tsr);
				
				exe.submit(t);
			}
		}
        catch (SQLException ex) {ex.printStackTrace();success = false;} 
        //close connection
        finally {
        	try {if (destCn != null && !destCn.isClosed()) destCn.close();} 
        	catch (SQLException ex) {ex.printStackTrace();}
		}
		
		exe.shutdown();
		return success;
	}
	
	
	/***
	 * 
	 * @author harrisonc
	 *
	 */
	class UpdateColStatsThread extends Thread {
		/***
		 * 
		 * @param srcCnString
		 * @param destCnString
		 * @param destDbName
		 * @param destSchema
		 * @param record
		 */
		public UpdateColStatsThread(String srcCnString,	String destCnString, String destDbName, String destSchema, 
				TblStatsRecord record) {
			
			this.srcCnString = srcCnString;
			this.destCnString = destCnString;
			this.destDbName = destDbName;
			this.destSchema = destSchema;
			this.record = record;
		}
		
		@Override
		public void run() {
			
			Connection destCn = null;
			try {
				//establish source connection
	        	destCn = DriverManager.getConnection(destCnString);  
	        	
	        	//render SQL string for record count

	        	String sqlTableColumns = sqlTableInformationSchema(
	        			record.getCnString(), 		//srcCnString, 
	        			record.getTableCatalog(),	//srcDatabaseName, 
	        			record.getTableSchema(),	//srcSchema, 
	        			record.getTableName(),		//srcTableName, 
	        			destDbName, 
	        			destSchema);
	        	
	        	System.out.println(SqlServerDiscovery.sqlPrint(sqlTableColumns));
	        	
		        Statement sta = destCn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
		        ResultSet res = sta.executeQuery(sqlTableColumns);
		        
		        RowSetFactory rsf = RowSetProvider.newFactory();
		        CachedRowSet rs = rsf.createCachedRowSet();
		        rs.populate(res);
		        
		        //RowSet is now populated, go ahead and close db connection
		        destCn.close();
		                
				while(rs.next()) {
					ColStatsRecord columnStats = new ColStatsRecord(
							rs.getString("CnString"),//String cnString, 
							rs.getString("TableCatalog"),//String dbName, 
							rs.getString("TableSchema"),//String schName, 
							rs.getString("TableName"),//String tblName, 
							rs.getString("ColumnName")//String colName,
							);
					
					//send column stat record and the datatype category of it
					this.columnUpdate(columnStats, rs.getString("DataTypeCategory"));
				}
			}
			//close connection
	        catch (SQLException ex) {ex.printStackTrace();} 
	        finally {
	        	try {if (destCn != null && !destCn.isClosed()) destCn.close();} 
	        	catch (SQLException ex) {ex.printStackTrace();}
			}
			
			
		}
		
		void columnUpdate(ColStatsRecord columnStats, String dataTypeCategory) {
			Connection srcCn = null;
			
			//render SQL string for col stats
			String sqlColStats = SqlServerDbScour.sqlColStats(
	        			columnStats.getDbName(), 
	        			columnStats.getSchName(), 
	        			columnStats.getTblName(),
	        			columnStats.getColName(),
	        			dataTypeCategory);
			
			System.out.println(SqlServerDiscovery.sqlPrint(sqlColStats));
			
			try {
				//establish source connection
	        	srcCn = DriverManager.getConnection(columnStats.getCnString());  
	        	
	        	//start time measure
	        	double start = System.currentTimeMillis();
	        	
	        	
	        	//grab record count and store in
	        	Statement sta = srcCn.createStatement();
				ResultSet res = sta.executeQuery(sqlColStats);
				res.next();
				
	        	//stop time measure
				double stop = System.currentTimeMillis();
	        	
				columnStats.setNullCount(res.getLong("NullCount"));
				columnStats.setBlankCount(res.getLong("BlankCount"));
				columnStats.setUniqueCount(res.getLong("UniqueCount"));
				columnStats.setQueryTimeSeconds((stop - start)/1000.0);
				
				//record.setQueryTimeSeconds((stop - start)/1000.0);
				
			}
			//close connection
	        catch (SQLException ex) {ex.printStackTrace();} 
	        finally {
	        	try {if (srcCn != null && !srcCn.isClosed()) srcCn.close();} 
	        	catch (SQLException ex) {ex.printStackTrace();}
			}
			
			
			System.out.println(columnStats.toSqlInsert(destSchema));
			//next, insert in TblStats table
			
			Connection destCn = null;
			try {
				//establish connection
	        	destCn = DriverManager.getConnection(destCnString);
	        	
	        	Statement sta = destCn.createStatement();

	        	sta.execute("USE " + destDbName);
	        	sta.executeUpdate(columnStats.toSqlInsert(destSchema));
			}
	        catch (SQLException ex) {ex.printStackTrace();} 
	        finally {
	        	//close connection
	        	try {if (destCn != null && !destCn.isClosed()) destCn.close();} 
	        	catch (SQLException ex) {ex.printStackTrace();}
			}
			
		}
		
		final String srcCnString;
		final String destCnString;
		final String destDbName;
		final String destSchema;
		final TblStatsRecord record;
	}
	
	
	
	
	/***
	 * Updates destination TblStats table
	 * 1 Thread per source table
	 * @param Source Connection String Used as a value in table 
	 * @param destCnString (used to gather existing table from 
	 * @param destDbName
	 * @param destSchema
	 * @param RowSet from vwTblStats rendered from destination table InformationSchema
	 * @return
	 */
	public boolean updateDestinationTableStats(String srcCnString,	String destCnString, 
			String destDbName, String destSchema, RowSet tblStats) {
		
		boolean success = true;
		Connection destCn = null;
		ThreadPoolExecutor exe = null;;

		//Open connection
        try {
            destDbName = SqlServerDiscovery.sqlObjBracket(destDbName);
    		if(destSchema == null || destSchema.length() == 0) destSchema = "dbo";
    		else destSchema = SqlServerDiscovery.sqlObjBracket(destSchema);
    		       	
        	//Thread pooler
        	exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);

        	destCn = DriverManager.getConnection(destCnString);            
        	Statement sta = destCn.createStatement();    		
    		    		
        	//makes sure we're on the right database before attempting any record modifications
        	sta.execute("USE " + destDbName);
        	//remove records based on the source connection string to reupdate to account for any changes.
        	sta.execute("DELETE FROM " + destSchema + ".TblStats WHERE CnString = " + SqlServerDiscovery.sqlStringClean(srcCnString));
        	
            //Begin RowSet Iteration
			while(tblStats.next()) {
				
				//Get fields from RowSet row
				TblStatsRecord tsr = new TblStatsRecord(
						tblStats.getString("CnString"),
						tblStats.getString("TableCatalog"),
						tblStats.getString("TableSchema"),						
						tblStats.getString("TableName"),
						tblStats.getInt("FieldCount")
				);
				
				UpdateTblStatsThread t = new UpdateTblStatsThread(
						 srcCnString,
						 destCnString,  
						 destDbName,  
						 destSchema, 
						 tsr);
				
				exe.submit(t);
			}
        } 
        catch (SQLException ex) {ex.printStackTrace();success = false;} 
        //close connection
        finally {
        	try {if (destCn != null && !destCn.isClosed()) destCn.close();} 
        	catch (SQLException ex) {ex.printStackTrace();}
		}

        //finish all threads before moving on
        exe.shutdown();
        return success; 
	}
	
	
	/***
	 * A thread implemented class to allow for updating 
	 * @author harrisonc
	 */
	class UpdateTblStatsThread extends Thread {
		/***
		 * 
		 * @param srcCnString
		 * @param destCnString
		 * @param destDbName
		 * @param destSchema
		 * @param record
		 */
		public UpdateTblStatsThread(String srcCnString,	String destCnString, String destDbName, String destSchema, 
				TblStatsRecord record ) {
			
			this.srcCnString = srcCnString;
			this.destCnString = destCnString;
			this.destDbName = destDbName;
			this.destSchema = destSchema;
			this.record = record;
		}
		
		@Override
		public void run() {
			
			//first get record count
			Connection srcCn = null;
			try {
				//establish source connection
	        	srcCn = DriverManager.getConnection(srcCnString);  
	        	
	        	//render SQL string for record count
	        	Statement sta = srcCn.createStatement();
	        	String sqlRecordCount = SqlServerDbScour.sqlRecordCount(
	        			record.getTableCatalog(), record.getTableSchema(), record.getTableName());
	        	
	        	//start time measure
	        	double start = System.currentTimeMillis();
	        	
	        	//grab record count and store in
				ResultSet res = sta.executeQuery(sqlRecordCount);
				res.next();
				
	        	//stop time measure
				double stop = System.currentTimeMillis();
	        	
				record.setRecordCount(res.getLong("RecordCount"));
				record.setQueryTimeSeconds((stop - start)/1000.0);
				
			}
			//close connection
	        catch (SQLException ex) {ex.printStackTrace();} 
	        finally {
	        	try {if (srcCn != null && !srcCn.isClosed()) srcCn.close();} 
	        	catch (SQLException ex) {ex.printStackTrace();}
			}
			
			//next, insert in TblStats table
			Connection destCn = null;
			try {
				//establish connection
	        	destCn = DriverManager.getConnection(destCnString);
	        	
	        	Statement sta = destCn.createStatement();
	        	System.out.println(record.toSqlInsert(destSchema));
	        	sta.execute("USE " + destDbName);
	        	sta.executeUpdate(record.toSqlInsert(destSchema));
			}
	        catch (SQLException ex) {ex.printStackTrace();} 
	        finally {
	        	//close connection
	        	try {if (destCn != null && !destCn.isClosed()) destCn.close();} 
	        	catch (SQLException ex) {ex.printStackTrace();}
			}
			
		}
		
		final String srcCnString;
		final String destCnString;
		final String destDbName;
		final String destSchema;
		final TblStatsRecord record;
	}
	
	
	
	/***
	 * 
	 * @param A Connection String used to establish a connection where the vwTableStats view resides
	 * @param A Database Name String used to establish a connection where the vwTableStats view resides
	 * @param The Schema Name of the vwTableStats view
	 * @param Filtering String targeting Sourced Connection String
	 * @return A Cached RowSet of the vwTableStats view
	 */
	public RowSet getDestVwTblStats(String srcCnString, String destCnString, String destDbName, String destSchema) {
		Connection cn = null;  //connection
		CachedRowSet rs = null;
		
		//Open connection, run sql statements
        try {
        	
            cn = DriverManager.getConnection(destCnString);  
            Statement sta = cn.createStatement();
            sta.execute("USE " + destDbName);
            
            sta = cn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            String sql = SqlServerDbScour.sqlSelectVwTblStats(srcCnString, null, destSchema);
  
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
	 * 
	 * @param Source Connection String Used as a value in table to update on (include databaseName recommended)
	 * @param Destination Connection String (used to perform update in table)
	 * @param Destination Database Name String (database to store results, won't assume catalog name in destCnString) 
	 * @param Destination Schema String (table schema in case not default "dbo")
	 * @param Information Schema RowSet (CachedRowSet)
	 * @return
	 */
	public boolean updateDestInformationSchema(String srcCnString,	String destCnString, String destDbName, 
			String destSchema, RowSet infSch) {
		boolean success = false;
		Connection cn = null;  //connection
		try {
			
			
            cn = DriverManager.getConnection(destCnString);            
            Statement sta = cn.createStatement();
            
            destDbName = SqlServerDiscovery.sqlObjBracket(destDbName);
    		if(destSchema == null || destSchema.length() == 0) destSchema = "dbo";
    		else destSchema = SqlServerDiscovery.sqlObjBracket(destSchema);
    		
    		//makes sure we're on the right database before attempting any record modifications
            sta.execute("USE " + destDbName);
            
            //remove records based on the source connection string to reupdate to account for any changes.
            //System.out.println("DELETE FROM " + destSchema + ".InformationdestSchema WHERE CnString = " + SqlServerDiscovery.sqlStringClean(srcCnString));
            sta.execute("DELETE FROM " + destSchema + ".InformationSchema WHERE CnString = " 
            		+ SqlServerDiscovery.sqlStringClean(srcCnString));
            
            //Begin RowSet Iteration
			while(infSch.next()) {

				//TODO: use InformationSchemaRecord
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
				StringBuilder sqlInsert = new StringBuilder("INSERT INTO " + destSchema + ".InformationSchema (");
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
				success = true;
			}
		}
		catch (SQLException ex) {ex.printStackTrace();}
		
		return success;
	}
	
	
	/***
	 * 
	 * @param Source  String
	 * @param Source Database Name
	 * @param Boolean includeViews
	 * @return Information Schema RowSet
	 */
	public RowSet getSrcInformationSchema(String srcCnString, String srcDbName, boolean includeViews) {
        //String sqlAllDatabase = SqlServerDiscovery.sqlAllUserDatabases();

		Connection cn = null;  //connection
		CachedRowSet rs = null;
		
		//Open connection, run drop sql statements
        try {
        	
            cn = DriverManager.getConnection(srcCnString);  
            
            //Either grab only user tables or both tables and views.
            String sql;
            if(includeViews == true) sql = SqlServerDiscovery.sqlDbTableViewColumns(srcDbName);
            else sql = SqlServerDiscovery.sqlDbTableColumns(srcDbName);
            
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
	 * @return True if successful, False if unsuccessful
	 */
	public static boolean createDbScourOjbects(String destCnString, String destDbName, String destSchema) {
		Connection cn = null;  //connection
		boolean success = false;
        try {
            cn = DriverManager.getConnection(destCnString);            
            Statement sta = cn.createStatement();
            destDbName = SqlServerDiscovery.sqlObjBracket(destDbName);
            sta.execute("USE " + destDbName);
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableInformationSchema(destSchema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableTblStats(destSchema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColStats(destSchema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColSearchResults(destSchema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateTableColStatsExt(destSchema));
            sta.executeUpdate(SqlServerDbScour.sqlCreateViewTblStats(destSchema));
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
	 * @return True if successful, False if unsuccessful
	 */
	public static boolean dropDbScourOjbects(String destCnString, String destDbName, String destSchema) {
		Connection cn = null;  //connection
		boolean success = false;
		
		//Open connection, run drop sql statements
        try {
            cn = DriverManager.getConnection(destCnString);            
            Statement sta = cn.createStatement();
            destDbName = SqlServerDiscovery.sqlObjBracket(destDbName);
            sta.execute("USE " + destDbName);
            sta.executeUpdate(SqlServerDbScour.sqlDropAllObjects(destSchema));
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
	 * 
	 * @param srcCnString
	 * @param srcDatabaseName
	 * @param srcSchema
	 * @param srcTableName
	 * @param destDbName
	 * @param destSchema
	 * @return
	 */
	public static String sqlTableInformationSchema(String srcCnString, String srcDatabaseName, String srcSchema, String srcTableName, 
			String destDbName, String destSchema) {
		String output = null;
		String from = SqlServerDiscovery.sqlFromClean(destDbName, destSchema, "InformationSchema");
		output = "SELECT CnString,TableType,TableCatalog,TableSchema,TableName,ColumnName,ConstraintName,DataType,  "
			   + "	OrdinalPosition,ColumnDefault,IsNullable,CharacterMaximumLength,CharacterOctetLength,NumericPrecision,  "
			   + "	NumericPrecisionRadix,NumericScale,DateTimePrecision,CharacterSetName,CollationCatalog,CollationSchema,  "
			   + "	CollationName,DomainCatalog,DomainSchema,DomainName,CharacterSetCatalog,CharacterSetSchema,DataTypeCategory,  "
			   + "	MeasureDate"
			   + "  FROM " + from
			   + "  WHERE TableName = " + SqlServerDiscovery.sqlStringClean(srcTableName)
			   + "  	AND TableSchema = " + SqlServerDiscovery.sqlStringClean(srcSchema)
			   + "  	AND TableCatalog = " + SqlServerDiscovery.sqlStringClean(srcDatabaseName)
			   + "  	AND CnString = " + SqlServerDiscovery.sqlStringClean(srcCnString)
			   + "  ORDER BY OrdinalPosition";
		
		return output;
	}
	
	public static String sqlColStats(String dbName, String schema, String tableName, String columnName, String dataTypeCategory) {

		StringBuilder output = new StringBuilder();
		output.append("SELECT  ");
		output.append(" (" + sqlNullCount(dbName, schema, tableName, columnName) + ") NullCount,");
		
		if(dataTypeCategory == "TEXT")
			output.append(" (" + sqlBlankCount(dbName, schema, tableName, columnName) + ") BlankCount,");
		else
			output.append("null BlankCount,");
		
		output.append(" (" + sqlUniqueCount(dbName, schema, tableName, columnName) + ") UniqueCount");
		
		return output.toString();
	}
	
	/***
	 * SQL Statement that returns the field RecordCount
	 * @param Database Name of Table (Optional, but omitted if schema is omitted)
	 * @param Scema Name of Table (Optional)
	 * @param tableName
	 * @param Column Name
	 * @return
	 */
	public static String sqlUniqueCount(String dbName, String schema, String tableName, String columnName) {
		String output = null;
		String from = null;
		
		//if talbleName not valid, allb ets off
		from = SqlServerDiscovery.sqlFromClean(dbName, schema, tableName);
		
		output = "SELECT COUNT(DISTINCT "+ SqlServerDiscovery.sqlObjBracket(columnName) +") RecordCount FROM " + from;
		return output;
	}
	
	
	/***
	 * 
	 * @param Database Name of Table (Optional, but omitted if schema is omitted)
	 * @param Scema Name of Table (Optional)
	 * @param tableName
	 * @param Column Name
	 * @return
	 */
	public static String sqlBlankCount(String dbName, String schema, String tableName, String columnName) {
		String output = null;
		
		output = sqlRecordCount(dbName, schema, tableName) 
				+ "  WHERE " + SqlServerDiscovery.sqlObjBracket(columnName) + " = ''";
		return output;
	}
	
	
	/***
	 * SQL Statement that returns the field RecordCount
	 * @param Database Name of Table (Optional, but omitted if schema is omitted)
	 * @param Scema Name of Table (Optional)
	 * @param tableName
	 * @param Column Name
	 * @return
	 */
	public static String sqlNullCount(String dbName, String schema, String tableName, String columnName) {
		String output = null;
		
		output = sqlRecordCount(dbName, schema, tableName) 
				+ "  WHERE " + SqlServerDiscovery.sqlObjBracket(columnName) + " IS NULL";
		return output;
	}
	
	/***
	 * SQL Statement that returns the field RecordCount
	 * @param Database Name of Table (Optional, but omitted if schema is omitted)
	 * @param Scema Name of Table (Optional)
	 * @param tableName
	 * @return
	 */
	public static String sqlRecordCount(String dbName, String schema, String tableName) {
		String output = null;
		String from = null;
		
		from = SqlServerDiscovery.sqlFromClean(dbName, schema, tableName);
		
		output = "SELECT COUNT(*) RecordCount FROM " + from;
		return output;
	}
	
	/***
	 * SQL Statement that returns the field RecordCount
	 * @param srcCnString
	 * @param srcDbName
	 * @param schema
	 * @return
	 */
	public static String sqlSelectVwTblStats(String srcCnString, String srcDbName, String srcSchema) {
		String output = null;
		if(srcSchema == null || srcSchema.length() == 0) srcSchema = "dbo";
		else srcSchema = SqlServerDiscovery.sqlObjBracket(srcSchema);
		
		output = 
				"SELECT CnString,TableCatalog,TableSchema,TableName,FieldCount  "
			  + "FROM " + srcSchema + ".vwTblStats  "
			  + "WHERE CnString = '" + srcCnString + "'";
		if(srcDbName != null && srcDbName.length() > 0) {
			output = output + "  	AND TabelCatalog = " + srcDbName + "'";
		}
		
		return output;
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
			"CREATE TABLE " + schema + ".InformationSchema (  "
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
			"CREATE TABLE " + schema + ".TblStats (  "
		  + "		CnString VARCHAR(512) NOT NULL,  "
		  + "		DbName VARCHAR(256) NOT NULL,  "
		  + "		SchName VARCHAR(256) NULL,  "
		  + "		TblName VARCHAR(256) NOT NULL,  "
		  + "		RecCount BIGINT NULL,  "
		  + "		ColCount INT NULL,  "
		  + "		QueryTimeSeconds FLOAT(53) NULL,  "
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
			"CREATE TABLE " + schema + ".ColStats (  "
		  + "		CnString VARCHAR(512) NOT NULL,  "
		  + "		DbName VARCHAR(256) NOT NULL,  "
		  + "		SchName VARCHAR(256) NULL,  "
		  + "		TblName VARCHAR(256) NOT NULL,  "
		  + "		ColName VARCHAR(256) NOT NULL,  "
		  + "		NullCount BIGINT NULL,  "
		  + "		BlankCount BIGINT NULL,  "
		  + "		UniqueCount BIGINT NULL,  "
		  + "		QueryTimeSeconds FLOAT(53) NULL,  "
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
			"CREATE TABLE " + schema + ".ColSearchResults (  "
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
		  + " 		COUNT(*) FieldCount  "
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
	
	
	public class ColStatsRecord {
		public ColStatsRecord(String cnString, String dbName, String schName, String tblName, String colName,
				Long nullCount, Long blankCount, Long uniqueCount, Double queryTimeSeconds) {
			super();
			this.cnString = cnString;
			this.dbName = dbName;
			this.schName = schName;
			this.tblName = tblName;
			this.colName = colName;
			this.nullCount = nullCount;
			this.blankCount = blankCount;
			this.uniqueCount = uniqueCount;
			this.queryTimeSeconds = queryTimeSeconds;
		}
		public ColStatsRecord(String cnString, String dbName, String schName, String tblName, String colName) {
			super();
			this.cnString = cnString;
			this.dbName = dbName;
			this.schName = schName;
			this.tblName = tblName;
			this.colName = colName;
			// TODO Auto-generated constructor stub
		}
		
		public String toSqlInsert(String schema) {
			StringBuilder sqlInsert = new StringBuilder(); 
			sqlInsert.append("INSERT INTO " + schema + ".ColStats (");
			sqlInsert.append("CnString,DbName,SchName,TblName,ColName,NullCount,BlankCount,UniqueCount,"
					+ "QueryTimeSeconds)  VALUES (");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(cnString) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(dbName) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(schName) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(tblName) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(colName) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlLongClean(nullCount) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlLongClean(blankCount) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlLongClean(uniqueCount) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlDoubleClean(queryTimeSeconds) + ")");
			return sqlInsert.toString();
		}
		
		public String getCnString() {
			return cnString;
		}
		public void setCnString(String cnString) {
			this.cnString = cnString;
		}
		public String getDbName() {
			return dbName;
		}
		public void setDbName(String dbName) {
			this.dbName = dbName;
		}
		public String getSchName() {
			return schName;
		}
		public void setSchName(String schName) {
			this.schName = schName;
		}
		public String getTblName() {
			return tblName;
		}
		public void setTblName(String tblName) {
			this.tblName = tblName;
		}
		public String getColName() {
			return colName;
		}
		public void setColName(String colName) {
			this.colName = colName;
		}
		public Long getNullCount() {
			return nullCount;
		}
		public void setNullCount(Long nullCount) {
			this.nullCount = nullCount;
		}
		public Long getBlankCount() {
			return blankCount;
		}
		public void setBlankCount(Long blankCount) {
			this.blankCount = blankCount;
		}
		public Long getUniqueCount() {
			return uniqueCount;
		}
		public void setUniqueCount(Long uniqueCount) {
			this.uniqueCount = uniqueCount;
		}
		public Double getQueryTimeSeconds() {
			return queryTimeSeconds;
		}
		public void setQueryTimeSeconds(Double queryTimeSeconds) {
			this.queryTimeSeconds = queryTimeSeconds;
		}

		String cnString;
		String dbName;
		String schName;
		String tblName;
		String colName;
		Long nullCount;
		Long blankCount;
		Long uniqueCount;
		Double queryTimeSeconds;
		
	}
	
	public class TblStatsRecord {
		public TblStatsRecord(String cnString, String tableCatalog, String tableSchema, String tableName,
				Long recordCount, Integer fieldCount, Double queryTimeSeconds) {
			super();
			this.cnString = cnString;
			this.dbName = tableCatalog;
			this.schName = tableSchema;
			this.tblName = tableName;
			this.recCount = recordCount;
			this.colCount = fieldCount;
			this.queryTimeSeconds = queryTimeSeconds;
		}
		
		public TblStatsRecord(String cnString, String tableCatalog, String tableSchema, String tableName, 
				Integer fieldCount) {
			this.cnString = cnString;
			this.dbName = tableCatalog;
			this.schName = tableSchema;
			this.tblName = tableName;
			this.colCount = fieldCount;
			// TODO Auto-generated constructor stub
		}
		
		public final String toSqlInsert(String schema) {
			StringBuilder sqlInsert = new StringBuilder(); 
			sqlInsert.append("INSERT INTO " + schema + ".TblStats (");
			sqlInsert.append("CnString,DbName,SchName,TblName,RecCount,ColCount,QueryTimeSeconds)  VALUES (");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(cnString) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(dbName) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(schName) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(tblName) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlLongClean(recCount) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlIntClean(colCount) + ",");
			sqlInsert.append(SqlServerDiscovery.sqlDoubleClean(queryTimeSeconds) + ")");
			return sqlInsert.toString();
		}

		public String getCnString() {
			return cnString;
		}
		public void setCnString(String cnString) {
			this.cnString = cnString;
		}
		public String getTableCatalog() {
			return dbName;
		}
		public void setTableCatalog(String tableCatalog) {
			this.dbName = tableCatalog;
		}
		public String getTableSchema() {
			return schName;
		}
		public void setTableSchema(String tableSchema) {
			this.schName = tableSchema;
		}
		public String getTableName() {
			return tblName;
		}
		public void setTableName(String tableName) {
			this.tblName = tableName;
		}
		public Long getRecordCount() {
			return recCount;
		}
		public void setRecordCount(Long recordCount) {
			this.recCount = recordCount;
		}
		public Integer getFieldCount() {
			return colCount;
		}
		public void setFieldCount(Integer fieldCount) {
			this.colCount = fieldCount;
		}

		public Double getQueryTimeSeconds() {
			return queryTimeSeconds;
		}
		public void setQueryTimeSeconds(Double queryTimeSeconds) {
			this.queryTimeSeconds = queryTimeSeconds;
		}

		String cnString; 
		String dbName;
		String schName;
		String tblName;
		Long recCount;
		Integer colCount;
		Double queryTimeSeconds;
		
	}
	
	public class InformationSchemaRecord{
		public InformationSchemaRecord(String cnString, String tableType, String tableCatalog, String tableSchema,
				String tableName, String columnName, String constraintName, String dataType, Integer ordinalPosition,
				String columnDefault, String isNullable, Integer characterMaximumLength, Integer characterOctetLength,
				Integer numericPrecision, Integer numericPrecisionRadix, Integer numericScale,
				Integer dateTimePrecision, String characterSetName, String collationCatalog, String collationSchema,
				String collationName, String domainCatalog, String domainSchema, String domainName,
				String characterSetCatalog, String characterSetSchema, String dataTypeCat) {
			super();
			this.cnString = cnString;
			this.tableType = tableType;
			this.tableCatalog = tableCatalog;
			this.tableSchema = tableSchema;
			this.tableName = tableName;
			this.columnName = columnName;
			this.constraintName = constraintName;
			this.dataType = dataType;
			this.ordinalPosition = ordinalPosition;
			this.columnDefault = columnDefault;
			this.isNullable = isNullable;
			this.characterMaximumLength = characterMaximumLength;
			this.characterOctetLength = characterOctetLength;
			this.numericPrecision = numericPrecision;
			this.numericPrecisionRadix = numericPrecisionRadix;
			this.numericScale = numericScale;
			this.dateTimePrecision = dateTimePrecision;
			this.characterSetName = characterSetName;
			this.collationCatalog = collationCatalog;
			this.collationSchema = collationSchema;
			this.collationName = collationName;
			this.domainCatalog = domainCatalog;
			this.domainSchema = domainSchema;
			this.domainName = domainName;
			this.characterSetCatalog = characterSetCatalog;
			this.characterSetSchema = characterSetSchema;
			this.dataTypeCat = dataTypeCat;
		}
		
		public final String toSqlInsert(String schema) {
			if(schema == null || schema.length() == 0) schema = "dbo";
			else schema = SqlServerDiscovery.sqlObjBracket(schema);
			StringBuilder sqlInsert = new StringBuilder("INSERT INTO " + schema + ".InformationSchema (");
			sqlInsert.append("CnString,TableType,TableCatalog,TableSchema,TableName,ColumnName,ConstraintName,DataType,OrdinalPosition,"
					       + "ColumnDefault,IsNullable,CharacterMaximumLength,CharacterOctetLength,NumericPrecision,NumericPrecisionRadix,"
					       + "NumericScale,DateTimePrecision,CharacterSetName,CollationCatalog,CollationSchema,CollationName,DomainCatalog,"
					       + "DomainSchema,DomainName,CharacterSetCatalog,CharacterSetSchema,DataTypeCategory)  VALUES(");
			
			sqlInsert.append(SqlServerDiscovery.sqlStringClean(cnString) + ", ");
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
			return sqlInsert.toString();
		}
		


		public String getCnString() {
			return cnString;
		}

		public void setCnString(String cnString) {
			this.cnString = cnString;
		}

		public String getTableType() {
			return tableType;
		}

		public void setTableType(String tableType) {
			this.tableType = tableType;
		}

		public String getTableCatalog() {
			return tableCatalog;
		}

		public void setTableCatalog(String tableCatalog) {
			this.tableCatalog = tableCatalog;
		}

		public String getTableSchema() {
			return tableSchema;
		}

		public void setTableSchema(String tableSchema) {
			this.tableSchema = tableSchema;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public String getColumnName() {
			return columnName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public String getConstraintName() {
			return constraintName;
		}

		public void setConstraintName(String constraintName) {
			this.constraintName = constraintName;
		}

		public String getDataType() {
			return dataType;
		}

		public void setDataType(String dataType) {
			this.dataType = dataType;
		}

		public Integer getOrdinalPosition() {
			return ordinalPosition;
		}

		public void setOrdinalPosition(Integer ordinalPosition) {
			this.ordinalPosition = ordinalPosition;
		}

		public String getColumnDefault() {
			return columnDefault;
		}

		public void setColumnDefault(String columnDefault) {
			this.columnDefault = columnDefault;
		}

		public String getIsNullable() {
			return isNullable;
		}

		public void setIsNullable(String isNullable) {
			this.isNullable = isNullable;
		}

		public Integer getCharacterMaximumLength() {
			return characterMaximumLength;
		}

		public void setCharacterMaximumLength(Integer characterMaximumLength) {
			this.characterMaximumLength = characterMaximumLength;
		}

		public Integer getCharacterOctetLength() {
			return characterOctetLength;
		}

		public void setCharacterOctetLength(Integer characterOctetLength) {
			this.characterOctetLength = characterOctetLength;
		}

		public Integer getNumericPrecision() {
			return numericPrecision;
		}

		public void setNumericPrecision(Integer numericPrecision) {
			this.numericPrecision = numericPrecision;
		}

		public Integer getNumericPrecisionRadix() {
			return numericPrecisionRadix;
		}

		public void setNumericPrecisionRadix(Integer numericPrecisionRadix) {
			this.numericPrecisionRadix = numericPrecisionRadix;
		}

		public Integer getNumericScale() {
			return numericScale;
		}

		public void setNumericScale(Integer numericScale) {
			this.numericScale = numericScale;
		}

		public Integer getDateTimePrecision() {
			return dateTimePrecision;
		}

		public void setDateTimePrecision(Integer dateTimePrecision) {
			this.dateTimePrecision = dateTimePrecision;
		}

		public String getCharacterSetName() {
			return characterSetName;
		}

		public void setCharacterSetName(String characterSetName) {
			this.characterSetName = characterSetName;
		}

		public String getCollationCatalog() {
			return collationCatalog;
		}

		public void setCollationCatalog(String collationCatalog) {
			this.collationCatalog = collationCatalog;
		}

		public String getCollationSchema() {
			return collationSchema;
		}

		public void setCollationSchema(String collationSchema) {
			this.collationSchema = collationSchema;
		}

		public String getCollationName() {
			return collationName;
		}

		public void setCollationName(String collationName) {
			this.collationName = collationName;
		}

		public String getDomainCatalog() {
			return domainCatalog;
		}

		public void setDomainCatalog(String domainCatalog) {
			this.domainCatalog = domainCatalog;
		}

		public String getDomainSchema() {
			return domainSchema;
		}

		public void setDomainSchema(String domainSchema) {
			this.domainSchema = domainSchema;
		}

		public String getDomainName() {
			return domainName;
		}

		public void setDomainName(String domainName) {
			this.domainName = domainName;
		}

		public String getCharacterSetCatalog() {
			return characterSetCatalog;
		}

		public void setCharacterSetCatalog(String characterSetCatalog) {
			this.characterSetCatalog = characterSetCatalog;
		}

		public String getCharacterSetSchema() {
			return characterSetSchema;
		}

		public void setCharacterSetSchema(String characterSetSchema) {
			this.characterSetSchema = characterSetSchema;
		}

		public String getDataTypeCat() {
			return dataTypeCat;
		}

		public void setDataTypeCat(String dataTypeCat) {
			this.dataTypeCat = dataTypeCat;
		}



		String cnString;
		String tableType;							//TableType
		String tableCatalog;					//TableCatalog
		String tableSchema;						//TableSchema
		String tableName;							//TableName
		String columnName;						//ColumnName
		String constraintName;				//ConstraintName
		String dataType;							//DataType
		Integer ordinalPosition;				//OrdinalPosition
		String columnDefault;					//ColumnDefault
		String isNullable;						//IsNullable
		Integer characterMaximumLength;	//CharacterMaximumLength
		Integer characterOctetLength;	 	//CharacterOctetLength
		Integer numericPrecision;				//NumericPrecision
		Integer numericPrecisionRadix;	//NumericPrecisionRadix
		Integer numericScale;						//NumericScale
		Integer dateTimePrecision;			//DateTimePrecision
		String characterSetName;			//CharacterSetName
		String collationCatalog;			//CollationCatalog
		String collationSchema;				//CollationSchema
		String collationName;					//CollationName
		String domainCatalog;					//DomainCatalog
		String domainSchema;					//DomainSchema
		String domainName;						//DomainName
		String characterSetCatalog;		//CharacterSetCatalog
		String characterSetSchema;		//CharacterSetSchema
		String dataTypeCat;						//DataTypeCategory
	}
	
	
}
