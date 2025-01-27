package jch.lib.cloud.snowflake;

import java.sql.SQLException;
import java.sql.Statement;

import jch.lib.log.QLog;


/***
 * A helper object that allows asynchronous call to Snowflake for multithreading
 * 
 * @author harrisonc
 *
 */
public	class ExecuteUpdateSnowflakeCommand extends Thread {
	public ExecuteUpdateSnowflakeCommand(String sqlCommand, java.sql.Connection sfConnection, long cCnt) {
		this.sqlCommand = sqlCommand;
		this.sfConnection = sfConnection;
		this.cCnt = cCnt;
	}
	
	@Override
	public void run() {
		Statement stmnt = null;
		
		try {
			if(sfConnection != null &&
			   sfConnection.isClosed() != true) {
				stmnt = sfConnection.createStatement();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ExecuteUpdateSnowflakeCommand Exception: " + e.toString(),true);
			QLog.log(e,true);
		}
		
		try {
			stmnt.executeUpdate(sqlCommand);
			QLog.log(cCnt + " executed!");
		} catch (SQLException e) {
			QLog.log(cCnt + " exception!");
			e.printStackTrace();
			QLog.log("ExecuteUpdateSnowflakeCommand Exception: " + e.toString(),true);
			QLog.log(e,true);;
		}
		
		try {
			stmnt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ExecuteUpdateSnowflakeCommand Exception: " + e.toString(),true);
			QLog.log(e,true);
		}
	}
	

	String sqlCommand;
	java.sql.Connection sfConnection;
	long cCnt;
}

