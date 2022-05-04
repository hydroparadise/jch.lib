package jch.lib.db.snowflake;

import java.sql.SQLException;
import java.sql.Statement;

import jch.lib.common.QLog;

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
		}
		
		try {
			stmnt.executeUpdate(sqlCommand);
			QLog.log(cCnt + " executed!");
		} catch (SQLException e) {
			QLog.log(cCnt + " exception!");
			e.printStackTrace();
			QLog.log(e.toString(),true);
		}
		
		try {
			stmnt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	String sqlCommand;
	java.sql.Connection sfConnection;
	long cCnt;
}

