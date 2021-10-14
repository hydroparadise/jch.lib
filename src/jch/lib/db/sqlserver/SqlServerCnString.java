package jch.lib.db.sqlserver;

public class SqlServerCnString {

	/***
	 * Sets parameters related to an integrated security connection and attempts to generate, set, and return a valid connection string
	 * for the source database connection.
	 * Example:
	 * "jdbc:sqlserver://vm-devanalytics;databaseName=dev01;integratedSecurity=true";
	 * 
	 * 
	 * @param String Source Hostname (Required)
	 * @param String Source SQL Server Instance Name (Optional)
	 * @param String Source Database Name (Optional, but recommended)
	 * @return Connections String
	 */
	public String setCnStringIntegratedSecurity(String hostName, String sqlServerInstanceName, String databaseName) {
		StringBuilder cnString = null;
		
		//returns true if HostName is valid
		//all bets off if this isn't filled in
		if(this.setHostName(hostName)) {
			
			//method indicates the use of integrated security
			this.setIntegratedSecurity(true);	
			
			//start cn string
			cnString = new StringBuilder("jdbc:sqlserver://" + this.getHostName());

		}
		
		//one more check point
		if(cnString != null) {
			//add Instance Name
			if(this.setSqlServerInstanceName(sqlServerInstanceName)) {
				cnString.append("\\" + this.getSqlServerInstanceName());
			}
			
			//Add database name
			if(this.setDatabaseName(databaseName)) {
				cnString.append(";databaseName=" + this.getDatabaseName());
			}
			
			//set integrated security
			cnString.append(";integratedSecurity=true");
			
			this.setCnString(cnString.toString());
		}
		
		return this.getCnString();
	}

	
	
	public String getHostName() {
		return hostName;
	}

	public boolean setHostName(String HostName) {
		if(HostName != null && 
		   HostName.length() > 0) {
			this.hostName = HostName;
			return true;
		}
		else return false;
	}


	public String getSqlServerInstanceName() {
		return sqlServerInstanceName;
	}


	public boolean setSqlServerInstanceName(String SqlServerInstanceName) {
		
		if(SqlServerInstanceName != null && 
		   SqlServerInstanceName.length() > 0) {
			this.sqlServerInstanceName = SqlServerInstanceName;		
			return true;
		}
		else return false;
		
	}

	public String getDatabaseName() {
		return databaseName;
	}


	public boolean setDatabaseName(String DatabaseName) {
		if(DatabaseName != null && 
		   DatabaseName.length() > 0) {
			this.databaseName = DatabaseName;
			return true;
		}
		else return false;
	}


	public boolean getIntegratedSecurity() {
		return integratedSecurity;
	}


	public void setIntegratedSecurity(boolean IntegratedSecurity) {
		this.integratedSecurity = IntegratedSecurity;
	}


	public String getUser() {
		return userName;
	}


	public void setUser(String userName) {
		this.userName = userName;
	}


	public String getPassord() {
		return password;
	}


	public void setSrPassord(String Passord) {
		this.password = password;
	}


	public String getCnString() {
		return cnString;
	}


	public void setCnString(String cnString) {
		this.cnString = cnString;
	}



	//connection string related fields
	String hostName = null;
	String sqlServerInstanceName = null;
	String databaseName = null;
	boolean integratedSecurity = false;
	String userName = null;
	String password = null;
	String cnString = null;
}
