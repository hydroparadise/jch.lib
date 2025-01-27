package jch.lib.secrets;

import org.json.simple.JSONObject;

import jch.lib.log.QLog;

public class EmailCreds extends Creds {
	public EmailCreds() {
		
	}
	
	public EmailCreds(String jsonFilePath) {
		parseJson(jsonFilePath);
	}
	
	public EmailCreds(String jsonFilePath, String password) {
		parseJson(jsonFilePath, password);
	}
	
	public void parseJson(JSONObject jsonObj) {
		this.mailServer = (String) jsonObj.get("mailServer");
		this.mailPort = (long) jsonObj.get("mailPort");
		this.userName = (String) jsonObj.get("userName");
		this.password = (String) jsonObj.get("password");
		
		//sender and recipient optional
		try {
			this.sender = (String) jsonObj.get("sender");
			this.recipient = (String) jsonObj.get("recipient");
		}
		catch(Exception e) {
			
		}
	}
	


	public String getMailServer() {
		return mailServer;
	}

	public void setMailServer(String mailServer) {
		this.mailServer = mailServer;
	}

	public long getMailPort() {
		return mailPort;
	}

	public void setMailPort(int mailPort) {
		this.mailPort = mailPort;
	}

	public String getSender() {
		return sender;
	}

	public void setSender(String sender) {
		this.sender = sender;
	}

	public String getRecipient() {
		return recipient;
	}

	public void setRecipient(String recipient) {
		this.recipient = recipient;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}



	private String mailServer;
	private long mailPort;
	private String sender;
	private String recipient;
	private String userName;
	private String password;
	//private String jsonFilePath;
	
}