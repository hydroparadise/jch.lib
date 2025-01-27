package jch.lib.secrets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jch.lib.log.QLog;

public class FtpCreds extends Creds {
	public FtpCreds() {
		
	}
	
	public FtpCreds(String jsonFilePath, String password) {
		parseJson(jsonFilePath, password);
	}
	
	public FtpCreds(String jsonFilePath) {
		parseJson(jsonFilePath);
	}
	
	public void parseJson(JSONObject jsonObj) {
		this.ftpHost = (String) jsonObj.get("ftpHost");
		this.userName = (String) jsonObj.get("userName");
		this.password = (String) jsonObj.get("password");
		this.port = (int) jsonObj.get("port");
		
	}
	
	public String getFtpHost() {
		return ftpHost;
	}

	public void setFtpHost(String ftpHost) {
		this.ftpHost = ftpHost;
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

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getJsonFilePath() {
		return jsonFilePath;
	}

	public void setJsonFilePath(String jsonFilePath) {
		this.jsonFilePath = jsonFilePath;
	}

	private String ftpHost;
	private String userName;
	private String password;
	private int port;
	
	private String jsonFilePath;
}