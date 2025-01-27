package jch.lib.secrets;

import org.json.simple.JSONObject;

import jch.lib.log.QLog;

public class AwsCreds extends Creds {
	public AwsCreds() {
		
	}
	
	public AwsCreds(String jsonFilePath) {
		parseJson(jsonFilePath);
	}
	
	public AwsCreds(String jsonFilePath, String password) {
		parseJson(jsonFilePath, password);
	}
	
	public void parseJson(JSONObject jsonObj) {
		this.bucketName = (String) jsonObj.get("bucketName");
		this.accessKey = (String) jsonObj.get("accessKey");
		this.secretKey = (String) jsonObj.get("secretKey");
		this.region = (String) jsonObj.get("region");
	}
	
	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getJsonFilePath() {
		return jsonFilePath;
	}

	public void setJsonFilePath(String jsonFilePath) {
		this.jsonFilePath = jsonFilePath;
	}

	private String bucketName;
	private String accessKey;
	private String secretKey;
	private String region;
	//private String jsonFilePath;
	
}