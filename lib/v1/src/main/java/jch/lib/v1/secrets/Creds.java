package jch.lib.secrets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jch.lib.log.QLog;

public abstract class Creds {
	

	public void parseJson(String filePath, String password) {
		JSONObject jsonObj = null;
		String encryptedString;
		String jsonString;
		
		try {
			
			encryptedString = Files.readString(Path.of(filePath));
			jsonString = KeyCryptAes.decrypt(encryptedString, password);
			
			
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
			parseJson(jsonObj);
			
			this.jsonFilePath = filePath;
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
		}
	}
	
	/***
	 * 
	 * @param filePath
	 */
	public void parseJson(String filePath) {
		JSONObject jsonObj = null;
		String jsonString;
		try {
			jsonString = Files.readString(Path.of(filePath));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
			parseJson(jsonObj);
			
			this.jsonFilePath = filePath;
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString(),true);
			QLog.log(e,true);
		}

	}
	
	public abstract void parseJson(JSONObject jsonObj);
	
	protected String jsonFilePath;
}