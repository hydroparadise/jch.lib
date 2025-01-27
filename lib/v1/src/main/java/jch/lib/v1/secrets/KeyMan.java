package jch.lib.secrets;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.util.ArrayList;
import java.util.Scanner;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import net.snowflake.client.jdbc.internal.org.bouncycastle.crypto.general.DES;
import jch.lib.file.FileUtil;


/***
 * 
 * https://medium.com/batc/combining-rsa-aes-encryption-to-secure-rest-endpoint-with-sensitive-data-eb3235b0c0cc
 * @author ChadHarrison
 *
 */
public class KeyMan {
	
	public static String CONFIG_KEY_PATH = "KeyPath";
	public static String CONFIG_ENCRYPTION_PASSWORD = "EncryptionPassword";
	public static String CONFIG_PUB_NAME = "PubRsaKeyFileName";
	public static String CONFIG_PRIV_NAME = "PrivRsaKeyFileName";	
	
	public static String PROJECT_PATH =  "./";
	public static String CONFIG_PATH = "config/";
	public static String KEYS_PATH = "keys/";
	public static String ENCRYPTED_PATH = "_encrypted/";
	public static String CONFIG_FILE_NAME = "KeyMan.json";
	public static String PUB_RSA_NAME = "pub.pem";
	public static String PRIV_RSA_NAME = "priv.key";
	public static String ENC_EXT = ".bin";
	
	public static boolean usingConfigFile = false;	
	public static String projKeyPath;	
	public static String keysPath;

	public static String pubRsaKeyFileName;
	public static String privRsaKeyFileName;	
	public static PublicKey pubRsaKey;
	public static PrivateKey privRsaKey;	
	
	public static ArrayList<String> subPaths = new ArrayList<String>();
	//public static ArrayList<String> listAllKeyPaths = new ArrayList<String>();
	
	/***
	 * Needed for any methods requiring Keys access
	 */
	static {
		projKeyPath = PROJECT_PATH + KEYS_PATH;
		
		//pickup of config file and get keysPath
		parseConfigJson(PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME);
		
		//verify config is being used
		QLog.log("Using "+ PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME + " config file: " + usingConfigFile);
		if(usingConfigFile) {
			QLog.log("keysPath: " + keysPath);
			QLog.log("pubRsaKeyFileName: " + pubRsaKeyFileName);
			QLog.log("privRsaKeyFileName: " + privRsaKeyFileName);
		}
		
		
		subPaths.add("aws/");
		subPaths.add("gcp/");
		subPaths.add("ftp/");
		subPaths.add("sql/");
		subPaths.add("pgp/");
		subPaths.add("email/");
		
		loadRsaKeys(keysPath);
	}
	
	public static void main(String[] args) {
		QLog.filePath = PROJECT_PATH;
		QLog.baseFileName = "pw_log.txt";
		QLog.charLimit = 2000;
		
		//QLog.log("Hello KeyMan");
		
		//processArgs(args);
		//initEncryptionPassword(PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME, true);
		runKeyMan();
	}
	
	public static void runKeyMan() {
		Scanner input = new Scanner(System.in);
		Console console = System.console();
		
		String selection = "";
		
		while(!selection.contentEquals("1") && !selection.contentEquals("2") ) {
			QLog.log("Please select from the available options:");
			QLog.log("1) Init RSA Certificates and deploy encrypted keys from local repo to local KeysPath");
			QLog.log("2) Check in and encrypt all existing keys in Keys and check in to local repo");
			
		    if (console == null)  {
		        System.out.print("Selection: ");
		        selection = input.nextLine();
		    } else {
		    	selection = new String(console.readPassword("Selection: "));
		    }
		}
		
		if(selection.contentEquals("1")) initEncryptionPassword(PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME, true);
		if(selection.contentEquals("2")) checkInKeys(keysPath, PROJECT_PATH + KEYS_PATH);
		
		input.close();
	}
	
	/***
	 * 
	 * @param configPath
	 * @param forceCreateConfig
	 */
	public static void initEncryptionPassword(String configPath, boolean forceCreateConfig) {
		QLog.log("Initializing encryption keys and password...");
		String defPath = PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME;
		
		if(configPath == null || configPath.equalsIgnoreCase("")) {
			configPath = defPath;
		}
		
		Scanner input = new Scanner(System.in);
		Console console = System.console();
	    String password = "";
	    String newKeyPath = "";
		
		//load
	    QLog.log("Looking for config file " + configPath);
		parseConfigJson(configPath);
		
		if(!usingConfigFile) {
			QLog.log("Could not load config file at " + configPath);
			if(forceCreateConfig) {
				QLog.log("Regenerating config file...");
				QLog.log("Please provide a desitination key path value (ie, C:\\Users\\ChadHarrison\\Keys\\) : ");
				newKeyPath = input.nextLine();
				
				QLog.log("Attempting to create " + configPath);
				initConfigFile(configPath, newKeyPath);
				parseConfigJson(configPath);
				
				if(!usingConfigFile) {
					QLog.log("Creation Failed...");
					if(!newKeyPath.equalsIgnoreCase(defPath)) {
						QLog.log("Falling back to default path: " + defPath);
						initConfigFile(defPath, newKeyPath);
						parseConfigJson(defPath);
						configPath = defPath;
					}
				}
			}
		}
		
	
		if(usingConfigFile) {
			do {
			    if (console == null)  {
			        System.out.print("Enter password: ");
			        password = input.nextLine();
			    } else {
			        password = new String(console.readPassword("Enter password: "));
			    }
				
			    if(password != null && !password.equalsIgnoreCase("")) {
			    	//create key paths if not exist
			    	initKeysPaths(keysPath);
			    	
			    	//create rsa keys if not exsit
			    	initRsaEncryptionKeys(keysPath);
			    	
			    	//load rsa keys from file
			    	loadRsaKeys(keysPath);
			    	
			    	//encrypt password vis RSA and store in config file
			    	setPassword(configPath, password);
			    	
			    	if(getPassword(configPath).equalsIgnoreCase(password)) {
			    		QLog.log("Password encryption successful!");
			    	}
			    	else QLog.log("Something went wrong with password encryption.");
			    }
			    else {
			    	QLog.log("Password must not be blank.");
			    }
			}
			while(password == null || password.equalsIgnoreCase(""));
			
			deployKeys(PROJECT_PATH + KEYS_PATH, keysPath);
		}
		else {
			QLog.log("Could not load a config file, abandoning encrytpion init.");
		}
		
	}
	
	/***
	 * 
	 * @param srcKeysPath
	 * @param destKeysPath
	 */
	public static void deployKeys(String srcKeysPath, String destKeysPath) {
		QLog.log("Deploying keys from " + srcKeysPath + " to " + destKeysPath);
		initKeysPaths(destKeysPath);
		copyEncryptedKeys(srcKeysPath, destKeysPath);
		decryptAllKeys(destKeysPath, getPassword());
	}
	
	/***
	 * 
	 */
	public static void initKeysPaths() {
		initKeysPaths(keysPath);
	}
	
	/***
	 * 
	 * @param keysPath
	 */
	public static void loadRsaKeys(String keysPath) {
		loadRsaEncryptionKeys(keysPath + pubRsaKeyFileName, keysPath + privRsaKeyFileName);
	}
	
	/***
	 * 
	 * @param password
	 */
	public static void setPassword(String password) {
		setPassword(PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME, password);
	}
			
	/***
	 * 
	 */
	public static void decryptAllKeys() {
		initKeysPaths(keysPath);
		decryptAllKeys(keysPath, getPassword(PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME));
	}
	
	/***
	 * 
	 */
	public static void encryptAllKeys() {
		initKeysPaths(keysPath);
		encryptAllKeys(keysPath, getPassword(PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME));
	}
	
	/***
	 * 
	 * @return
	 */
	public static String getPassword() {
		return getPassword(PROJECT_PATH + CONFIG_PATH + CONFIG_FILE_NAME);
	}
	
	/***
	 * 
	 * @param srcKeysPath
	 * @param destKeysPath
	 */
	public static void checkInKeys(String srcKeysPath, String destKeysPath) {
		encryptAllKeys(srcKeysPath, getPassword());
		copyEncryptedKeys(srcKeysPath, destKeysPath);
	}
		

	
	/***
	 * CONFIG_KEY_PATH = "KeyPath";
	 * CONFIG_ENCRYPTION_PASSWORD = "EncryptionPassword";
	 * CONFIG_PUB_NAME = "PubRsaKeyFileName";
	 * CONFIG_PRIV_NAME = "PrivRsaKeyFileName";
	 * 
	 * @param destPath
	 * @param setKeysPath
	 */
	public static void initConfigFile(String destPath, String setKeysPath){
		JSONObject jsonObj = new JSONObject();
		jsonObj.put(CONFIG_KEY_PATH, setKeysPath);
		jsonObj.put(CONFIG_PUB_NAME, PUB_RSA_NAME);
		jsonObj.put(CONFIG_PRIV_NAME, PRIV_RSA_NAME);
		jsonObj.put(CONFIG_ENCRYPTION_PASSWORD, "");
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String outJson = gson.toJson(jsonObj);
		
		FileWriter file;
		try {
			file = new FileWriter(destPath);
			file.write(outJson);
			file.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	/***
	 * 
	 * @param keysPath
	 * @param password
	 */
	public static void decryptAllKeys(String keysPath, String password) {
		for(String subPath : subPaths) {
			String checkPath = keysPath + ENCRYPTED_PATH + subPath;
			for(String fileName : FileUtil.listFilesLocal(checkPath)) {
				
				try {
					Files.createDirectories(Paths.get(checkPath));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					QLog.log(e.getMessage(), true);
				}
				
				String fileContent;
				
				try {
					fileContent = Files.readString(Path.of(checkPath+fileName));
					//QLog.log(fileContent);
					
					//String encrypted = KeyCryptAes.encrypt(fileContent, password);
					String decrypted = KeyCryptAes.decrypt(fileContent, password);
					
					String newPath = keysPath + subPath + fileName;
					newPath = newPath.replace(ENC_EXT, "");
					
					Files.writeString(Paths.get(newPath), decrypted);
					QLog.log(newPath + " created...");
					
				} catch (IOException e) {
					e.printStackTrace();
					QLog.log(e.toString(),true);
					QLog.log(e,true);
				}
			}
		}
	}
	
	/***
	 * 
	 * @param keysPath
	 * @param password
	 */
	public static void encryptAllKeys(String keysPath, String password) {
		
		for(String subPath : subPaths) {
			String checkPath = keysPath + subPath;
			
			try {
				Files.createDirectories(Paths.get(checkPath));
				QLog.log(checkPath + " created...");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.getMessage(), true);
			}
			
			for(String fileName : FileUtil.listFilesLocal(checkPath)) {
				String fileContent;
				
				try {
					fileContent = Files.readString(Path.of(checkPath+fileName));
					//QLog.log(fileContent);
					
					String encrypted = KeyCryptAes.encrypt(fileContent, password);
					//String decrypted = KeyCryptAes.decrypt(encrypted, key);
					
					String newPath = keysPath + ENCRYPTED_PATH + subPath + fileName + ENC_EXT;
					
					try {
						Files.createDirectories(Paths.get(keysPath + ENCRYPTED_PATH + subPath));
						QLog.log(keysPath + ENCRYPTED_PATH + subPath + " created...");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						QLog.log(e.getMessage(), true);
					}
					
					Files.writeString(Paths.get(newPath), encrypted);
					QLog.log(newPath + " created...");
			
					
				} catch (IOException e) {
					e.printStackTrace();
					QLog.log(e.toString());
				}
			}
		}
	}
	
	/***
	 * 
	 * @param configFilePath
	 * @return
	 */
	public static String getPassword(String configFilePath) {
		String output = null;
		JSONObject jsonObj = null;
		String jsonString;
		try {
			jsonString = Files.readString(Path.of(configFilePath));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
			String encr1 = (String) jsonObj.get(CONFIG_ENCRYPTION_PASSWORD);
			
			byte[] decr1 = KeyCryptRsa.base64MimeDecode(encr1);
			String decr2 = KeyCryptRsa.decrypt(decr1, privRsaKey);
			
			output = decr2;
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.toString());
		}
		return output;
	}
	
	/***
	 * 
	 * @param configFilePath
	 * @param password
	 */
	@SuppressWarnings("unchecked")
	public static void setPassword(String configFilePath, String password) {
		String output = null;
		JSONObject jsonObj = null;
		String jsonString;
		try {
			jsonString = Files.readString(Path.of(configFilePath));

			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);	
			
			
			String encr2 = "";
			try {
				byte[] encr1 = KeyCryptRsa.encrypt(password, pubRsaKey);
				encr2 = KeyCryptRsa.base64MimeEndcode(encr1);	//store in a safe format
			} catch (Exception e) {
				e.printStackTrace();
				QLog.log(e.toString());
			}
			
			jsonObj.put("EncryptionPassword", encr2);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String outJson = gson.toJson(jsonObj);
			
			FileWriter file = new FileWriter(configFilePath);
			file.write(outJson);
			file.close();
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString());
		}
	}
	
	/***
	 * 
	 * @param publicKeyFilePath
	 * @param privateKeyFilePath
	 * @return
	 */
	public static boolean loadRsaEncryptionKeys(String publicKeyFilePath, String privateKeyFilePath) {
		pubRsaKey = KeyCryptRsa.loadPublicKeyFile(publicKeyFilePath);
		privRsaKey = KeyCryptRsa.loadPrivateKeyFile(privateKeyFilePath);
		boolean output = true;
		if(pubRsaKey == null || privRsaKey == null) {
			output = false;
			QLog.log("WARNING: Could not load one or more RSA keys.");
		}
		return output;
	}
	
	/***
	 * 
	 * @param srcKeysPath
	 * @param destKeysPath
	 */
	public static void copyEncryptedKeys(String srcKeysPath, String destKeysPath) {
		//QLog.log("srcKeysPath: " + srcKeysPath);
		//QLog.log("destKeysPath: " + destKeysPath);
		
		for(String subPath : subPaths) {
			String checkPath = srcKeysPath + ENCRYPTED_PATH + subPath;
			
			String srcFile = "";
			String destFile = "";
			
			QLog.log(checkPath);
			
			ArrayList<String> fileNames = FileUtil.listFilesLocal(checkPath);
			
			if(fileNames != null && fileNames.size() > 0)
			for(String fileName : fileNames) {
				
				srcFile = srcKeysPath + ENCRYPTED_PATH + subPath + fileName;
				destFile = destKeysPath + ENCRYPTED_PATH + subPath + fileName;
				QLog.log(srcFile + " -> " + destFile);
				
				File source = new File(srcFile);
				File dest = new File(destFile);

				try {
				    FileUtils.copyFile(source, dest);
				} catch (IOException e) {
				    e.printStackTrace();
				}
			}
		}
	}
	
	/***
	 * 
	 * @param keysPath
	 */
	public static void initKeysPaths(String keysPath) {
		try {

			if(!Files.exists(Paths.get(keysPath))) {
				Files.createDirectories(Paths.get(keysPath));
				QLog.log(keysPath +  " created...");
			}
			
			if(!Files.exists(Paths.get(keysPath + ENCRYPTED_PATH))) {
				Files.createDirectories(Paths.get(keysPath + ENCRYPTED_PATH));
				QLog.log(keysPath + ENCRYPTED_PATH +  " created...");
			}
			
			for(String subPath : subPaths) {
				if(!Files.exists(Paths.get(keysPath + subPath))) {
					Files.createDirectories(Paths.get(keysPath + subPath));
					QLog.log(keysPath + subPath +  " created...");
				}
				
				if(!Files.exists(Paths.get(keysPath + ENCRYPTED_PATH + subPath))) {
					Files.createDirectories(Paths.get(keysPath + ENCRYPTED_PATH + subPath));
					QLog.log(keysPath + ENCRYPTED_PATH + subPath +  " created...");
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
	}
	
	/***
	 * 
	 * @param keysPath
	 */
	public static void initRsaEncryptionKeys(String keysPath) {
		//QLog.log(keysPath);
		
		KeyPair kp = null; 
		try {
			kp = KeyCryptRsa.generateRSAKKeyPair();
		} catch (Exception e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
		
		if(!Files.exists(Paths.get(keysPath + pubRsaKeyFileName))) {
			KeyCryptRsa.savePublicKeyFile(kp.getPublic(), keysPath + pubRsaKeyFileName);
			QLog.log(keysPath + pubRsaKeyFileName + " created...");
		}
		
		if(!Files.exists(Paths.get(keysPath + privRsaKeyFileName))) {
			KeyCryptRsa.savePrivateKeyFile(kp.getPrivate(), keysPath + privRsaKeyFileName);
			QLog.log(keysPath + privRsaKeyFileName + " created...");
		}
	}
	
	/***
	 * CONFIG_KEY_PATH = "KeyPath";
	 * CONFIG_ENCRYPTION_PASSWORD = "EncryptionPassword";
	 * CONFIG_PUB_NAME = "PubRsaKeyFileName";
	 * CONFIG_PRIV_NAME = "PrivRsaKeyFileName";
	 * @param configFilePath
	 */
	public static void parseConfigJson(String configFilePath) {
		JSONObject jsonObj = null;
		String jsonString;
		usingConfigFile = false;
		try {
			jsonString = Files.readString(Path.of(configFilePath));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
			keysPath = (String) jsonObj.get(CONFIG_KEY_PATH);
			pubRsaKeyFileName = (String) jsonObj.get(CONFIG_PUB_NAME);
			privRsaKeyFileName = (String) jsonObj.get(CONFIG_PRIV_NAME);
			
			usingConfigFile = true;
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString());
			QLog.log("Could not parse JSON file " + configFilePath);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString());
			QLog.log("Could not open file " + configFilePath);
		}

	}
	
	/***
	 * 
	 * @param basePath
	 * @return
	 */
	public static ArrayList<String> getAllKeyFiles(String basePath) {
		ArrayList<String> output = new ArrayList<String>();
		
		for(String subPath : subPaths) {
			String checkPath = basePath + subPath;
						
			for(String fileName : FileUtil.listFilesLocal(checkPath)) {
				output.add(checkPath + fileName);
			}

		}
		
		return output;
	}
	
	/***
	 * 
	 * @param credsPaths
	 * @return
	 */
	public static ArrayList<AwsCreds> listAwsCreds(ArrayList<String> credsPaths) {
		ArrayList<AwsCreds> output = new ArrayList<AwsCreds>();
		for(String file : credsPaths) {
			
			if(file.contains("aws/")) {
				output.add(new AwsCreds(file));
			}
		}
		
		return output;
	}
	
	/***
	 * 
	 * @param bucketName
	 * @return
	 */
	public static AwsCreds getAwsCreds(String bucketName) {
		AwsCreds output = null;
		for (AwsCreds creds : listAwsCreds(getAllKeyFiles(keysPath))) {
			if(creds.getBucketName().equalsIgnoreCase(bucketName))
				output = creds;
		}
		
		return output;
	}

	/***
	 * 
	 * @param credsPaths
	 * @return
	 */
	public static ArrayList<GcpCreds> listGcpCreds(ArrayList<String> credsPaths) {
		ArrayList<GcpCreds> output = new ArrayList<GcpCreds>();
		for(String file : credsPaths) {
			if(file.contains("gcp/")) {
				output.add(new GcpCreds(file));
			}
		}
		
		return output;
	}
	
	/***
	 * 
	 * @param projectId
	 * @return
	 */
	public static GcpCreds getGcpCreds(String projectId) {
		GcpCreds output = null;
		for (GcpCreds creds : listGcpCreds(getAllKeyFiles(keysPath))) {
			if(creds.getProject_id().equalsIgnoreCase(projectId))
				output = creds;
		}
		
		return output;
	}
	
	/***
	 * 
	 * @param credsPaths
	 * @return
	 */
	public static ArrayList<FtpCreds> listFtpCreds(ArrayList<String> credsPaths) {
		ArrayList<FtpCreds> output = new ArrayList<FtpCreds>();
		for(String file : credsPaths) {
			if(file.contains("ftp/")) {
				output.add(new FtpCreds(file));
			}
		}
		
		return output;
	}
	
	/***
	 * 
	 * @param ftpHost
	 * @return
	 */
	public static FtpCreds getFtpCreds(String ftpHost) {
		FtpCreds output = null;
		for (FtpCreds creds : listFtpCreds(getAllKeyFiles(keysPath))) {
			if(creds.getFtpHost().equalsIgnoreCase(ftpHost))
				output = creds;
		}
		
		return output;
	}
	
	/***
	 * 
	 * @param credsPaths
	 * @return
	 */
	public static ArrayList<EmailCreds> listEmailCreds(ArrayList<String> credsPaths) {
		ArrayList<EmailCreds> output = new ArrayList<EmailCreds>();
		for(String file : credsPaths) {
			if(file.contains("email/")) {
				output.add(new EmailCreds(file));
			}
		}
		
		return output;
	}
	
	/***
	 * 
	 * @param mailServer
	 * @return
	 */
	public static EmailCreds getEmailCreds(String mailServer) {
		EmailCreds output = null;
		for (EmailCreds creds : listEmailCreds(getAllKeyFiles(keysPath))) {
			if(creds.getMailServer().equalsIgnoreCase(mailServer))
				output = creds;
		}
		
		return output;
	}
	
}