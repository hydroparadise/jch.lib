package jch.lib.secrets;

import java.util.*;
import java.io.IOException;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import javax.crypto.*;

import jch.lib.log.QLog;


/***
 * 
 * https://www.ssl.com/guide/pem-der-crt-and-cer-x-509-encodings-and-conversions/#:~:text=PEM%20(originally%20%E2%80%9CPrivacy%20Enhanced%20Mail,%2D%2D%2D%2D%2D%20).
 * https://www.baeldung.com/java-base64-encode-and-decode
 * https://stackoverflow.com/questions/9755057/converting-strings-to-encryption-keys-and-vice-versa-java
 * 
 * @author ChadHarrison
 *
 */
public class KeyCryptRsa {
	
	/***
	 * 
	 * @return
	 * @throws Exception
	 */
	public static KeyPair generateRSAKKeyPair() throws Exception {
	    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
	    keyPairGenerator.initialize(3072);
	    //keyPairGenerator.initialize(16384);
	    return keyPairGenerator.generateKeyPair();
	}
	
	/*
	public static String encryptString(String plainText, PublicKey publicKey) throws Exception {
		byte[] ct = encrypt(plainText, publicKey);
		return new String(ct);
		
	}
	
	public static String decrypt(String cipherText, PrivateKey privateKey) throws Exception {
		byte[] ct = cipherText.getBytes(StandardCharsets.UTF_8);
		return decrypt(ct, privateKey);
	}
	*/
	
	/***
	 * 
	 * @param plainText
	 * @param publicKey
	 * @return
	 * @throws Exception
	 */
	public static byte[] encrypt(String plainText, PublicKey publicKey) throws Exception {
	    Cipher cipher = Cipher.getInstance("RSA");
	    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
	    return cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
	}
	
	

	/***
	 * 
	 * @param cipherText
	 * @param privateKey
	 * @return
	 * @throws Exception
	 */
	public static String decrypt(byte[] cipherText, PrivateKey privateKey) throws Exception {
	    Cipher cipher = Cipher.getInstance("RSA");
	    cipher.init(Cipher.DECRYPT_MODE, privateKey);
	    byte[] result = cipher.doFinal(cipherText);
	    return new String(result);
	}
	
	
	
	/***
	 * 
	 * @param filePath
	 * @return
	 */
	public static PublicKey loadPublicKeyFile(String filePath) {
		PublicKey output = null;
		String key = null;
		try {
			key = Files.readString(Path.of(filePath));
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
			QLog.log("Could not load public key at " + filePath);
		}
		
		if(key != null && !key.equalsIgnoreCase("")) {
			
			key = removePemHeaderFooter(key);
			try {
				output = loadPublicKey(key);
			} catch (GeneralSecurityException e) {
				QLog.log(e.getMessage());
				e.printStackTrace();
			}
		}
	
		return output;
	}
	
	/***
	 * 
	 * @param filePath
	 * @return
	 */
	public static PrivateKey loadPrivateKeyFile(String filePath) {
		PrivateKey output = null;
		String key = null;
		try {
			key = Files.readString(Path.of(filePath));
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
		if(key != null && !key.equalsIgnoreCase("")) {
			key = removePemHeaderFooter(key);
			
			try {
				output = loadPrivateKey(key);
			} catch (GeneralSecurityException e) {
				QLog.log(e.getMessage());
				e.printStackTrace();
			}
		}
		return output;
	}
	
	/***
	 * 
	 * @param publ
	 * @param filePath
	 */
	public static void savePublicKeyFile(PublicKey publ, String filePath) {
		String key = null;
		try {
			key = savePublicKey(publ);
		} catch (GeneralSecurityException e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
		
		key = addPemHeaderFooter(key);
		try {
			Files.writeString(Path.of(filePath), key);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
	}
	
	/***
	 * 
	 * @param priv
	 * @param filePath
	 */
	public static void savePrivateKeyFile(PrivateKey priv, String filePath) {
		String key = null;
		try {
			key = savePrivateKey(priv);
		} catch (GeneralSecurityException e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
		
		key = addPemHeaderFooter(key);
		try {
			Files.writeString(Path.of(filePath), key);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
	}
	
	/***
	 * 
	 * @param key
	 * @return
	 */
	private static String removePemHeaderFooter(String key) {
		String output = key;
		output = output.replace("-----BEGIN CERTIFICATE-----\r\n", "");
		output = output.replace("\r\n-----END CERTIFICATE-----", "");
		
		return output;
	}
	
	/***
	 * 
	 * @param key
	 * @return
	 */
	private static String addPemHeaderFooter(String key) {
		String output = key;
		output = "-----BEGIN CERTIFICATE-----\r\n" + output;
		output = output + "\r\n-----END CERTIFICATE-----";
		
		return output;
	}
	
	/***
	 * 
	 * @param key64
	 * @return
	 * @throws GeneralSecurityException
	 */
	public static PrivateKey loadPrivateKey(String key64) throws GeneralSecurityException {
	    byte[] clear = base64MimeDecode(key64);
	    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(clear);
	    KeyFactory fact = KeyFactory.getInstance("RSA");
	    PrivateKey priv = fact.generatePrivate(keySpec);
	    Arrays.fill(clear, (byte) 0);
	    return priv;
	}
	
	/***
	 * 
	 * @param stored
	 * @return
	 * @throws GeneralSecurityException
	 */
	public static PublicKey loadPublicKey(String stored) throws GeneralSecurityException {
	    byte[] data = base64MimeDecode(stored);
	    X509EncodedKeySpec spec = new X509EncodedKeySpec(data);
	    KeyFactory fact = KeyFactory.getInstance("RSA");
	    return fact.generatePublic(spec);
	}

	/***
	 * 
	 * @param priv
	 * @return
	 * @throws GeneralSecurityException
	 */
	public static String savePrivateKey(PrivateKey priv) throws GeneralSecurityException {
	    KeyFactory fact = KeyFactory.getInstance("RSA");
	    PKCS8EncodedKeySpec spec = fact.getKeySpec(priv,
	            PKCS8EncodedKeySpec.class);
	    byte[] packed = spec.getEncoded();
	    String key64 = base64MimeEndcode(packed);

	    Arrays.fill(packed, (byte) 0);
	    return key64;
	}

	/***
	 * 
	 * @param publ
	 * @return
	 * @throws GeneralSecurityException
	 */
	public static String savePublicKey(PublicKey publ) throws GeneralSecurityException {
	    KeyFactory fact = KeyFactory.getInstance("RSA");
	    X509EncodedKeySpec spec = fact.getKeySpec(publ, X509EncodedKeySpec.class);
	    return base64MimeEndcode(spec.getEncoded());
	}

	/*
	public static String base64MimeDecodeString(String endcoded) {
		
		return new String(base64MimeDecode(endcoded));
	}
	
	public static String base64MimeEndcode(String decoded) {
		return base64MimeEndcode(decoded.getBytes(StandardCharsets.UTF_8));
	}	
	*/	
	
	/***
	 * 
	 * @param endcoded
	 * @return
	 */
	public static byte[] base64MimeDecode(String endcoded) {
		
		return Base64.getMimeDecoder().decode(endcoded);
	}
	

	/***
	 * 
	 * @param decoded
	 * @return
	 */
	public static String base64MimeEndcode(byte[] decoded) {
		return Base64.getMimeEncoder().encodeToString(decoded);
	}


}