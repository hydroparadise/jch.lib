package jch.lib.cloud.azure;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import jch.lib.log.QLog;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.api.client.util.IOUtils;

/***
 * 
 * 
 * 
 * @author harrisonc
 *
 */
public class AzureBlob {
	/*
	 * Sample JSON file
	 *	{
	 *		"HTTPSStorageHost":"https://example.blob.core.windows.net", 
	 *		"Container":"azureblobexample", 
	 *		"ContainerSAS":"a;slkdjfa;soidfs"
	 *	}
	 */
	
	

	/***
	 * Deletes a specific blob file
	 * 
	 * @param String azBlobCredsLoc:
	 * @param String blobName:
	 * @param String blobPath:
	 */
	public static void deleteBlob(String azBlobCredsLoc, String blobName, String blobPath) {
	    JSONObject jsonObj = null;
	    
	    //retrieve Blob container details from JSON config file
		try {
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	    
		String azBlobCnString = "";
		
		//grab values from JSON file and must be casted to String
		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		azBlobCnString = host + "/" + container;
		
		QLog.log("blob dir: " + blobPath);
		
		//assemble path to blob container URL if one was specified
		if(blobPath != null) 
			azBlobCnString = azBlobCnString + "/" + blobPath ;
		
		//TODO: what to do if blob name wasn't specified?
		if(blobName != null)
			azBlobCnString = azBlobCnString	+ "/" + blobName;
		
		//assemble URL with SAS token
		azBlobCnString = azBlobCnString + "?" + sas;

		QLog.log(azBlobCnString);
				
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				
			httpCon.setDoOutput(true);
			httpCon.setRequestMethod("DELETE");
			
			//get connection response
			String res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8) ;
			
			QLog.log("AzureBlob: Azure DELETE response: " + res);
			QLog.log(azBlobCnString);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
	}
	
	
	/***
	 * 
	 * @param azBlobCredsLoc
	 * @param blobName
	 */
	public static void deleteBlob(String azBlobCredsLoc, String blobName) {
		deleteBlob(azBlobCredsLoc, blobName, null);
	}
	
	
	/***
	 * 
	 *Sample XML 
		<EnumerationResults ServiceEndpoint="https://testdevblob.blob.core.windows.net/" ContainerName="testcontainer">
		<Blobs>
			<Blob>
				<Name>test.txt</Name>
				<Properties>
				<Creation-Time>Thu, 17 Feb 2022 22:18:03 GMT</Creation-Time>
				<Last-Modified>Wed, 23 Feb 2022 15:05:01 GMT</Last-Modified>
				<Etag>0x8D9F6DDDDB2D701</Etag>
				<Content-Length>166</Content-Length>
				<Content-Type>application/octet-stream</Content-Type>
				<Content-Encoding/>
				<Content-Language/>
				<Content-CRC64/>
				<Content-MD5>0srpkvJx2WnY0Mo7rKNNWQ==</Content-MD5>
				<Cache-Control/>
				<Content-Disposition/>
				<BlobType>BlockBlob</BlobType>
				<AccessTier>Cool</AccessTier>
				<AccessTierInferred>true</AccessTierInferred>
				<LeaseStatus>unlocked</LeaseStatus>
				<LeaseState>available</LeaseState>
				<ServerEncrypted>true</ServerEncrypted>
				</Properties>
				<OrMetadata/>
			</Blob>
			<Blob>
			</Blob>
		</Blobs>
		<NextMarker/>
		</EnumerationResults>
		@param String azBlobCredsLoc
		@param String azBlobFolderPath
		@return ArrayList<String>
	 */
	public static ArrayList<String> listContainerBlobFiles(String azBlobCredsLoc, String azBlobFolderPath) {
		// TODO Auto-generated method stub
		ArrayList<String> output = null;
			
		String xmlResp = xmlListContainerBlobFiles(azBlobCredsLoc,azBlobFolderPath);
		
        try {		
        	
        	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        	DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(new InputSource(new StringReader(xmlResp)));
			
			doc.getDocumentElement().normalize();
			
			NodeList list = doc.getElementsByTagName("Blob");
			
			output = new ArrayList<String>();
			
			//start packing in directory items
			for(int i = 0; i < list.getLength(); i++) {
				
				Node node = list.item(i);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					output.add(element.getElementsByTagName("Name").item(0).getTextContent());
					
					//QLog.log(element.getElementsByTagName("Name").item(0).getTextContent());
				}
			}
			
		} catch ( ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
		return output;
	}
	
	
	/***
	 * 
	 * @param String azBlobCredsLoc:
	 * @param String azBlobFolderPath:
	 * @return String
	 */
	public static String xmlListContainerBlobFiles(String azBlobCredsLoc, String azBlobFolderPath) {
		String output = null;
		
		//retrieve Blob container details from JSON config file
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	    
		String azBlobCnString = "";

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		azBlobCnString = host + "/" + container;
		
		if(azBlobFolderPath != null)
			azBlobCnString = azBlobCnString + "/" + azBlobFolderPath; 
		
		azBlobCnString = azBlobCnString + "?" + sas;
		azBlobCnString = azBlobCnString + "&restype=container";
		azBlobCnString = azBlobCnString + "&comp=list";
		
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();

			httpCon.setRequestMethod("GET");
			
			String res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8) ;
			output = res;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
		//if first character value is 65279, trim it off
		if((int) output.charAt(0)  == 65279) {
			output = output.substring(1);
			
			QLog.log("xmlListContainerBlobFiles: Trimmed the question (65279)");
			
		}
		
		return output;
	}
	
	
	/***
	 * 
	 * 
	 * @param String azBlobCredsLoc:
	 * @param String sourceBlobName:
	 * @param String sourceBlobPath:
	 * @param String destBlobPath:
	 */
	public static void copyBlobFile(String azBlobCredsLoc, String sourceBlobName, String sourceBlobPath,
			String destBlobPath) {
		
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	    

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		String azBlobCnString = "";		
		
		azBlobCnString = host + "/" + container;	
		if(destBlobPath != null) 
			azBlobCnString = azBlobCnString + "/" + destBlobPath ;
		azBlobCnString = azBlobCnString	+ "/" + sourceBlobName + "?" + sas;

		QLog.log(azBlobCnString);
		
		String sourceNameUrl = null;
		sourceNameUrl = host + "/" + container;
		if(sourceBlobPath != null) 
			sourceNameUrl = sourceNameUrl + "/" + sourceBlobPath ;
		sourceNameUrl = sourceNameUrl + "/" + sourceBlobName + "?" + sas;;
		
		//System.out.println(sourceNameUrl);
		QLog.log(sourceNameUrl);
		
		String res = null;
		
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				
			httpCon.setDoOutput(true);
			httpCon.setRequestProperty("x-ms-copy-source", sourceNameUrl);
			httpCon.setFixedLengthStreamingMode(0); //response code 411 if not set
			httpCon.setRequestMethod("PUT");
			
			res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			
			//Error response if needed
			//String err = new String(httpCon.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
		QLog.log("Azure COPY response: " + res);
	}
	
	
	/***
	 * 
	 * 
	 * @param String azBlobCredsLoc:
	 * @param String sourcePath:
	 * @param String sourceName:
	 */
	public static void putBlobFile(String azBlobCredsLoc, String sourcePath, String sourceName) {
		putBlobFile(azBlobCredsLoc,sourcePath,sourceName, null);
		
	}
	
	
	/***
	 * Sends file to Azure Blob container via a PUT request
	 * 
	 * @param String azBlobCredsLoc:
	 * @param String sourcePath:
	 * @param String sourceName:
	 * @param String blobDir:
	 */
	public static void putBlobFile(String azBlobCredsLoc, String sourcePath, String sourceName, String blobDir) {
	    JSONObject jsonObj = null;
	    
		try {
			
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	
		String azBlobCnString = "";

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		azBlobCnString = host + "/" + container;
		
		QLog.log("blob dir: " + blobDir);
		
		if(blobDir != null) 
			azBlobCnString = azBlobCnString + "/" + blobDir ;
		
		azBlobCnString = azBlobCnString	+ "/" + sourceName + "?" + sas;
		
		QLog.log("AzureTest: " + sourcePath + sourceName);
		QLog.log("AzureTest: " + azBlobCnString);
		
		
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				
			httpCon.setDoOutput(true);
			httpCon.setRequestProperty("x-ms-blob-type", "BlockBlob");
			httpCon.setRequestMethod("PUT");
			
			QLog.log("AzureBlob: Put request for " + sourcePath + sourceName);
			IOUtils.copy(new FileInputStream(sourcePath + sourceName),httpCon.getOutputStream());
			
			QLog.log("AzureBlob: Getting Response....");
			String res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8) ;
			
			//System.out.println(res);
			QLog.log("AzureBlob: Azure COPY response: " + res);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
		
	}
	
	
	/***
	 * 
	 * @param azBlobCredsLoc
	 * @param sourceName
	 * @return
	 */
	public static String printURL(String azBlobCredsLoc, String sourceName) {
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ETL Exception: " + e.toString());
			QLog.log(e);
		}
	    
		String azBlobCnString = "";

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		azBlobCnString = host + "/" + container + "/" + sourceName + "?" + sas;
		return azBlobCnString;
	}
	
}
