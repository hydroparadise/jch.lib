package jch.lib.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.api.client.util.IOUtils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/***
 * Azure uses XML
 * 
 * 
 * @author harrisonc
 *
 */
public class JchLib_AzureTest {

	/*
	 * Sampple JSON file
		{
			"HTTPSStorageHost":"https://example.blob.core.windows.net", 
			"Container":"azureblobexample", 
			"ContainerSAS":"a;slkdjfa;soidfs"
		}
	 */
	
	
	public static void deleteBlob(String azBlobCredsLoc, String blobName, String blobPath) {
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	
		String azBlobCnString = "";

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		azBlobCnString = host + "/" + container;
		
		System.out.println("blob dir: " + blobPath);
		
		if(blobPath != null) 
			azBlobCnString = azBlobCnString + "/" + blobPath ;
		
		if(blobName != null)
			azBlobCnString = azBlobCnString	+ "/" + blobName;
		
		azBlobCnString = azBlobCnString + "?" + sas;
		
		//System.out.println(sourcePath + sourceName);
		System.out.println(azBlobCnString);
		
		
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				
			httpCon.setDoOutput(true);
			//httpCon.setRequestProperty("x-ms-blob-type", "BlockBlob");
			httpCon.setRequestMethod("DELETE");
			
			
			String res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8) ;
			
			System.out.println(res);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

	
	
	/*
	 * 
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
			
			for(int i = 0; i < list.getLength(); i++) {
				
				Node node = list.item(i);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					System.out.println(element.getElementsByTagName("Name").item(0).getTextContent());
					output.add(element.getElementsByTagName("Name").item(0).getTextContent());
				}
				
			}
			
			
		} catch ( ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return output;
		
	}
	
	
	/***
	 * 
	 * @param azBlobCredsLoc
	 * @param azBlobFolderPath
	 * @return
	 */
	public static String xmlListContainerBlobFiles(String azBlobCredsLoc, String azBlobFolderPath) {
		String output = null;
		
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		
		//System.out.println(azBlobCnString);
		
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				
			//httpCon.setDoOutput(true);
			//httpCon.setRequestProperty("restype", "container");
			//httpCon.setRequestProperty("comp", "list");
			httpCon.setRequestMethod("GET");
			
			
			String res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8) ;
			//String body = httpCon.getResponseMessage();
			
			output = res;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*
		System.out.println("at 0: " + output.charAt(0)+ ", " + (int)output.charAt(0));
		System.out.println("at 1: " + output.charAt(1)+ ", " + (int)output.charAt(1));
		System.out.println("at 2: " + output.charAt(2)+ ", " + (int)output.charAt(2));
		System.out.println("at 3: " + output.charAt(3)+ ", " + (int)output.charAt(3));
		*/
		//at 0: ?, 65279
		if((int) output.charAt(0)  == 65279) {
			output = output.substring(1);
			System.out.println("Trimmed the quesion");
			
			/*
			System.out.println("at 0: " + output.charAt(0)+ ", " + (int)output.charAt(0));
			System.out.println("at 1: " + output.charAt(1)+ ", " + (int)output.charAt(1));
			System.out.println("at 2: " + output.charAt(2)+ ", " + (int)output.charAt(2));
			System.out.println("at 3: " + output.charAt(3)+ ", " + (int)output.charAt(3));
			*/
		}
		
		return output;
	}
	
	
	
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		String azBlobCnString = "";		
		azBlobCnString = host + "/" + container;	
		if(destBlobPath != null) 
			azBlobCnString = azBlobCnString + "/" + destBlobPath ;
		azBlobCnString = azBlobCnString	+ "/" + sourceBlobName + "?" + sas;
		//azBlobCnString = azBlobCnString	+ "/" + sourceBlobName;
		System.out.println(azBlobCnString);
		
		String sourceNameUrl = null;
		sourceNameUrl = host + "/" + container;
		if(sourceBlobPath != null) 
			sourceNameUrl = sourceNameUrl + "/" + sourceBlobPath ;
		sourceNameUrl = sourceNameUrl + "/" + sourceBlobName + "?" + sas;;
		System.out.println(sourceNameUrl);
		
		String res = null;
		
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				
			httpCon.setDoOutput(true);
			
			
			//httpCon.setRequestProperty("Autorization", sas);
			
			httpCon.setRequestProperty("x-ms-copy-source", sourceNameUrl);
			httpCon.setFixedLengthStreamingMode(0); //response code 411 if not set
			httpCon.setRequestMethod("PUT");
			
			res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			//String err = new String(httpCon.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
					
		System.out.println(res);
		System.out.println(res.length());
	}
	
	
	/***
	 * 
	 * @param azBlobCredsLoc
	 * @param sourcePath
	 * @param sourceName
	 */
	public static void putBlobFile(String azBlobCredsLoc, String sourcePath, String sourceName) {
		putBlobFile(azBlobCredsLoc,sourcePath,sourceName, null);
		
	}
	
	/***
	 * 
	 * @param azBlobCredsLoc
	 * @param sourcePath
	 * @param sourceName
	 * @param blobDir
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	
		String azBlobCnString = "";

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		azBlobCnString = host + "/" + container;
		
		System.out.println("blob dir: " + blobDir);
		
		if(blobDir != null) 
			azBlobCnString = azBlobCnString + "/" + blobDir ;
		
		azBlobCnString = azBlobCnString	+ "/" + sourceName + "?" + sas;
		
		//System.out.println(sourcePath + sourceName);
		System.out.println(azBlobCnString);
		
		
		URL url;
		try {
			url = new URL(azBlobCnString);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				
			httpCon.setDoOutput(true);
			httpCon.setRequestProperty("x-ms-blob-type", "BlockBlob");
			httpCon.setRequestMethod("PUT");
			
			IOUtils.copy(new FileInputStream(sourcePath + sourceName),httpCon.getOutputStream());
			String res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8) ;
			
			System.out.println(res);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

	
	
	public static void putTest(String azBlobCfsconnectorscuLoc, String sourcePath, String sourceName) {
		    JSONObject jsonObj = null;
			try {
				String jsonString = Files.readString(Path.of(azBlobCfsconnectorscuLoc));
				JSONParser jsonParser = new JSONParser();
				jsonObj =  (JSONObject) jsonParser.parse(jsonString);
				
				
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		
			String azBlobCnString = "";

			String host = (String) jsonObj.get("HTTPSStorageHost");
			String container = (String) jsonObj.get("Container");
			String sas = (String) jsonObj.get("ContainerSAS");
			
			azBlobCnString = host + "/" + container + "/" + sourceName + "?" + sas;
			
			System.out.println(sourcePath + sourceName);
			System.out.println(azBlobCnString);
			
			
			URL url;
			try {
				url = new URL(azBlobCnString);
				HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
					
				httpCon.setDoOutput(true);
				httpCon.setRequestProperty("x-ms-blob-type", "BlockBlob");
				httpCon.setRequestMethod("PUT");
				
				//OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream());
				//Path source = Paths.get(sourceName);
				//FileInputStream fis = new FileInputStream(source.toFile());
				
				IOUtils.copy(new FileInputStream(sourcePath + sourceName),httpCon.getOutputStream());
				String res = new String(httpCon.getInputStream().readAllBytes(), StandardCharsets.UTF_8) ;
				
				System.out.println(res);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			
			
			/*test 1
			out.write("Resource content");
			*/
			
			/*test 2
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                //out.write(buffer, 0, len);
            	out.write(buffer.toString(),0,len);
            	
            }
			*/
			
			/*
			IOUtils.copy(new FileInputStream(sourceName),httpCon.getOutputStream());
			String res = httpCon.getInputStream().toString();
			
			out.close();
			*/
			//httpCon.getInputStream();
	

	}
	
	
	public static String printURL(String azBlobCredsLoc, String sourceName) {
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(azBlobCredsLoc));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
			
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	
		String azBlobCnString = "";

		String host = (String) jsonObj.get("HTTPSStorageHost");
		String container = (String) jsonObj.get("Container");
		String sas = (String) jsonObj.get("ContainerSAS");
		
		return azBlobCnString = host + "/" + container + "/" + sourceName + "?" + sas;
	}
	
	
	


	
}