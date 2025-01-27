

package jch.lib.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.DataOutputStream;
//import java.net.HttpURLConnection;

import java.net.URL;
import java.net.URLEncoder;

import javax.net.ssl.HttpsURLConnection;


/*
 * 
 * https://en.wikipedia.org/wiki/List_of_HTTP_header_fields
 * https://curl.haxx.se/rfc/cookie_spec.html
 * https://stackoverflow.com/questions/26701116/cookies-not-set-with-httpurlconnection
 * https://www.codejava.net/java-se/networking/java-urlconnection-and-httpurlconnection-examples
 * https://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
 * https://www.w3schools.com/tags/att_form_target.asp
 * https://cloud.google.com/appengine/docs/standard/java/issue-requests?csw=1
 * https://alvinalexander.com/blog/post/java/how-encode-java-string-send-web-server-safe-url
 * https://stackoverflow.com/questions/11494693/httpsurlconnection-and-cookies
 * 
 */
public class HttpWorker implements Runnable {
	
	
	private String baseUri = "";
	private String uriSuffix = "";


	private String userAgent = "";
	private String requestMethod = "GET";

	private int timeOutEncountered = 15 * 1000;
	@SuppressWarnings("unused")
	private boolean doOutput;
	
	/*
	 * Although annoying, sometimes we need to get through a proxy
	 */
	@SuppressWarnings("unused")
	private String proxyAddress = "";
	@SuppressWarnings("unused")
	private String proxyUsername = "";
	@SuppressWarnings("unused")
	private String proxyPassword = "";
	
	
	/*
	 * Credentials used to login to a give site,
	 * but this is experimental as it has no guarantees 
	 */
	@SuppressWarnings("unused")
	private String siteUsername = "";
	@SuppressWarnings("unused")
	private String sitePassword = "";
	
	
	private URL request = null;
	public HttpsURLConnection con = null;
	private String response = "";
	private int responseCode = -1;
	
	@SuppressWarnings("unused")
	private String cookies = "";
	/**
	 * Just a constructor
	 */
	public HttpWorker() {
		timeOutEncountered = 15 * 1000;
		requestMethod = "GET";
	}
	
	/**
	 * A constructor that accepts a base URI
	 * @param baseUri
	 */
	public HttpWorker(String baseUri) {
		this.baseUri = baseUri;
		timeOutEncountered = 15 * 1000;
		requestMethod = "GET";
		userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36";
	}
	
	/**
	 * Returns the assigned suffix portion of the full URI
	 * @return String
	 */
	public String getUriSuffix() {
		return uriSuffix;
	}

	/**
	 * The brave soul that chooses to use this monstrosity, unless 
	 * you think it's convenience (just means you're evil ;)
	 * @param uriSuffix String
	 */
	public void setUriSuffix(String uriSuffix) {
		this.uriSuffix = uriSuffix;
	}


	
	/**
	 * Concatenates the base URI with URI suffix.  It's a usability\experimental thing
	 * Again, it's kind of evil
	 * @return String
	 */
	public String getFullUri() {
		if(uriSuffix != null && uriSuffix.length() > 0)
			return baseUri.concat(uriSuffix);
		else 
			return baseUri;
	}
	
	/**
	 * Assuming valid input, attempts to connect to get a response
	 * over HTTPS, and fails "silently".
	 */
	public void connectGet() {
		requestMethod = "GET";
		try {
			request = new URL(this.getFullUri());
			con = (HttpsURLConnection) request.openConnection();
			con.setRequestMethod(requestMethod);
			con.setReadTimeout(timeOutEncountered);  //15 seconds
			con.setDoInput(true);
			con.setDoOutput(true);
			con.addRequestProperty("User-Agent", this.userAgent);
			
			//con.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void connectPost() {
		
	}
	
	public void connectPostB() {
		requestMethod = "POST";
		try {
			request = new URL(this.getFullUri());
			con = (HttpsURLConnection) request.openConnection();
			con.setRequestMethod(requestMethod);
			con.setReadTimeout(timeOutEncountered);  //15 seconds
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setRequestProperty("User-Agent", this.userAgent);
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.9");
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void pushParamters(String urlParameters) {
		if(urlParameters.length() > 0) {
			//wr.writeBytes(urlParameters);
			
			
			try {
				DataOutputStream wr = new DataOutputStream(con.getOutputStream());
				wr.writeBytes(URLEncoder.encode(
						urlParameters,
						java.nio.charset.StandardCharsets.ISO_8859_1.toString()));
				wr.flush();
				wr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	public void setReferer(String referer) {
		con.setRequestProperty("Referer" , referer);
	}
	
	public String getReferer() {
		return con.getRequestProperty("Referer");
	}
	/**
	 * Politely disconnects
	 */
	public void disconnect() {
		con.disconnect();
	}
	
	/**
	 * Simply returns a string that represents the type of HTTP request that will
	 * be made upon connection attempt
	 * @return String
	 */
	public String getRequestMethod() {
		return requestMethod;
	}
	
	/**
	 * 
	 * @param requestMethod
	 */
	public void setRequestMethod(String requestMethod) {
		this.requestMethod = requestMethod;
	}

	/**
	 * Brings down response and stores to a string, getResponse()
	 */
	public void setInputStreamToResponse() {
		

		//for(int i = 0; i < con.getHeaderFields().size(); i++) {
		//	System.out.print(con.getHeaderFieldKey(i) + " - ");
		//	System.out.println(con.getHeaderField(i));
		//}
		
		
		try {
			this.responseCode = con.getResponseCode();
		}  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			StringBuilder t = new StringBuilder();
			BufferedReader reader = 
					new BufferedReader(new InputStreamReader(con.getInputStream()));
			String line = null;
			while((line = reader.readLine()) != null) {
				t.append(line + "\n");
			}
			
			this.response = t.toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Typically used after using the setInputStreamToResponse() to get the 
	 * HTTP response data
	 * @return
	 */
	public String getResponse() {
		return this.response;
	}
	
	public int getResponseCode() {
		return this.responseCode;
	}
	
	public String getCookies() {
		return con.getHeaderField("Set-Cookie");
	}
	
	public void setCookies(String cookie) {
		//con.setHeaderField("Cookie", cookie);
		con.setRequestProperty("Cookie", cookie);
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
	

}
