package jch.lib.test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jch.lib.analytics.text.book.bible.Bible;
import jch.lib.analytics.text.book.bible.BibleSyntaxDatasetEntry;
import jch.lib.analytics.text.book.bible.KingJamesVersionBible;
import jch.lib.log.QLog;
import jch.lib.list.ChunkList;


import com.google.cloud.language.v1.AnalyzeSyntaxRequest;
//Imports the Google Cloud client library
import com.google.cloud.language.v1.Document;
import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.v1.EncodingType;
import com.google.cloud.language.v1.LanguageServiceClient;
import com.google.cloud.language.v1.Sentiment;
import com.google.cloud.language.v1.Token;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import javafx.application.Application;

import com.google.cloud.language.v1.AnalyzeSyntaxResponse;


public class GoogleTest {

	public GoogleTest() {
		// TODO Auto-generated constructor stub
		int i;
	}

	
	/***
	 * Sample for reading json file for creds
	 * @param loc
	 */
	public static void gcloudReadCreds(String credentialPath) {
	    //use JSON object to pull sensitive information instead of hardcoding
	    JSONObject jsonObj = null;
		try {
			String jsonString = Files.readString(Path.of(credentialPath));
			JSONParser jsonParser = new JSONParser();
			jsonObj =  (JSONObject) jsonParser.parse(jsonString);
			
		} catch (ParseException e) {
			e.printStackTrace();
			QLog.log(e.toString());
			QLog.log(e);
		} catch (IOException e) {
			e.printStackTrace();
			QLog.log(e.toString());
			QLog.log(e);
		}
	    
		QLog.log("test read:   "+ (String) jsonObj.get("type"));
		
		
	}
	
/*

{
  "type": "service_account",
  "project_id": "",
  "private_key_id": "",
  "private_key": "",
  "client_email": "",
  "client_id": "",
  "auth_uri": "",
  "token_uri": "",
  "auth_provider_x509_cert_url": "",
  "client_x509_cert_url": ""
}
*/
	
	
	public static void gcloudNaturalLanguage_example4() throws IOException {
		// Instantiates a client
		
		String file = "data/lang/eng/seed/published/project_gutenburg/json/KJV/00003-KJV-The Old Testament-The First Book of Moses  Called Genesis-1-3.txt";
		ChunkList cl = ChunkList.loadFile(file);
		

			// Instantiates a client
		try (LanguageServiceClient language = LanguageServiceClient.create()) {
		
			String text = "I have an angel on my shoulder but a devil in my hand.";
			com.google.cloud.language.v1.Document doc = com.google.cloud.language.v1.Document.newBuilder()
				      .setContent(text)
				      .setType(Type.PLAIN_TEXT)
				      .build();
			AnalyzeSyntaxRequest request = AnalyzeSyntaxRequest.newBuilder()
				      .setDocument(doc)
				      .setEncodingType(EncodingType.UTF16)
				      .build();
				  // analyze the syntax in the given text
			
			
			AnalyzeSyntaxResponse response =  language.analyzeSyntax(request);
				  // print the response
			  
			//System.out.println(response.toString());
			System.out.println(response);
		}
	}
	

	

	public static void gcloudNaturalLanguage_example2() {
		// Instantiates a client

		String fLoc = "data/lang/eng/seed/published/project_gutenburg/json/KJV/";
		String fName = "";
		int c = 0;
		Bible kjv = KingJamesVersionBible.buildBible();
		
		do {
			c++;
			
			fName = String.format ("%05d", c) + "-";
			fName = fName + "KJV-";
			fName = fName + kjv.getCurrentTestament().getCleanRefValue().substring(0,17) + "-";
			fName = fName + kjv.getCurrentBook().getCleanRefValue().trim() + "-";
			fName = fName + kjv.getCurrentChapter().getChapterNumber() + "-";
			fName = fName + kjv.getCurrentVerse().getVerseNumber() + ".json";
			fName = fName.replaceAll(":", "");
			
			System.out.println(fName);
			//System.out.println(kjv.getCurrentVerse().toString());
			
			
			String text;
			try (LanguageServiceClient language = LanguageServiceClient.create()) {
				
				text = kjv.getCurrentVerse().toString().trim();
				System.out.println(text);
				com.google.cloud.language.v1.Document doc = com.google.cloud.language.v1.Document.newBuilder()
					      .setContent(text)
					      .setType(Type.PLAIN_TEXT)
					      .build();
				AnalyzeSyntaxRequest request = AnalyzeSyntaxRequest.newBuilder()
					      .setDocument(doc)
					      .setEncodingType(EncodingType.UTF16)
					      .build();
				
				AnalyzeSyntaxResponse response =  language.analyzeSyntax(request);
				
				//print the response
				//System.out.println(response.toString());
				System.out.println(response.toString());
				
				
				FileWriter fw;
				try {
					fw = new FileWriter(fLoc+fName, true);
					fw.write(response.toString());
					fw.close();
			
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		while(kjv.moveNextVerse());
		
	 	
	}

	
	

	public static void gcloudNaturalLanguage_example3() throws IOException {
		// Instantiates a client
		String fLoc = "data/lang/eng/seed/published/project_gutenburg/dataset/KJV/";
		String fName = "kjv_full_syntax.csv";
		int c = 0;
		Bible kjv = KingJamesVersionBible.buildBible();
		FileWriter fw;
		fw = new FileWriter(fLoc+fName, true);
		
		fw.write(BibleSyntaxDatasetEntry.getHeaderString() + "\r\n");
		
		do {
			String text;
			try (LanguageServiceClient language = LanguageServiceClient.create()) {
				
				text = kjv.getCurrentVerse().toString().trim();
				System.out.println(text);
				com.google.cloud.language.v1.Document doc = com.google.cloud.language.v1.Document.newBuilder()
					      .setContent(text)
					      .setType(Type.PLAIN_TEXT)
					      .build();
				AnalyzeSyntaxRequest request = AnalyzeSyntaxRequest.newBuilder()
					      .setDocument(doc)
					      .setEncodingType(EncodingType.UTF16)
					      .build();
				
				AnalyzeSyntaxResponse response =  language.analyzeSyntax(request);
				
				//print the response
				//System.out.println(response.toString());
				//System.out.println(response.toString());
				
				BibleSyntaxDatasetEntry record = new BibleSyntaxDatasetEntry();
				
				
				for (Token token : response.getTokensList()) {
						
						record.setTestamentIdx(kjv.getIdxTestament());
						record.setBookIdx(kjv.getIdxBook());
						record.setChapterIdx(kjv.getIdxChapter());
						record.setVerseIdx(kjv.getIdxVerse());
						record.setText(token.getText().getContent());
						record.setBeginOffset(token.getText().getBeginOffset());
						record.setLemma(token.getLemma());
						record.setPartOfSpeechTag(token.getPartOfSpeech().getTag().toString());
						record.setAspect(token.getPartOfSpeech().getAspect().toString());
						record.setCasing(token.getPartOfSpeech().getCase().toString());
						record.setForm(token.getPartOfSpeech().getForm().toString());
						record.setGender(token.getPartOfSpeech().getGender().toString());
						record.setMood(token.getPartOfSpeech().getMood().toString());
						record.setNumber(token.getPartOfSpeech().getNumber().toString());
						record.setPerson(token.getPartOfSpeech().getPerson().toString());
						record.setProper(token.getPartOfSpeech().getProper().toString());
						record.setReciprocity(token.getPartOfSpeech().getReciprocity().toString());
						record.setTense(token.getPartOfSpeech().getTense().toString());
						record.setVoice(token.getPartOfSpeech().getVoice().toString());
						record.setDependencyEdgeHeadToken(token.getDependencyEdge().getHeadTokenIndex());
						record.setDependencyEdgeLabel(token.getDependencyEdge().getLabel().toString());
						
						System.out.println(record.getStringRecord());
						
						
						try {
							fw.write(record.getStringRecord() + "\r\n");
					
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		
		}
		while(kjv.moveNextVerse());
		
		fw.close();

	}

	public static void gcloudNaturalLanguage_example() {
		// Instantiates a client
		try (LanguageServiceClient language = LanguageServiceClient.create()) {
		
			String text = "I have an angel on my shoulder but a devil in my hand.";
			com.google.cloud.language.v1.Document doc = com.google.cloud.language.v1.Document.newBuilder()
				      .setContent(text)
				      .setType(Type.PLAIN_TEXT)
				      .build();
			AnalyzeSyntaxRequest request = AnalyzeSyntaxRequest.newBuilder()
				      .setDocument(doc)
				      .setEncodingType(EncodingType.UTF16)
				      .build();
				  // analyze the syntax in the given text
			
			
			AnalyzeSyntaxResponse response =  language.analyzeSyntax(request);
				  // print the response
			  
			System.out.println(response.toString());
  
				  
			
			for (Token token : response.getTokensList()) {
					
				    System.out.printf("\tText: %s\n", token.getText().getContent());
				    System.out.printf("\tBeginOffset: %d\n", token.getText().getBeginOffset());
				    System.out.printf("Lemma: %s\n", token.getLemma());
				    System.out.printf("PartOfSpeechTag: %s\n", token.getPartOfSpeech().getTag());
				    System.out.printf("\tAspect: %s\n", token.getPartOfSpeech().getAspect());
				    System.out.printf("\tCase: %s\n", token.getPartOfSpeech().getCase());
				    System.out.printf("\tForm: %s\n", token.getPartOfSpeech().getForm());
				    System.out.printf("\tGender: %s\n", token.getPartOfSpeech().getGender());
				    System.out.printf("\tMood: %s\n", token.getPartOfSpeech().getMood());
				    System.out.printf("\tNumber: %s\n", token.getPartOfSpeech().getNumber());
				    System.out.printf("\tPerson: %s\n", token.getPartOfSpeech().getPerson());
				    System.out.printf("\tProper: %s\n", token.getPartOfSpeech().getProper());
				    System.out.printf("\tReciprocity: %s\n", token.getPartOfSpeech().getReciprocity());
				    System.out.printf("\tTense: %s\n", token.getPartOfSpeech().getTense());
				    System.out.printf("\tVoice: %s\n", token.getPartOfSpeech().getVoice());
				    System.out.println("DependencyEdge");
				    System.out.printf("\tHeadTokenIndex: %d\n", token.getDependencyEdge().getHeadTokenIndex());
				    System.out.printf("\tLabel: %s\n\n", token.getDependencyEdge().getLabel());
				    
				    
				  }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	      // The text to analyze
	     /*
	      Document doc = Document.newBuilder()
	          .setContent(text).setType(Type.PLAIN_TEXT).build();

	      // Detects the sentiment of the text
	      Sentiment sentiment = language.analyzeSentiment(doc).getDocumentSentiment();
	      

	      System.out.printf("Text: %s%n", text);
	      System.out.printf("Sentiment: %s, %s%n", sentiment.getScore(), sentiment.getMagnitude());
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
	    	
	}
	


}
