package jch.lib.file;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.*;
import java.io.*;

public class XmlWorker implements Runnable {
	DocumentBuilderFactory docFactory = null;
	DocumentBuilder docBuilder = null;
	Document doc = null;
	
	public XmlWorker() {
		docFactory = DocumentBuilderFactory.newInstance();
		try {
			docBuilder = docFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public XmlWorker(String xmlInput) {
		docFactory = DocumentBuilderFactory.newInstance();
		try {
			docBuilder = docFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			doc = docBuilder.parse(xmlInput);
		} catch (SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public NodeList getElementsByTagName(String tagName) {
		if(doc != null) return doc.getElementsByTagName(tagName);
		else return null;
	}
	
	public Element getDocumentElement() {
		if(doc != null) return doc.getDocumentElement();
		else return null;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

}
