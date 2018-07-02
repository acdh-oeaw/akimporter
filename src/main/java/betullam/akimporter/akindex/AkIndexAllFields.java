package main.java.betullam.akimporter.akindex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import ak.xmlhelper.XmlParser;
import main.java.betullam.akimporter.main.AkImporterHelper;


public class AkIndexAllFields {

	private String akiSolr = null;
	private String akiAllFieldsPath = null;
	private boolean print = false;
	long startTimestampLong = new Date().getTime();
	String timestampString = String.valueOf(this.startTimestampLong);
	String timeFormatted = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(startTimestampLong));

	
	public AkIndexAllFields(String akiSolr, String akiAllFieldsPath, boolean print) {
		this.akiSolr = akiSolr;
		this.akiAllFieldsPath = akiAllFieldsPath;
		this.print = print;

		File phpFile = new File(akiAllFieldsPath);
		File phpFileParent = phpFile.getParentFile();
		if (!phpFileParent.exists()) {
			System.err.print("\nERROR: directory does not exist\nThe parent directory \"" + phpFileParent.getAbsolutePath() + "\" for the php file that you indicated in \"AkImporter.properties\" (setting \"akindex.setting.path.allfields\") does not exist. Make sure that this directory exists.\n");
		} else if (!phpFile.isFile() || !phpFile.getName().endsWith(".php")) {
			System.err.print("\nERROR: invalid file\nThe file with the name \"" + phpFile.getAbsolutePath() + "\" that you indicated in \"AkImporter.properties\" (setting \"akindex.setting.path.allfields\") is not a PHP file. Make sure the filename ends with \".php\" and that it has a valid path.\n");
		} else if (!phpFileParent.canWrite()) {
			System.err.print("\nERROR: Not writeable\nYou are not allowed to write to the directory \"" + phpFileParent.getAbsolutePath() + "\" that you indicated in \"AkImporter.properties\" (setting \"akindex.setting.path.allfields\"). Make sure the user who executes this script is allowed to write to this directory.\n");
		} else {
			this.generateAllFieldsFile();
		}
	}

	
	private boolean generateAllFieldsFile() {
		boolean successfullyGeneratedFile = false;
		
		AkImporterHelper.print(this.print, "\n-----------------------------------------------------------------------------");
		AkImporterHelper.print(this.print, "\nGenerating the \"all fields\" file for AKindex: " + this.timeFormatted);

		// Creating Solr server
		HttpSolrServer sServerAkIndex =  new HttpSolrServer(this.akiSolr);
		String lukeUrl = sServerAkIndex.getBaseURL() + "/admin/luke";
		Document lukeDoc = getLukeDomDocument(lukeUrl);
		
		// We could not get the document in getLukeDomDocument, so we return false to stop the execution of the other code below
		if (lukeDoc == null) {
			return false;
		}

		XmlParser xmlp = new XmlParser();
		try {
			String phpArrayString = "";
			Node fieldsNode = xmlp.getNodeByXpath(lukeDoc, "/response/lst[@name='fields']");
			NodeList fieldsNodeChilds = fieldsNode.getChildNodes();
			
			for (int i = 0; i < fieldsNodeChilds.getLength(); i++) {
		        Node field = fieldsNodeChilds.item(i);
		        
		        if (field.getNodeType() == Node.ELEMENT_NODE) {
		        	String fieldName = field.getAttributes().getNamedItem("name").getNodeValue();
		        	NodeList fieldChilds = field.getChildNodes();
		        	String dynamicBase = null;
		        	
		        	for (int j = 0; j < fieldChilds.getLength(); j++) {
		        		Node fieldChild = fieldChilds.item(j);
		        		String attrValue = fieldChild.getAttributes().getNamedItem("name").getNodeValue();
		        		
		        		if (attrValue.equals("dynamicBase")) {
		        			dynamicBase = fieldChild.getTextContent();
		        		}
		        	}

		        	// Get only real MARC/MAB fieldnames
		        	if (dynamicBase != null && dynamicBase.equals("*_ss") && !fieldName.equals("indexTimestamp_s") && !fieldName.equals("indexTimestamp_l")) {
			        	String solrFieldName = fieldName;
		        		String fieldNameClean = fieldName.replaceFirst("_ss$", "").replaceFirst("^_", "");
		        		String fieldType = null;
		        		
		        		if (fieldNameClean.length() == 3) {
		        			fieldType = "controlfield";
		        		} else if (fieldNameClean.length() == 6) {
		        			fieldType = "datafield";
		        			// Add $ signs:
		        			//fieldNameClean = new StringBuilder(fieldNameClean).insert(3, "\\$").insert(7, "\\$").toString();
		        		}

		        		if (fieldType != null) {			        		
			        		phpArrayString += "\t\"" + solrFieldName + "\"=>\"" + fieldNameClean + "\",\n";
		        		}
		        	}
		        }
		    }
			
			// Get rid of last comma
			phpArrayString = (phpArrayString != null) ? phpArrayString.replaceAll(",$", "") : null;
			if (phpArrayString != null && !phpArrayString.isEmpty()) {
				String allFieldsString = "$allFields = [\n" + phpArrayString + "];";
				String phpString = "<?php\n" + allFieldsString + "\n?>";
				FileWriter fileWriter = null;
				BufferedWriter bufferedWriter = null;
				
				try {
					fileWriter = new FileWriter(this.akiAllFieldsPath);
					bufferedWriter = new BufferedWriter(fileWriter);
					bufferedWriter.write(phpString);
				} catch (IOException e) {
					System.err.println("ERROR!\n");
					e.printStackTrace();
					successfullyGeneratedFile = false;
				} finally {
					try {
						if (bufferedWriter != null) { bufferedWriter.close(); }
						if (fileWriter != null) { fileWriter.close(); }
					} catch (IOException e) {
						System.err.println("ERROR!\n");
						e.printStackTrace();
						successfullyGeneratedFile = false;
					}
				}
			}
			
			long endTimestampLong = new Date().getTime();
			AkImporterHelper.print(print, "\nDone. Execution time: " + AkImporterHelper.getExecutionTime(startTimestampLong, endTimestampLong) + "\n");
			
			successfullyGeneratedFile = true;
		} catch (XPathExpressionException e) {
			System.err.println("ERROR!\n");
			e.printStackTrace();
			successfullyGeneratedFile = false;
		}
		
		return successfullyGeneratedFile;
	}


	private Document getLukeDomDocument(String lukeUrl) {
		URL url;
		Document document = null;
		HttpURLConnection conn = null;

		try {
			url = new URL(lukeUrl);
			conn = (HttpURLConnection)url.openConnection();
			int httpResponseCode = conn.getResponseCode();
			if (httpResponseCode == 200) {
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();
				document = db.parse(conn.getInputStream());
			} else {
				System.err.print("\nERROR: Getting HTTP response code " + httpResponseCode + " from url " + lukeUrl + "\n");
			}
		} catch (MalformedURLException e) {
			System.err.print("Error while indexing all fields");
			e.printStackTrace();
			document = null;
		}  catch (ParserConfigurationException e) {
			System.err.print("Error while indexing all fields");
			e.printStackTrace();
			document = null;
		} catch (SAXException e) {
			System.err.print("Error while indexing all fields");
			e.printStackTrace();
			document = null;
		} catch (IOException e) {
			System.err.print("Error while indexing all fields");
			e.printStackTrace();
			document = null;
		} catch (Exception e) {
			System.err.print("Error while indexing all fields");
			e.printStackTrace();
			document = null;
		} finally {
			if (conn != null) { conn.disconnect(); }
		}

		return document;
	}
}