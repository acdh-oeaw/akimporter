package main.java.betullam.akimporter.solrmab;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import ak.xmlhelper.XmlValidator;
import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.solrmab.indexing.XmlContentHandler;
import main.java.betullam.akimporter.updater.FtpDownload;

public class XmlIndex {

	private String xmlName;
	private String path;
	private String propertiesFile;
	private String solrBibl;
	private String elements;
	private List<String> include;
	private List<String> exclude;
	private String deleteBeforeImport;
	private boolean ftpDownload;
	private String xmlFtpHost;
	private int xmlFtpPort;
	private String xmlFtpUser;
	private String xmlFtpPass;
	private String xmlFtpRemotePath;
	private String xmlFtpLocalPath;
	private boolean compareFiles;
	private boolean print;
	private boolean optimize;
	private String indexTimestamp;

	public XmlIndex(
			String xmlName,
			String path,
			String propertiesFile,
			String solrBibl,
			String elements,
			List<String> include,
			List<String> exclude,
			String deleteBeforeImport,
			boolean ftpDownload,
			String xmlFtpHost,
			int xmlFtpPort,
			String xmlFtpUser,
			String xmlFtpPass,
			String xmlFtpRemotePath,
			String xmlFtpLocalPath,
			boolean compareFiles,
			boolean print,
			boolean optimize) {

		this.xmlName = xmlName;
		this.path = path;
		this.propertiesFile = propertiesFile;
		this.solrBibl = solrBibl;
		this.elements = elements;
		this.include = include;
		this.exclude = exclude;
		this.deleteBeforeImport = deleteBeforeImport;
		this.ftpDownload = ftpDownload;
		this.xmlFtpHost = xmlFtpHost;
		this.xmlFtpPort = xmlFtpPort;
		this.xmlFtpUser = xmlFtpUser;
		this.xmlFtpPass = xmlFtpPass;
		this.xmlFtpRemotePath = xmlFtpRemotePath;
		this.xmlFtpLocalPath = xmlFtpLocalPath;
		this.compareFiles = compareFiles;
		this.print = print;
		this.optimize = optimize;
		this.indexTimestamp = String.valueOf(new Date().getTime());

		this.xmlGenericIndexing();
	}


	private boolean xmlGenericIndexing() {
		boolean isIndexingSuccessful = false;
		
		AkImporterHelper.print(print, "\n-----------------------------------------------------------------------------");
		AkImporterHelper.print(print, "\nStarting XML importing: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.indexTimestamp))));

		if (this.ftpDownload) {
			if (this.xmlFtpHost != null && this.xmlFtpUser != null && this.xmlFtpPass != null && this.xmlFtpLocalPath != null) {
				FtpDownload ftpDownload = new FtpDownload();
				ftpDownload.downloadFiles(this.xmlFtpRemotePath, null, this.xmlFtpLocalPath, this.xmlFtpHost, this.xmlFtpPort, this.xmlFtpUser, this.xmlFtpPass, this.indexTimestamp, this.compareFiles, true);
			} else {
				System.err.println("Error: Check if settings \"xml."+this.xmlName+".ftpHost\", \"xml."+this.xmlName+".ftpUser\", \"xml."+this.xmlName+".ftpPass\" and \"xml."+this.xmlName+".ftpLocalPath\" are set in AkImporter.properties.");
				System.exit(1);
			}
		}
		
		// Creating Solr server
		HttpSolrServer sServerBiblio =  new HttpSolrServer(this.solrBibl);

		File xmlFile = new File(this.path);

		List<File> fileList = new ArrayList<File>();
		if (xmlFile.isDirectory()) {
			fileList = (List<File>)FileUtils.listFiles(xmlFile, new String[] {"xml"}, true); // Get all xml-files recursively
		} else {
			fileList.add(xmlFile);
		}

		
		// XML Validation
		boolean allFilesValid = false;
		AkImporterHelper.print(print, "\nStart validating XML data ... ");
		XmlValidator bxh = new XmlValidator();
		for (File file : fileList) {
			boolean hasValidationPassed = bxh.validateXML(file.getAbsolutePath());
			if (hasValidationPassed) {
				allFilesValid = true;
			} else {
				allFilesValid = false;
				System.err.println("Error in file " + file.getName() + ". Import process was cancelled.");
				return allFilesValid;
			}
		}
		// If all files are valid, go on with the import process
		if (allFilesValid) {
			AkImporterHelper.print(print, "Done\n");
		} else {
			// If there are errors in at least one file, stop the import process:
			System.err.println("\nError while validating. Import process was cancelled!\n");
			return false;
		}
		
		if (deleteBeforeImport != null && !deleteBeforeImport.trim().isEmpty()) {
			AkImporterHelper.deleteRecordsByQuery(sServerBiblio, deleteBeforeImport);
		}
		
		for (File file : fileList) {
			isIndexingSuccessful = indexXmlData(file.getAbsolutePath(), sServerBiblio);

			if (isIndexingSuccessful) {
				try {
					// Commit to Solr server:
					sServerBiblio.commit();
					
					if (optimize) {
						AkImporterHelper.print(print, "\nOptimizing Solr Server ... ");
						AkImporterHelper.solrOptimize(sServerBiblio);
						AkImporterHelper.print(print, "Done");
					}
				} catch (SolrServerException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return isIndexingSuccessful;

	}
	
	
	public boolean indexXmlData(String fileName, HttpSolrServer solrServerBiblio) {
		
		boolean isIndexingSuccessful = false;
		
		try {
			// Create InputSource from XML file
			FileReader xmlData = new FileReader(fileName);
			InputSource inputSource = new InputSource(xmlData);

			// Create variable for content handler
			ContentHandler contentHandler = null;

			// Create content handler for generic XML data
			contentHandler = new XmlContentHandler(solrServerBiblio, elements, include, exclude, propertiesFile, indexTimestamp, print);
			
			// Create SAX parser and set content handler:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();
			
			// Set SAX parser namespace aware (namespaceawareness)
			xmlReader.setFeature("http://xml.org/sax/features/namespace-prefixes", true);
			
			// Set the content handler for the SAX parser
			xmlReader.setContentHandler(contentHandler);

			// Start parsing & indexing:
			AkImporterHelper.print(print, "\nIndexing documents to Solr ... ");
			xmlReader.parse(inputSource);
			isIndexingSuccessful = true;
			if (isIndexingSuccessful) {
				AkImporterHelper.print(print, "Done");
				AkImporterHelper.print(print, "\nEVERYTHING WAS SUCCESSFUL");
			} else {
				AkImporterHelper.print(print, "ERROR");
			}
			
		} catch (FileNotFoundException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (SAXException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (IOException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		}

		return isIndexingSuccessful;
	}

	
	
}