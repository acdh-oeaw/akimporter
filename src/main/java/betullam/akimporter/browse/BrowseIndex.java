package main.java.betullam.akimporter.browse;

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

public class BrowseIndex {

	String biSolr = null;
	String biPath = null;
	String biElements = null;
	String biElementsLevel = null;
	String biIdXpath = null;
	boolean print = false;
	boolean optimize = false;
	
	//endTime = System.currentTimeMillis();
	//AkImporterHelper.print(print, "Done indexing to Solr. Execution time: " + AkImporterHelper.getExecutionTime(startTime, endTime) + "\n\n");
	long indexStartTimestampLong = new Date().getTime();
	String indexTimestampString = String.valueOf(this.indexStartTimestampLong);
	String indexTimeFormatted = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(indexStartTimestampLong));

	public BrowseIndex(String biSolr, String biPath, String biElements, String biElementsLevel, String biIdXpath, boolean print, boolean optimize) {
		/*
		System.out.println("biSolr: " + biSolr);
		System.out.println("biPath: " + biPath);
		System.out.println("biElements: " + biElements);
		System.out.println("biElementsLevel: " + biElementsLevel);
		System.out.println("biIdXpath: " + biIdXpath);
		*/
		
		this.biSolr = biSolr;
		this.biPath = biPath;
		this.biElements = biElements;
		this.biElementsLevel = biElementsLevel;
		this.biIdXpath = biIdXpath;
		this.print = print;
		this.optimize = optimize;

		this.startBrowseIndexing();
	}


	private boolean startBrowseIndexing() {
		boolean isBrowseIndexingSuccessful = false;

		AkImporterHelper.print(this.print, "\n-----------------------------------------------------------------------------");
		AkImporterHelper.print(this.print, "\nStarting browse indexing: " + this.indexTimeFormatted);

		// Creating Solr server
		HttpSolrServer sServerBrowseIndex =  new HttpSolrServer(this.biSolr);

		// Getting xml file(s)
		File xmlFile = new File(this.biPath);
		List<File> fileList = new ArrayList<File>();
		if (xmlFile.isDirectory()) {
			fileList = (List<File>)FileUtils.listFiles(xmlFile, new String[] {"xml"}, true); // Get all xml-files recursively
		} else {
			fileList.add(xmlFile);
		}

		// XML Validation
		boolean allFilesValid = false;
		AkImporterHelper.print(this.print, "\nStart validating XML data\t-> please wait ...\r");
		XmlValidator bxh = new XmlValidator();
		for (File file : fileList) {
			boolean hasValidationPassed = bxh.validateXML(file.getAbsolutePath());
			if (hasValidationPassed) {
				allFilesValid = true;
			} else {
				allFilesValid = false;
				System.err.println("\nError in file " + file.getName() + ". Import process was cancelled.\n");
				return allFilesValid;
			}
		}

		// If all files are valid, go on with the import process
		if (allFilesValid) {
			AkImporterHelper.print(this.print, "Start validating XML data\t-> Done             ");
		} else {
			// If there are errors in at least one file, stop the import process:
			System.err.println("\nError while validating. Import process was cancelled!\n");
			return false;
		}

		for (File file : fileList) {
			isBrowseIndexingSuccessful = this.browseIndexing(file.getAbsolutePath(), sServerBrowseIndex);

			if (isBrowseIndexingSuccessful) {
				try {
					// Commit to Solr server:
					sServerBrowseIndex.commit();

					if (optimize) {
						AkImporterHelper.print(this.print, "\nOptimizing Solr Server\t\t-> please wait ...\r");
						AkImporterHelper.solrOptimize(sServerBrowseIndex);
						AkImporterHelper.print(this.print, "Optimizing Solr Server\t\t-> Done             ");
					}
				} catch (SolrServerException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		long indexEndTimestampLong = new Date().getTime();
		AkImporterHelper.print(print, "\nDone indexing to Solr. Execution time: " + AkImporterHelper.getExecutionTime(indexStartTimestampLong, indexEndTimestampLong) + "\n\n");
		
		return isBrowseIndexingSuccessful;
	}


	private boolean browseIndexing(String fileName, HttpSolrServer sServerBrowseIndex) {

		boolean isIndexingSuccessful = false;

		try {
			// Create InputSource from XML file
			FileReader xmlData = new FileReader(fileName);
			InputSource inputSource = new InputSource(xmlData);

			// Create variable for content handler
			ContentHandler contentHandler = null;

			// Create content handler for XML data
			contentHandler = new BrowseIndexContentHandler(sServerBrowseIndex, this.biElements, this.biIdXpath, this.indexTimestampString, this.indexTimeFormatted, this.print);

			// Create SAX parser and set content handler:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();

			// Set SAX parser namespace aware (namespaceawareness)
			xmlReader.setFeature("http://xml.org/sax/features/namespace-prefixes", true);

			// Set the content handler for the SAX parser
			xmlReader.setContentHandler(contentHandler);

			// Start parsing & indexing:
			AkImporterHelper.print(this.print, "\nIndexing documents to Solr\t-> please wait ...\r");
			xmlReader.parse(inputSource);
			isIndexingSuccessful = true;
			if (isIndexingSuccessful) {
				AkImporterHelper.print(this.print, "Indexing documents to Solr\t-> Done             ");
				//AkImporterHelper.print(this.print, "\nINDEXING WAS SUCCESSFUL");
			} else {
				AkImporterHelper.print(this.print, "ERROR");
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
