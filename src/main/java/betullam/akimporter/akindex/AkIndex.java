package main.java.betullam.akimporter.akindex;

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

public class AkIndex {

	private String akiSolr = null;
	private String akiPath = null;
	private String akiElements = null;
	private String akiIdXpath = null;
	private boolean akiValidateSkip = false;
	private boolean print = false;
	private boolean optimize = false;
	
	//endTime = System.currentTimeMillis();
	//AkImporterHelper.print(print, "Done indexing to Solr. Execution time: " + AkImporterHelper.getExecutionTime(startTime, endTime) + "\n\n");
	long indexStartTimestampLong = new Date().getTime();
	String indexTimestampString = String.valueOf(this.indexStartTimestampLong);
	String indexTimeFormatted = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(indexStartTimestampLong));

	public AkIndex(String akiSolr, String akiPath, String akiElements, String akiElementsLevel, String akiIdXpath, boolean akiValidateSkip, boolean print, boolean optimize) {
		
		this.akiSolr = akiSolr;
		this.akiPath = akiPath;
		this.akiElements = akiElements;
		this.akiIdXpath = akiIdXpath;
		this.akiValidateSkip = akiValidateSkip;
		this.print = print;
		this.optimize = optimize;

		this.startAkIndexing();
	}


	private boolean startAkIndexing() {
		boolean isAkIndexingSuccessful = false;

		AkImporterHelper.print(this.print, "\n-----------------------------------------------------------------------------");
		AkImporterHelper.print(this.print, "\nStarting indexing for AKindex: " + this.indexTimeFormatted);

		// Creating Solr server
		HttpSolrServer sServerAkIndex =  new HttpSolrServer(this.akiSolr);

		// Getting xml file(s)
		File xmlFile = new File(this.akiPath);
		List<File> fileList = new ArrayList<File>();
		if (xmlFile.isDirectory()) {
			fileList = (List<File>)FileUtils.listFiles(xmlFile, new String[] {"xml"}, true); // Get all xml-files recursively
		} else {
			fileList.add(xmlFile);
		}

		// XML Validation
		boolean allFilesValid = false;
		if (!this.akiValidateSkip) {
			AkImporterHelper.print(this.print, "\nStart validating XML data\t-> please wait ...");
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
		} else {
			// We skip validation so we have to set allFilesValid to true
			AkImporterHelper.print(this.print, "\nSkipping validation\t\t-> Done");
			allFilesValid = true;
		}

		// If all files are valid, go on with the import process
		if (allFilesValid && !this.akiValidateSkip) {
			AkImporterHelper.print(this.print, "\nStart validating XML data\t-> Done");
		} else if (!allFilesValid) {
			// If there are errors in at least one file, stop the import process:
			System.err.println("\nError while validating. Import process was cancelled!\n");
			return false;
		}

		for (File file : fileList) {
			isAkIndexingSuccessful = this.akIndexing(file.getAbsolutePath(), sServerAkIndex);

			if (isAkIndexingSuccessful) {
				try {
					// Commit to Solr server:
					sServerAkIndex.commit();

					if (optimize) {
						AkImporterHelper.print(this.print, "\nOptimizing Solr Server\t\t-> please wait ...");
						AkImporterHelper.solrOptimize(sServerAkIndex);
						AkImporterHelper.print(this.print, "\nOptimizing Solr Server\t\t-> Done");
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
		
		return isAkIndexingSuccessful;
	}


	private boolean akIndexing(String fileName, HttpSolrServer sServerAkIndex) {

		boolean isIndexingSuccessful = false;

		try {
			// Create InputSource from XML file
			FileReader xmlData = new FileReader(fileName);
			InputSource inputSource = new InputSource(xmlData);

			// Create variable for content handler
			ContentHandler contentHandler = null;

			// Create content handler for XML data
			contentHandler = new AkIndexContentHandler(sServerAkIndex, this.akiElements, this.akiIdXpath, this.indexTimestampString, this.indexTimeFormatted, this.print);

			// Create SAX parser and set content handler:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();

			// Set SAX parser namespace aware (namespaceawareness)
			xmlReader.setFeature("http://xml.org/sax/features/namespace-prefixes", true);

			// Set the content handler for the SAX parser
			xmlReader.setContentHandler(contentHandler);

			// Start parsing & indexing:
			AkImporterHelper.print(this.print, "\nIndexing documents to Solr\t-> please wait ...");
			xmlReader.parse(inputSource);
			isIndexingSuccessful = true;
			if (isIndexingSuccessful) {
				AkImporterHelper.print(this.print, "\nIndexing documents to Solr\t-> Done");
			} else {
				System.err.println("ERROR");
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
