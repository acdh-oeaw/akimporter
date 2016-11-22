/**
 * Updating Solr index via OAI harvesting. Mainly written for updating
 * authority records from DNB (GND), but parts could be used for other
 * sources providing an OAI interface.
 * 
 * Copyright (C) AK Bibliothek Wien 2016, Michael Birkner
 * 
 * This file is part of AkImporter.
 * 
 * AkImporter is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * AkImporter is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with AkImporter.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author   Michael Birkner <michael.birkner@akwien.at>
 * @license  http://www.gnu.org/licenses/gpl-3.0.html
 * @link     http://wien.arbeiterkammer.at/service/bibliothek/
 */

package main.java.betullam.akimporter.updater;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.w3c.dom.Document;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import ak.xmlhelper.XmlMerger;
import ak.xmlhelper.XmlParser;
import ak.xmlhelper.XmlValidator;
import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.main.Authority;
import main.java.betullam.akimporter.solrmab.Relate;
import main.java.betullam.akimporter.solrmab.indexing.MetsContentHandler;
import main.java.betullam.akimporter.solrmab.indexing.XmlContentHandler;
import main.java.betullam.akimporter.solrmab.relations.AuthorityFlag;
import main.java.betullam.akimporter.solrmab.relations.AuthorityMerge;


public class OaiUpdater {

	private long indexTimestamp;
	private int fileCounter = 0;


	/**
	 * Downloading, merging, parsing and indexing data from a generic OAI interface.
	 * 
	 * @param oaiUrl				String:			An URL to an OAI interface (everything before "?verb=...")
	 * @param format				String:			The format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param sets					List<String>:	The set(s) of the OAI interface that should be harvested
	 * @param structElements		List<String>:	If harvesting METS/MODS data with structure type data (e. g. from Goobi), define which structure types should be indexed.
	 * @param destinationPath		String:			A path to a local directory where the downloaded data should be stored (e. g. /home/username/oai_data)
	 * @param elementsToMerge		String:			The elements of the original XML that should be merged (e. g. "record")
	 * @param elementsToMergeLevel	int:			The level of the element that should be merged. Use other than "1" only if you have nested elements.
	 * @param oaiDatefile			String:			A path to a .properties file with at least a "from" date/time in format YYYY-MM-DDTHH:MM:SSZ. It could also contain an "until" date/time. The indicated time(s) are used for OAI harvesting. Example: /path/to/oai_date-time_file.properties.
	 * @param oaiPropertiesFile		String:			A path to a .properties file that contains instructions for parsing the XML data. The contents should be like "solrfield: /xpath/expression[1], /xpath/expression[2], rule1, rule2". Example: /path/to/oai_parsing.properties.
	 * @param solrServerBiblio		String:			An URL incl. core name of the Solr bibliographic index (e. g. http://localhost:8080/solr/biblio)
	 * @param print					boolean:		True if status messages sould be print, false otherwise.
	 * @param optimize				boolean:		True if the Solr server should be optimized after indexing, false otherwise.
	 */
	public void oaiGenericUpdate(
			String oaiUrl,
			String format,
			List<String> sets,
			List<String> structElements,
			String destinationPath,
			String elementsToMerge,
			int elementsToMergeLevel,
			String oaiDatefile,
			String oaiPropertiesFile,
			String solrServerBiblio,
			boolean print,
			boolean optimize
			) {
		this.indexTimestamp = new Date().getTime();
		String strIndexTimestamp = String.valueOf(this.indexTimestamp);

		AkImporterHelper.print(print, "\n-----------------------------------------------------------------------------");
		AkImporterHelper.print(print, "\nOAI harvest started: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.indexTimestamp))));

		// Creating Solr server
		HttpSolrServer sServerBiblio =  new HttpSolrServer(solrServerBiblio);

		// Set variables
		String oaiPathOriginal = stripFileSeperatorFromPath(destinationPath) + File.separator + "original" + File.separator + this.indexTimestamp;
		String oaiPathMerged = stripFileSeperatorFromPath(destinationPath) + File.separator + "merged" + File.separator + this.indexTimestamp;

		// Create directories if they do not exist
		mkDirIfNoExists(oaiPathOriginal);
		mkDirIfNoExists(oaiPathMerged);

		// Get "from" date/time from .properties file
		Properties oaiDateTimeOriginal = getOaiDateTime(oaiDatefile);
		String fromOriginal = oaiDateTimeOriginal.getProperty("from"); // Should be something like 2016-01-13T14:00:00Z

		for (String set : sets) {
			// Before downloading a set, write original "from" date/time to date/time-file so that every set downloads "from" and "until" the same time
			this.writeOaiDateFile(oaiDatefile, fromOriginal);

			// Download XML data of one set from an OAI interface
			oaiDownload(oaiUrl, format, set, oaiPathOriginal, oaiDatefile, this.indexTimestamp, print);
		}

		// Start merging all downloaded updates into one file
		AkImporterHelper.print(print, "\nMerging downloaded XML data ... ");
		String mergedFileName = oaiPathMerged + File.separator + this.indexTimestamp + ".xml";
		boolean isMergeSuccessful = mergeXmlFiles(oaiPathOriginal, mergedFileName, elementsToMerge, elementsToMergeLevel);
		if (isMergeSuccessful) {
			AkImporterHelper.print(print, "Done");
		} else {
			System.err.print("\nERROR: Merging downloaded files from OAI was not successful!");
			mergedFileName = null;
		}

		boolean isIndexingSuccessful = indexDownloadedOaiData(mergedFileName, format, sServerBiblio, structElements, elementsToMerge, strIndexTimestamp, oaiPropertiesFile, print);

		if (isIndexingSuccessful) {
			try {
				// Commit to Solr server:
				sServerBiblio.commit();

				// Connect child and parent volumes:
				AkImporterHelper.print(print, "\nStart linking parent and child records ... ");
				Relate relate = new Relate(sServerBiblio, strIndexTimestamp, false, false);
				boolean isRelateSuccessful = relate.isRelateSuccessful();
				if (isRelateSuccessful) {
					AkImporterHelper.print(print, "Done");
				}

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

	/**
	 * Reindex already downloaded data from OAI
	 * @param pathToOaiDir
	 * @param isValidationOk
	 * @param format
	 * @param sServerBiblio
	 * @param structElements
	 * @param elementsToMerge
	 * @param strIndexTimestamp
	 * @param oaiPropertiesFile
	 * @param optimize
	 * @param print
	 * @return
	 */
	public boolean reImportOaiData(String pathToOaiDir, boolean isValidationOk, String format, String solrServerBiblio, List<String> structElements, String elementsToMerge, String oaiPropertiesFile, boolean optimize, boolean print) {
		boolean isReindexingSuccessful = false;
		
		this.indexTimestamp = new Date().getTime();
		String strIndexTimestamp = String.valueOf(this.indexTimestamp);
		
		// Creating Solr server
		HttpSolrServer sServerBiblio =  new HttpSolrServer(solrServerBiblio);
		
		// Get a sorted list (oldest to newest) from all ongoing data deliveries:
		File fPathToMergedDir = new File(stripFileSeperatorFromPath(pathToOaiDir) + File.separator + "merged");
		List<File> fileList = (List<File>)FileUtils.listFiles(fPathToMergedDir, new String[] {"xml"}, true); // Get all xml-files recursively
		Collections.sort(fileList); // Sort oldest to newest

		boolean allFilesValid = false;

		if (isValidationOk) {
			AkImporterHelper.print(print, "\nStart validating all data ... ");
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
				AkImporterHelper.print(print, "Done");
			} else {
				// If there are errors in at least one file, stop the import process:
				System.err.println("\nError while validating. Import process was cancelled!\n");
				return false;
			}
		} else {
			AkImporterHelper.print(print, "\nSkipped validation!");
		}
		
		
		for (File file : fileList) {
			boolean isIndexingSuccessful = indexDownloadedOaiData(file.getAbsolutePath(), format, sServerBiblio, structElements, elementsToMerge, strIndexTimestamp, oaiPropertiesFile, print);

			if (isIndexingSuccessful) {
				try {
					
					// Commit to Solr server:
					sServerBiblio.commit();

					// Connect child and parent volumes:
					AkImporterHelper.print(print, "\nStart linking parent and child records ... ");
					Relate relate = new Relate(sServerBiblio, strIndexTimestamp, false, false);
					boolean isRelateSuccessful = relate.isRelateSuccessful();
					if (isRelateSuccessful) {
						AkImporterHelper.print(print, "Done");
					}

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
		
		return isReindexingSuccessful;
	}


	/**
	 * Start the parsing and indexing process of the downloaded XML data.
	 * 
	 * @param mergedFileName		String:			The paht to a file containing the downloaded XML data from the OAI interface
	 * @param format				String:			The format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param solrServerBiblio		String:			An URL incl. core name of the Solr bibliographic index (e. g. http://localhost:8080/solr/biblio)
	 * @param structElements		List<String>:	If harvesting METS/MODS data with structure type data (e. g. from Goobi), define which structure types should be indexed.
	 * @param elementsToMerge		String:			The elements of the original XML that should be merged (e. g. "record")
	 * @param strIndexTimestamp		String:			The current index timestamp
	 * @param oaiPropertiesFile		String:			A path to a .properties file that contains instructions for parsing the XML data. The contents should be like "solrfield: /xpath/expression[1], /xpath/expression[2], rule1, rule2". Example: /path/to/oai_parsing.properties.
	 * @param print					boolean:		True if status messages sould be print, false otherwise
	 * @return						boolean:		True if the index process was successful, false otherwise
	 */
	public boolean indexDownloadedOaiData(String mergedFileName, String format, HttpSolrServer sServerBiblio, List<String> structElements, String elementsToMerge, String strIndexTimestamp, String oaiPropertiesFile, boolean print) {
		boolean isIndexingSuccessful = false;
		try {
			// Create InputSource from XML file
			FileReader xmlData = new FileReader(mergedFileName);
			InputSource inputSource = new InputSource(xmlData);

			// Create variable for content handler
			ContentHandler contentHandler = null;

			if (format.contains("mets")) {
				// Create content handler for Mets/Mods data (e. g. from Goobi)
				contentHandler = new MetsContentHandler(sServerBiblio, structElements, strIndexTimestamp, print);
			} else {
				// Create content handler for generic XML data (e. g. from SSOAR)
				contentHandler = new XmlContentHandler(sServerBiblio, elementsToMerge, oaiPropertiesFile, strIndexTimestamp, print);
			}

			// Create SAX parser and set content handler:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();
			xmlReader.setContentHandler(contentHandler);

			// Start parsing & indexing:
			xmlReader.parse(inputSource);

			isIndexingSuccessful = true;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return isIndexingSuccessful;
	}


	/**
	 * Downloading, merging, parsing and indexing of authority records.
	 * 
	 * @param oaiUrl				String:			An URL to an OAI interface (everything before "?verb=...")
	 * @param format				String:			The format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param sets					List<String>:	The set(s) of the OAI interface that should be harvested
	 * @param destinationPath		String:			A path to a local directory where the downloaded data should be stored (e. g. /home/username/oai_data)
	 * @param oaiDatefile			String:			A path to a .properties file with at least a "from" date/time in format YYYY-MM-DDTHH:MM:SSZ. It could also contain an "until" date/time. The indicated time(s) are used for OAI harvesting. Example: /path/to/oai_date-time_file.properties.
	 * @param useDefaultAuthProps	boolean:		True if the default authority properties should be used, false otherwise
	 * @param customAuthProps		String:			A path to a .properties file if "useDefaultAuthProps" is false, e. g. /home/username/my_authority.properties
	 * @param solrServerAuth		String:			An URL incl. core name of the Solr authority index (e. g. http://localhost:8080/solr/authority)
	 * @param solrServerBiblio		String:			An URL incl. core name of the Solr bibliographic index (e. g. http://localhost:8080/solr/biblio)
	 * @param entities				String:			Authority entities (Persons, Corporation, Subjects, ...)
	 * @param merge					boolean:		True if authority data should be merged into the bibliographic data, false otherwise
	 * @param print					boolean:		True if status messages sould be print, false otherwise
	 * @param optimize				boolean:		True if the Solr server should be optimized after indexing, false otherwise
	 */
	public void oaiGndUpdate(String oaiUrl, String format, List<String> sets, String destinationPath, String oaiDatefile, boolean useDefaultAuthProps, String customAuthProps, String solrServerAuth, String solrServerBiblio, String entities, boolean merge, boolean print, boolean optimize) {

		boolean isAuthorityUpdateSuccessful = false;
		this.indexTimestamp = new Date().getTime();
		String strIndexTimestamp = String.valueOf(this.indexTimestamp);

		AkImporterHelper.print(print, "\n-------------------------------------------");
		AkImporterHelper.print(print, "\nOAI harvest started: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.indexTimestamp))));

		// Set variables
		String oaiPathOriginal = stripFileSeperatorFromPath(destinationPath) + File.separator + "original" + File.separator + this.indexTimestamp;
		String oaiPathMerged = stripFileSeperatorFromPath(destinationPath) + File.separator + "merged" + File.separator + this.indexTimestamp;

		// Create directories if they do not exist
		mkDirIfNoExists(oaiPathOriginal);
		mkDirIfNoExists(oaiPathMerged);

		// Get "from" date/time from .properties file
		Properties oaiDateTimeOriginal = getOaiDateTime(oaiDatefile);
		String fromOriginal = oaiDateTimeOriginal.getProperty("from"); // Should be something like 2016-01-13T14:00:00Z

		// First start of downloading and mergeing XML files from OAI interface:
		for (String set : sets) {
			// Before downloading a set, write original "from" date/time to date/time-file so that every set downloads "from" and "until" the same time
			this.writeOaiDateFile(oaiDatefile, fromOriginal);

			// Download XML data of one set from an OAI interface
			oaiDownload(oaiUrl, format, set, oaiPathOriginal, oaiDatefile, this.indexTimestamp, print);
		}

		// Start merging all downloaded updates into one file
		AkImporterHelper.print(print, "\nMerging downloaded XML data ... ");
		String mergedFileName = oaiPathMerged + File.separator + this.indexTimestamp + ".xml";
		boolean isMergeSuccessful = mergeXmlFiles(oaiPathOriginal, mergedFileName, "slim:record", 1);
		if (isMergeSuccessful) {
			AkImporterHelper.print(print, "Done");
		} else {
			System.err.print("\nERROR: Merging downloaded files from OAI was not successful!");
			mergedFileName = null;
		}

		if (mergedFileName != null) {
			AkImporterHelper.print(print, "\nIndexing new authority data ... ");
			// Index authority data from merged XML file:
			Authority auth = new Authority(
					false,
					false,
					null,
					mergedFileName,
					useDefaultAuthProps,
					customAuthProps,
					solrServerAuth,
					solrServerBiblio,
					strIndexTimestamp,
					false,
					optimize
					);
			isAuthorityUpdateSuccessful = auth.indexAuthority();
			AkImporterHelper.print(print, "Done");
		}

		if (isAuthorityUpdateSuccessful) {

			if (merge) {
				HttpSolrServer sServerAuth = new HttpSolrServer(solrServerAuth);
				HttpSolrServer sServerBiblio =  new HttpSolrServer(solrServerBiblio);

				// Set flag of existance to authority records:
				AuthorityFlag af = new AuthorityFlag(sServerBiblio, sServerAuth, strIndexTimestamp, true, print);
				af.setFlagOfExistance();

				// Merge authority records to bibliographic records:
				AuthorityMerge am = new AuthorityMerge(sServerBiblio, sServerAuth, strIndexTimestamp, true, print);
				am.mergeAuthorityToBiblio(entities);
			}

			AkImporterHelper.print(print, "\nDone updating from OAI interface.\nEVERYTHING WAS SUCCESSFUL!");
		} else {
			System.err.print("\nERROR WHILE UPDATING AUTHORITY DATA!");
		}

	}


	/**
	 * Downloading XML data from OAI interface.
	 * 
	 * @param oaiUrl				String:		An URL to an OAI interface (everything before "?verb=...")
	 * @param format				String:		The format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param sets					String:		The set of the OAI interface that should be harvested
	 * @param oaiPathOriginal		String:		Path to the local directory where the downloaded originals (not merged) should be stored (e. g. /home/username/oai_data/original)
	 * @param oaiDatefile			String:		A path to a .properties file with at least a "from" date/time in format YYYY-MM-DDTHH:MM:SSZ. It could also contain an "until" date/time. The indicated time(s) are used for OAI harvesting. Example: /path/to/oai_date-time_file.properties.
	 * @param downloadTimestamp		long:		Timstamp of the beginning of the downloading/harvesting process
	 * @param print					boolean:	True if status messages sould be print, false otherwise
	 */
	private void oaiDownload(String oaiUrl, String format, String set, String oaiPathOriginal, String oaiDatefile, long downloadTimestamp, boolean print) {

		String downloadDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date(downloadTimestamp)); // Use download time of the first run as "until" time

		// Get "from" and "until" date/time from .properties file
		Properties oaiDateTime = getOaiDateTime(oaiDatefile);
		String from = oaiDateTime.getProperty("from"); // Should be something like 2016-01-13T14:00:00Z
		String until = oaiDateTime.getProperty("until", downloadDateTime);

		// Get "from" timestamp using the date in the .properties file:
		long fromTimeStamp = 0;
		try {
			DateFormat fromDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date date = fromDateTimeFormat.parse(from);
			fromTimeStamp = date.getTime();
		} catch (ParseException e) {
			System.err.print("\nERROR: Parse error! Check if from date in .properties file is in format yyyy-MM-ddTHH:mm:ssZ.");
			e.printStackTrace();
		} catch (Exception e) {
			System.err.print("\nERROR");
			e.printStackTrace();
		}

		// Get difference between "from" and "until"
		long timeSpan = downloadTimestamp - fromTimeStamp;

		try {
			int httpResponseCode = getHttpResponseCode(new URL(oaiUrl+"?verb=ListRecords&metadataPrefix="+format+"&set="+set+"&from="+from+"&until="+until));

			if (httpResponseCode == 200) {

				AkImporterHelper.print(print, "\nDownloading XML from OAI interface ...\n\tSource:\t" + oaiUrl + "\n\tFormat:\t" + format + "\n\tSet:\t" + set + "\n\tTime:\t" + from + " - " + until);

				// Download updates from OAI interface and save them to a file. If there is a resumptionToken ("pages"),
				// then download all resumptions and save each to a sepearate file:
				String resumptionToken = null;
				do {
					fileCounter++;

					Document doc = getOaiUpdated(oaiUrl, format, set, from, until, resumptionToken);
					if (doc != null) {
						resumptionToken = getResumptionToken(doc);
						String fileName = this.indexTimestamp + "_" + String.format("%08d", fileCounter) + ".xml";
						writeXmlToFile(doc, oaiPathOriginal, fileName);
					} else {
						String urlCalled = "";
						if (resumptionToken == null) {
							urlCalled = oaiUrl+"?verb=ListRecords&metadataPrefix="+format+"&set="+set+"&from="+from+"&until="+until;
						} else {
							urlCalled = oaiUrl+"?verb=ListRecords&resumptionToken="+resumptionToken;
						}
						System.err.print("\nReturned XML document from OAI interface is null! URL called to get this document was: " + urlCalled);
						resumptionToken = null;
					}
				} while (resumptionToken != null);

				// Write current date/time to date/time-file for next update:
				this.writeOaiDateFile(oaiDatefile, until);

				// Start downloading XML data again until we reach today:
				if (this.indexTimestamp > downloadTimestamp) {
					oaiDownload(oaiUrl, format, set, oaiPathOriginal, oaiDatefile, this.indexTimestamp, print);
				}

			} else if (httpResponseCode == 413) { // Request entity too large (too many documents were requested from OAI interface)
				// Calculate new timestamp
				long newTimeStamp = (long)(downloadTimestamp - (timeSpan*0.15));
				oaiDownload(oaiUrl, format, set, oaiPathOriginal, oaiDatefile, newTimeStamp, print);
			} else {
				System.err.print("\nERROR: Getting HTTP response code " + httpResponseCode + " from OAI interface at " + oaiUrl);
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * ORIGINAL
	 * Downloads, saves and merges data from an OAI interface.
	 * 
	 * @param oaiUrl				String representing an URL to an OAI interface
	 * @param format				Format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param set					String representing the set of the OAI interface you want to harvest
	 * @param destinationPath		String representing a path to a local directory where the downloaded data should be stored (e. g. /home/username/oai_data)
	 * @param elementsToMerge		String representing the XML element that should be merged into a new file after downloading the OAI data (e. g. record).
	 * @param elementsToMergeLevel	int: Level of the XML element that should be merge if there are more of one of the same name. As default and for the top level element, use 1.
	 * @param oaiDatefile			String representing a path to a .properties file with at least a "from" date/time in format YYYY-MM-DDTHH:MM:SSZ. It could also contain an "until" date/time. Example: /path/to/oai_date-time_file.properties
	 * @param downloadTimestamp		long: Timestamp of start of current downloading task
	 * @param counter				int: Counter for naming files of downloaded OAI result pages (created by resumptionToken)
	 * @param print					boolean indicating whether to print status messages or not
	 * @return						String representing the path to a file with the downloaded data
	 */
	/* 
	private String oaiDownload(String oaiUrl, String format, String set, String destinationPath, String elementsToMerge, int elementsToMergeLevel, String oaiDatefile, long downloadTimestamp, int counter, boolean print) {

		String mergedFileName = null;
		String downloadDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date(downloadTimestamp)); // Use download time of the first run as "until" time

		// Get "from" and "until" date/time from .properties file
		Properties oaiDateTime = getOaiDateTime(oaiDatefile);
		String from = oaiDateTime.getProperty("from"); // Should be something like 2016-01-13T14:00:00Z
		String until = oaiDateTime.getProperty("until", downloadDateTime);

		// Get "from" timestamp using the date in the .properties file:
		long fromTimeStamp = 0;
		try {
			DateFormat fromDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date date = fromDateTimeFormat.parse(from);
			fromTimeStamp = date.getTime();
		} catch (ParseException e) {
			System.err.print("\nERROR: Parse error! Check if from date in .properties file is in format yyyy-MM-ddTHH:mm:ssZ.");
			e.printStackTrace();
		} catch (Exception e) {
			System.err.print("\nERROR");
			e.printStackTrace();
		}

		// Get difference between "from" and "until"
		long timeSpan = downloadTimestamp - fromTimeStamp;

		try {
			int httpResponseCode = getHttpResponseCode(new URL(oaiUrl+"?verb=ListRecords&metadataPrefix="+format+"&set="+set+"&from="+from+"&until="+until));

			if (httpResponseCode == 200) {

				AkImporterHelper.print(print, "\nHarvesting date range: " + from + " - " + until);

				// Set variables
				String oaiPathOriginal = stripFileSeperatorFromPath(destinationPath) + File.separator + "original" + File.separator + this.indexTimestamp;
				String oaiPathMerged = stripFileSeperatorFromPath(destinationPath) + File.separator + "merged" + File.separator + this.indexTimestamp;

				// Create directories if they do not exist
				mkDirIfNoExists(oaiPathOriginal);
				mkDirIfNoExists(oaiPathMerged);

				// Download updates from OAI interface and save them to a file. If there is a resumptionToken ("pages"),
				// then download all resumptions and save each to a sepearate file:
				AkImporterHelper.print(print, "\nDownloading XML data ... ");
				String resumptionToken = null;
				do {
					counter++;

					Document doc = getOaiUpdated(oaiUrl, format, set, from, until, resumptionToken);
					if (doc != null) {
						resumptionToken = getResumptionToken(doc);
						String fileName = this.indexTimestamp + "_" + String.format("%08d", counter) + ".xml";
						writeXmlToFile(doc, oaiPathOriginal, fileName);
					} else {
						String urlCalled = "";
						if (resumptionToken == null) {
							urlCalled = oaiUrl+"?verb=ListRecords&metadataPrefix="+format+"&set="+set+"&from="+from+"&until="+until;
						} else {
							urlCalled = oaiUrl+"?verb=ListRecords&resumptionToken="+resumptionToken;
						}
						System.err.print("\nReturned XML document from OAI interface is null! URL called to get this document was: " + urlCalled);
						resumptionToken = null;
					}
				} while (resumptionToken != null);
				AkImporterHelper.print(print, "Done");

				// Write current date/time to date/time-file for next update:
				this.writeOaiDateFile(oaiDatefile, until);

				// Start downloading XML data again until we reach today:
				if (this.indexTimestamp > downloadTimestamp) {
					oaiDownload(oaiUrl, format, set, destinationPath, elementsToMerge, elementsToMergeLevel, oaiDatefile, this.indexTimestamp, counter, print);
				} else {
					// Reached today - stop downloading and start merging
					AkImporterHelper.print(print, "\nAll OAI updates are downloaded.");

					// Start merging all downloaded updates into one file:
					AkImporterHelper.print(print, "\nMerging downloaded XML data ... ");
					mergedFileName = oaiPathMerged + File.separator + this.indexTimestamp + ".xml";
					boolean isMergeSuccessful = mergeXmlFiles(oaiPathOriginal, mergedFileName, elementsToMerge, elementsToMergeLevel);
					if (isMergeSuccessful) {
						AkImporterHelper.print(print, "Done");
					} else {
						System.err.print("\nERROR: Merging downloaded files from OAI was not successful!");
						mergedFileName = null;
					}
				}

			} else if (httpResponseCode == 413) {			
				// Calculate new timestamp
				long newTimeStamp = (long)(downloadTimestamp - (timeSpan*0.15));
				//System.err.print("\nToo many documents requested from OAI interface (> 100000) . Trying again with shorter time span ...");
				oaiDownload(oaiUrl, format, set, destinationPath, elementsToMerge, elementsToMergeLevel, oaiDatefile, newTimeStamp, counter, print);
				mergedFileName = null;
			} else {
				System.err.print("\nERROR: Getting HTTP response code " + httpResponseCode + " from OAI interface at " + oaiUrl);
				mergedFileName = null;
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
			mergedFileName = null;
		} catch (Exception e) {
			e.printStackTrace();
			mergedFileName = null;
		}

		return mergedFileName;

	}
	 */


	/**
	 * Getting the values from the .properties file for the date/time information for the OAI harvest date span ("from" and "until").
	 * 
	 * @param propertiesFile	String:		Path to the .properties file for the date/time information for the OAI harvest date span
	 * @return					Properties:	Properties object containing all values of the .properties file
	 */
	private Properties getOaiDateTime(String propertiesFile) {

		Properties oaiDateTimeProperties = new Properties();

		// Load .properties file:
		BufferedInputStream propertiesInputStream;
		try {
			propertiesInputStream = new BufferedInputStream(new FileInputStream(propertiesFile));
			oaiDateTimeProperties.load(propertiesInputStream);
			propertiesInputStream.close();
		} catch (FileNotFoundException e) {
			System.err.print("\nProperties file not found. Please make sure the file " + propertiesFile + " exists and is valid.");
			oaiDateTimeProperties = null;
			System.exit(1); // Stop execution of program
		} catch (IOException e) {
			e.printStackTrace();
			oaiDateTimeProperties = null;
			System.exit(1); // Stop execution of program
		} catch (Exception e) {
			e.printStackTrace();
		}

		return oaiDateTimeProperties;
	}


	/**
	 * Writing a given date/time to the .properties file for the "from" date/time information for the OAI harvest date span
	 * 
	 * @param oaiDatefilePath	String:	The path to the .properties file for the date/time information for the OAI harvest date span
	 * @param dateTime			String:	The "from" date/time in format YYYY-MM-DDTHH:MM:SSZ to write to the file
	 */
	private void writeOaiDateFile(String oaiDatefilePath, String dateTime) {
		File oaiDateFile = new File(oaiDatefilePath);
		String content = "from: " + dateTime;
		try {
			FileUtils.writeStringToFile(oaiDateFile, content);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * Merge multiple XML files into one.
	 * 
	 * @param sourcePath			String:		A path to a directory with multiple XML files (e. g. /folder/with/multiple/xml/files)
	 * @param destinationPath		String:		A path to a file containing the merged XML data (e. g. /path/to/merged_file.xml)
	 * @param elementsToMerge		String:		The XML element that should be merged into a new file after downloading the OAI data (e. g. record).
	 * @param elementsToMergeLevel	int:		Level of the XML element that should be merge if there are more of one of the same name. As default and for the top level element, use 1.
	 * @return						boolean:	True if the merging was successfull, false otherwise.
	 */
	private boolean mergeXmlFiles(String sourcePath, String destinationPath, String elementsToMerge, int elementsToMergeLevel) {
		XmlMerger xmlm = new XmlMerger();
		boolean isMergeSuccessful = xmlm.mergeElements(sourcePath, destinationPath, "collection", elementsToMerge, elementsToMergeLevel);
		return isMergeSuccessful;
	}


	/**
	 * Writing the downloaded data to a local XML file.
	 * 
	 * @param doc						Document:	The DOM document representing the downloaded XML data.
	 * @param destinationDirectory		String:		The path to a directory where to save the new XML file locally (e. g. /save/xml/file/here)
	 * @param fileName					String:		The name of the xml file (e. g. myNewXmlFile.xml)
	 */
	private void writeXmlToFile(Document doc, String destinationDirectory, String fileName) {

		destinationDirectory = stripFileSeperatorFromPath(destinationDirectory);
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer;

		try {
			transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

			Result result = new StreamResult(new File(destinationDirectory + File.separator + fileName));
			Source source = new DOMSource(doc);
			transformer.transform(source, result);
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * Getting the DOM document representing XML data from an OAI interface.
	 * 
	 * @param oaiUrl				String:		An URL to an OAI interface
	 * @param metadataPrefix		String:		Format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param set					String:		The set of the OAI interface that should be harvested
	 * @param from					String:		The "from" date/time (format YYYY-MM-DDTHH:MM:SSZ) for the date span in which to harvest changed, new or deleted records
	 * @param until					String:		The "until" date/time (format YYYY-MM-DDTHH:MM:SSZ) for the date span in which to harvest changed, new or deleted records
	 * @param resumptionToken		String:		The resumption token of the OAI response
	 * @return						Document:	A DOM document representing the XML requested from the OAI interface
	 */
	private Document getOaiUpdated(String oaiUrl, String metadataPrefix, String set, String from, String until, String resumptionToken) {
		URL url;
		Document document = null;

		try {
			if (resumptionToken == null) {
				url = new URL(oaiUrl+"?verb=ListRecords&metadataPrefix="+metadataPrefix+"&set="+set+"&from="+from+"&until="+until);
			} else {
				url = new URL(oaiUrl+"?verb=ListRecords&resumptionToken="+resumptionToken);
			}

			URLConnection conn = url.openConnection();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			document = db.parse(conn.getInputStream());
		} catch (MalformedURLException e) {
			e.printStackTrace();
			document = null;
		}  catch (ParserConfigurationException e) {
			e.printStackTrace();
			document = null;
		} catch (SAXException e) {
			e.printStackTrace();
			document = null;
		} catch (IOException e) {
			e.printStackTrace();
			document = null;
		} catch (Exception e) {
			e.printStackTrace();
			document = null;
		}

		return document;
	}


	/**
	 * Getting HTTP response code (e. g. 200, 404, etc.) to check for errors
	 * 
	 * @param url	URL:	The resource to check.
	 * @return		int:	The HTTP response code as an integer
	 */
	private int getHttpResponseCode(URL url) {
		int httpResponseCode = 0;
		try {
			HttpURLConnection conn = (HttpURLConnection)url.openConnection();
			httpResponseCode = conn.getResponseCode();
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return httpResponseCode;
	}


	/**
	 * Getting the resumption token of an OAI interface query result
	 * 
	 * @param doc	Document:	A DOM document representing the XML of the OAI interface query result
	 * @return		String:		The resumption token or null if none was found
	 */
	private String getResumptionToken(Document doc) {
		String resumptionToken = null;
		XmlParser xmlParser = new XmlParser();
		String xpath = "/OAI-PMH/ListRecords/resumptionToken";
		try {
			resumptionToken = xmlParser.getTextValue(doc, xpath);
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resumptionToken;
	}


	/**
	 * Remove last file separator character of a String representing a path to a directory
	 *  
	 * @param path	String:	A path to a directory.
	 * @return		String:	The path without the last file separator character.
	 */
	private static String stripFileSeperatorFromPath(String path) {
		if (!path.equals(File.separator) && (path.length() > 0) && (path.charAt(path.length()-1) == File.separatorChar)) {
			path = path.substring(0, path.length()-1);
		}
		return path;
	}


	/**
	 * Creates a directory if it does not exist.
	 * 
	 * @param path	String:	Path to the directory that should be created.
	 */
	private static void mkDirIfNoExists(String path) {
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
	}

}