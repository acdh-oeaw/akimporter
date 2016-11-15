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
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import ak.xmlhelper.XmlMerger;
import ak.xmlhelper.XmlParser;
import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.main.Authority;
import main.java.betullam.akimporter.solrmab.Relate;
import main.java.betullam.akimporter.solrmab.indexing.MetsContentHandler;
import main.java.betullam.akimporter.solrmab.relations.AuthorityFlag;
import main.java.betullam.akimporter.solrmab.relations.AuthorityMerge;


public class OaiUpdater {

	//String indexTimeStamp;
	//private HttpSolrServer solrServer = null;
	private long indexTimestamp;
	private AkImporterHelper akiHelper = null;


	public void oaiGenericUpdate(
			String oaiUrl,
			String format,
			String set,
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
		
		try {
			// Creating Solr server
			HttpSolrServer sServerBiblio =  new HttpSolrServer(solrServerBiblio);
			
			// Creating instance of AkImporterHelper
			akiHelper = new AkImporterHelper(sServerBiblio);
			
			akiHelper.print(print, "\n-------------------------------------------");
			akiHelper.print(print, "\nOAI harvest started: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.indexTimestamp))));
			 
			// First start of downloading and mergeing XML files from OAI interface:
			String mergedOaiDataFileName = oaiDownload(oaiUrl, format, set, destinationPath, elementsToMerge, elementsToMergeLevel, oaiDatefile, this.indexTimestamp, 0, print);
			
			// Create InputSource from XML file
			FileReader xmlData = new FileReader(mergedOaiDataFileName);
			InputSource inputSource = new InputSource(xmlData);
			
			// Create content handler for Mets/Mods data
			MetsContentHandler metsContentHandler = new MetsContentHandler(sServerBiblio, structElements, strIndexTimestamp, print);
			
			// Create SAX parser and set content handler:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();
			xmlReader.setContentHandler(metsContentHandler);
			
			// Start parsing & indexing:
			xmlReader.parse(inputSource);

			// Connect child and parent volumes:
			akiHelper.print(print, "\nStart linking parent and child records ... ");
			Relate relate = new Relate(sServerBiblio, strIndexTimestamp, false, false);
			boolean isRelateSuccessful = relate.isRelateSuccessful();
			if (isRelateSuccessful) {
				akiHelper.print(print, "Done");
			}
			
			if (optimize) {
				akiHelper.print(print, "\nOptimizing Solr Server ... ");
				akiHelper.solrOptimize();
				akiHelper.print(print, "Done");
			}
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}


	/**
	 * Starting the harvesting and index process of authority records.
	 * 
	 * @param oaiUrl					String representing an URL to an OAI interface
	 * @param format					String representing the format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param set						String representing the set of the OAI interface you want to harvest
	 * @param destinationPath			String representing a path to a local directory where the downloaded data should be stored (e. g. /home/username/oai_data)
	 * @param oaiDatefile				String representing a path to a .properties file with at least a "from" date/time in format YYYY-MM-DDTHH:MM:SSZ. It could also contain an "until" date/time. Example: /path/to/oai_date-time_file.properties
	 * @param useDefaultAuthProps		boolean indicating whether tho use the default authority properties or not
	 * @param customAuthProps			If using custom properties for authority indexing, a String representing the path to a .properties file, e. g. /home/username/my_authority.properties
	 * @param solrServerAuth			String indicating the URL incl. core name of the Solr authority index (e. g. http://localhost:8080/solr/authority)
	 * @param solrServerBiblio			String indicating the URL incl. core name of the Solr bibliographic index (e. g. http://localhost:8080/solr/biblio)
	 * @param entities					String indicating the authority entities (Persons, Corporation, Subjects, ...)
	 * @param merge						boolean indicating whether to merge the authority data into the bibliographic data or not
	 * @param print						boolean indicating whether to print status messages or not
	 * @param optimize					boolean indicating whether to optimize the solr index not
	 */
	public void oaiGndUpdate(String oaiUrl, String format, String set, String destinationPath, String oaiDatefile, boolean useDefaultAuthProps, String customAuthProps, String solrServerAuth, String solrServerBiblio, String entities, boolean merge, boolean print, boolean optimize) {

		boolean isAuthorityUpdateSuccessful = false;
		this.indexTimestamp = new Date().getTime();
		String strIndexTimestamp = String.valueOf(this.indexTimestamp);

		akiHelper.print(print, "\n-------------------------------------------");
		akiHelper.print(print, "\nOAI harvest started: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.indexTimestamp))));

		// First start of downloading and mergeing XML files from OAI interface:
		String mergedAuthFileName = oaiDownload(oaiUrl, format, set, destinationPath, "slim:record", 1, oaiDatefile, this.indexTimestamp, 0, print);

		if (mergedAuthFileName != null) {
			akiHelper.print(print, "\nIndexing new authority data ... ");
			// Index authority data from merged XML file:
			Authority auth = new Authority(
					false,
					false,
					null,
					mergedAuthFileName,
					useDefaultAuthProps,
					customAuthProps,
					solrServerAuth,
					solrServerBiblio,
					strIndexTimestamp,
					false,
					optimize
					);
			isAuthorityUpdateSuccessful = auth.indexAuthority();
			akiHelper.print(print, "Done");
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

			akiHelper.print(print, "\nDone updating from OAI interface.\nEVERYTHING WAS SUCCESSFUL!");
		} else {
			System.err.print("\nERROR WHILE UPDATING AUTHORITY DATA!");
		}

	}


	/**
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

				akiHelper.print(print, "\nHarvesting date range: " + from + " - " + until);

				// Set variables
				String oaiPathOriginal = stripFileSeperatorFromPath(destinationPath) + File.separator + "original" + File.separator + this.indexTimestamp;
				String oaiPathMerged = stripFileSeperatorFromPath(destinationPath) + File.separator + "merged" + File.separator + this.indexTimestamp;

				// Create directories if they do not exist
				mkDirIfNoExists(oaiPathOriginal);
				mkDirIfNoExists(oaiPathMerged);

				// Download updates from OAI interface and save them to a file. If there is a resumptionToken ("pages"),
				// then download all resumptions and save each to a sepearate file:
				akiHelper.print(print, "\nDownloading XML data ... ");
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
				akiHelper.print(print, "Done");

				// Write current date/time to date/time-file for next update:
				this.writeOaiDateFile(oaiDatefile, until);

				// Start downloading XML data again until we reach today:
				if (this.indexTimestamp > downloadTimestamp) {
					oaiDownload(oaiUrl, format, set, destinationPath, elementsToMerge, elementsToMergeLevel, oaiDatefile, this.indexTimestamp, counter, print);
				} else {
					// Reached today - stop downloading and start merging
					akiHelper.print(print, "\nAll OAI updates are downloaded.");

					// Start merging all downloaded updates into one file:
					akiHelper.print(print, "\nMerging downloaded XML data ... ");
					mergedFileName = oaiPathMerged + File.separator + this.indexTimestamp + ".xml";
					boolean isMergeSuccessful = mergeXmlFiles(oaiPathOriginal, mergedFileName, elementsToMerge, elementsToMergeLevel);
					if (isMergeSuccessful) {
						akiHelper.print(print, "Done");
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

	/**
	 * Getting the values from the .properties file for the date/time information for the OAI harvest date span ("from" and "until").
	 * 
	 * @param propertiesFile	String representing the path to the .properties file for the date/time information for the OAI harvest date span
	 * @return					Properties object containing all values of the .properties file
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
	 * @param oaiDatefilePath	String representing the path to the .properties file for the date/time information for the OAI harvest date span
	 * @param dateTime			The "from" date/time in format YYYY-MM-DDTHH:MM:SSZ to write to the file
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
	 * @param sourcePath			String representing a path to a directory with multiple XML files (e. g. /folder/with/multiple/xml/files)
	 * @param destinationPath		String representing a path to a file containing the merged XML data (e. g. /path/to/merged_file.xml)
	 * @param elementsToMerge		String representing the XML element that should be merged into a new file after downloading the OAI data (e. g. record).
	 * @param elementsToMergeLevel	int: Level of the XML element that should be merge if there are more of one of the same name. As default and for the top level element, use 1.
	 * @return
	 */
	private boolean mergeXmlFiles(String sourcePath, String destinationPath, String elementsToMerge, int elementsToMergeLevel) {
		XmlMerger xmlm = new XmlMerger();

		// Old merge method (using DOM parser - problem with level of element):
		//boolean isMergeSuccessful = xmlm.mergeMultipleElementNodes(sourcePath, destinationPath, "collection", "slim:record");

		// New merge method (using SAX parser):
		boolean isMergeSuccessful = xmlm.mergeElements(sourcePath, destinationPath, "collection", elementsToMerge, elementsToMergeLevel);

		return isMergeSuccessful;
	}


	/**
	 * Writing the downloaded data to a local XML file.
	 * 
	 * @param doc						Document: The DOM document representing the downloaded XML data.
	 * @param destinationDirectory		String: The path to a directory where to save the new XML file locally (e. g. /save/xml/file/here)
	 * @param fileName					String: The name of the xml file (e. g. myNewXmlFile.xml)
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
	 * @param oaiUrl				String representing an URL to an OAI interface
	 * @param metadataPrefix		Format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param set					String representing the set of the OAI interface you want to harvest
	 * @param from					String representing the "from" date/time (format YYYY-MM-DDTHH:MM:SSZ) for the date span in which to harvest changed, new or deleted records
	 * @param until					String representing the "until" date/time (format YYYY-MM-DDTHH:MM:SSZ) for the date span in which to harvest changed, new or deleted records
	 * @param resumptionToken		String representing the resumption token of the OAI response
	 * @return						Document: A DOM document representing the XML requested from the OAI interface
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
	 * @param url	URL to the resource to check.
	 * @return		The HTTP response code as an integer
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
	 * @param doc	Document: A DOM document representing the XML of the OAI interface query result
	 * @return		String representing the resumption token or null if none was found
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
	 * @param path	A string representing a path to a directory.
	 * @return		The path without the last file separator character.
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
	 * @param path	Path to the directory that should be created.
	 */
	private static void mkDirIfNoExists(String path) {
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
	}

}
