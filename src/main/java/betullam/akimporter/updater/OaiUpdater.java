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
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.xml.sax.SAXException;

import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlParser;
import main.java.betullam.akimporter.main.Authority;
import main.java.betullam.akimporter.solrmab.SolrMabHelper;
import main.java.betullam.akimporter.solrmab.relations.AuthorityFlag;
import main.java.betullam.akimporter.solrmab.relations.AuthorityMerge;


public class OaiUpdater {

	String timeStamp;
	SolrMabHelper smHelper = new SolrMabHelper();

	/**
	 * Starting the harvesting and index process of authority records.
	 * 
	 * @param oaiUrl					String representing an URL to an OAI interface
	 * @param format					Format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param set						String representing the set of the OAI interface you want to harvest
	 * @param destinationPath			String representing a path to a local directory where the downloaded data should be stored (e. g. /home/username/oai_data)
	 * @param oaiDatefile				String representing a path to a .properties file with at least a "from" date/time in format YYYY-MM-DDTHH:MM:SSZ. It could also contain an "until" date/time. Example: /path/to/oai_date-time_file.properties
	 * @param useDefaultAuthProps		boolean indicating whether tho use the default authority properties or not
	 * @param customAuthProps			If using custom properties for authority indexing, a String representing the path to a .properties file, e. g. /home/username/my_authority.properties
	 * @param solrServerAuth			String indicating the URL incl. core name of the Solr authority index (e. g. http://localhost:8080/solr/authority)
	 * @param solrServerBiblio			String indicating the URL incl. core name of the Solr bibliographic index (e. g. http://localhost:8080/solr/biblio)
	 * @param print						boolean indicating whether to print status messages or not
	 * @param optimize					boolean indicating whether to optimize the solr index not
	 */
	public void oaiUpdate(String oaiUrl, String format, String set, String destinationPath, String oaiDatefile, boolean useDefaultAuthProps, String customAuthProps, String solrServerAuth, String solrServerBiblio, String entities, boolean merge, boolean print, boolean optimize) {

		boolean isAuthorityUpdateSuccessful = false;
		this.timeStamp = String.valueOf(new Date().getTime());

		// Download and merge XML files from OAI interface:
		String mergedAuthFileName = oaiDownload(oaiUrl, format, set, destinationPath, oaiDatefile, print);

		if (mergedAuthFileName != null) {
			smHelper.print(print, "\nIndexing new authority data ...  ");
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
					this.timeStamp,
					false,
					optimize
					);
			isAuthorityUpdateSuccessful = auth.indexAuthority();
			smHelper.print(print, "Done");
		}

		if (isAuthorityUpdateSuccessful) {

			// TODO: Merge authority data to bibliographic data:
			// 1. Get all IDs of bibliographic records using the authority IDs that were just updated
			// 2. Start AuthorityFlag and AuthorityMerge for these bibliographic records
			// TEST merge - Begin
			if (merge) {
				//Set<String> currentyIndexedAuthRecordIds = getCurrentlyIndexedAuthRecordIds(solrServerAuth, this.timeStamp);
				//SolrDocumentList biblioRecordsForFlagAndMerge = getBiblioRecordsWithGnd(solrServerBiblio, currentyIndexedAuthRecordIds);
				HttpSolrServer sServerAuth = new HttpSolrServer(solrServerAuth);
				HttpSolrServer sServerBiblio =  new HttpSolrServer(solrServerBiblio);
				
				// Set flag of existance to authority records:
				AuthorityFlag af = new AuthorityFlag(sServerBiblio, sServerAuth, this.timeStamp, true, print);
				af.setFlagOfExistance();
				
				// Merge authority records to bibliographic records:
				AuthorityMerge am = new AuthorityMerge(sServerBiblio, sServerAuth, this.timeStamp, true, print);
				am.mergeAuthorityToBiblio(entities);
				
			}
			// TEST merge - End

			smHelper.print(print, "\nDONE UPDATING AUTHORITY DATA. EVERYTHING WAS SUCCESSFUL");
		} else {
			System.err.println("\nERROR WHILE UPDATING AUTHORITY DATA!");
		}

	}


	/**
	 * Downloads, saves and merges data from an OAI interface.
	 * 
	 * @param oaiUrl				String representing an URL to an OAI interface
	 * @param format				Format (metadataPrefix) of the data the OAI interface should issue (e. g. oai_dc, MARC21-xml, ...)
	 * @param set					String representing the set of the OAI interface you want to harvest
	 * @param destinationPath		String representing a path to a local directory where the downloaded data should be stored (e. g. /home/username/oai_data)
	 * @param oaiDatefile			String representing a path to a .properties file with at least a "from" date/time in format YYYY-MM-DDTHH:MM:SSZ. It could also contain an "until" date/time. Example: /path/to/oai_date-time_file.properties
	 * @param print					boolean indicating whether to print status messages or not
	 * @return						String representing the path to a file with the downloaded data
	 */
	private String oaiDownload(String oaiUrl, String format, String set, String destinationPath, String oaiDatefile, boolean print) {

		String mergedFileName = null;

		String currentDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date(Long.valueOf(this.timeStamp)));
		Properties oaiDateTime = getOaiDateTime(oaiDatefile);

		String from = oaiDateTime.getProperty("from");
		String until = oaiDateTime.getProperty("until", currentDateTime);

		try {
			int httpResponseCode = getHttpResponseCode(new URL(oaiUrl+"?verb=ListRecords&metadataPrefix="+format+"&set="+set+"&from="+from+"&until="+until));

			if (httpResponseCode == 200) {

				smHelper.print(print, "\n-------------------------------------------");
				smHelper.print(print, "\nOAI harvest starting: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.timeStamp))));
				smHelper.print(print, "\nHarvesting date range: " + from + " - " + until);

				// Set variables
				String oaiPathOriginal = stripFileSeperatorFromPath(destinationPath) + File.separator + "original" + File.separator + this.timeStamp;
				String oaiPathMerged = stripFileSeperatorFromPath(destinationPath) + File.separator + "merged" + File.separator + this.timeStamp;

				// Create directories if they do not exist
				mkDirIfNoExists(oaiPathOriginal);
				mkDirIfNoExists(oaiPathMerged);

				// Download updates from OAI interface and save them to a file. If there is a resumptionToken ("pages"),
				// then download all resumptions and save each to a sepearate file:
				smHelper.print(print, "\nDownloading XML data ...  ");
				String resumptionToken = null;
				int counter = 0;
				do {
					counter++;
					Document doc = getOaiUpdated(oaiUrl, format, set, from, until, resumptionToken);
					if (doc != null) {
						resumptionToken = getResumptionToken(doc);
						String fileName = this.timeStamp + "_" + String.format("%06d", counter) + ".xml";
						writeXmlToFile(doc, oaiPathOriginal, fileName);
					}
				} while (resumptionToken != null);
				smHelper.print(print, "Done");

				smHelper.print(print, "\nMerging downloaded XML data ...  ");

				// Merge all downloaded updates into one file:
				mergedFileName = oaiPathMerged + File.separator + this.timeStamp + ".xml";
				boolean isMergeSuccessful = mergeXmlFiles(oaiPathOriginal, mergedFileName);
				if (isMergeSuccessful) {
					smHelper.print(print, "Done");
					// Write current date/time to date/time-file for next update:
					this.writeOaiDateFile(oaiDatefile, until);
				} else {
					System.err.println("\nMerging downloaded files from OAI was not successful!");
					mergedFileName = null;
				}
			} else if (httpResponseCode == 413) {
				System.err.println("\n-------------------------------------------");
				System.err.println("\nToo many documents requested from OAI interface (> 100000) . Please specify \"until\" date for OAI harvesting.");
				mergedFileName = null;
			} else {
				System.err.println("\nError! Getting HTTP response code " + httpResponseCode + " from OAI interface at " + oaiUrl);
				mergedFileName = null;
			}
		} catch (MalformedURLException e) {
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
			System.err.println("Properties file not found. Please make sure the file " + propertiesFile + " exists and is valid.");
			oaiDateTimeProperties = null;
			System.exit(1); // Stop execution of program
		} catch (IOException e) {
			e.printStackTrace();
			oaiDateTimeProperties = null;
			System.exit(1); // Stop execution of program
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
		}		
	}


	/**
	 * Merge multiple XML files into one.
	 * 
	 * @param sourcePath			String representing a path to a directory with multiple XML files (e. g. /folder/with/multiple/xml/files)
	 * @param destinationPath		String representing a path to a file containing the merged XML data (e. g. /path/to/merged_file.xml)
	 * @return
	 */
	private boolean mergeXmlFiles(String sourcePath, String destinationPath) {
		XmlMerger xmlm = new XmlMerger();
		boolean isMergeSuccessful = xmlm.mergeMultipleElementNodes(sourcePath, destinationPath, "collection", "slim:record");
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

	
	/*
	private static Set<String> getCurrentlyIndexedAuthRecordIds(String solrServerAuthAddr, String timeStamp) {

		// Set up variables
		Set<String> distinctAuthIds = new HashSet<String>();
		SolrDocumentList currentlyIndexedAuthRecords = null;
		SolrQuery query = new SolrQuery();
		HttpSolrServer solrServerAuth = new HttpSolrServer(solrServerAuthAddr);

		// Set no of rows
		// The query should not give back a lot of records, so we use Integer.MAX_VALUE. But in the future we should use a better strategy.
		// TODO: Change to deep pageing query in case we get back too much documents (performance).
		query.setRows(Integer.MAX_VALUE);

		// Add sorting
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Filter all records that were indexed with the current import process (timeStamp)
		query.setFilterQueries("indexTimestamp_str:"+timeStamp, "id:*");

		// Set fields that should be given back from the query
		query.setFields("id", "gndId035_str_mv");

		try {
			// Execute query and get results
			currentlyIndexedAuthRecords = solrServerAuth.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		if (!currentlyIndexedAuthRecords.isEmpty() && currentlyIndexedAuthRecords != null) {
			for (SolrDocument authRecord : currentlyIndexedAuthRecords) {
				// Get all IDs of the authority document
				String authId = authRecord.getFieldValue("id").toString();
				Collection<Object> gndIds035 = (authRecord.getFieldValues("gndId035_str_mv") != null && !authRecord.getFieldValues("gndId035_str_mv").isEmpty()) ? authRecord.getFieldValues("gndId035_str_mv") : null;

				// Add authority IDs to a Set<String> to get a deduplicated list of authority IDs 
				distinctAuthIds.add(authId);
				if (gndIds035 != null) {
					for (Object gndId035 : gndIds035) {
						distinctAuthIds.add(gndId035.toString());
					}
				}
			}
		}

		return distinctAuthIds;
	}


	private SolrDocumentList getBiblioRecordsWithGnd(String solrServerBiblioAddr, Set<String> authIds) {
		
		// Set variables
		HttpSolrServer solrServerBiblio = new HttpSolrServer(solrServerBiblioAddr);
		SolrDocumentList biblioRecords = new SolrDocumentList();
		SolrQuery query = new SolrQuery();

		// Set no of rows
		// The query should not give back a lot of records, so we use Integer.MAX_VALUE. But in the future we should use a better strategy.
		// TODO: Change to deep pageing query in case we get back too much documents (performance).
		query.setRows(Integer.MAX_VALUE);

		// Add sorting
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance (filter query users filter cache)
		query.setQuery("*:*");

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_txt", "author_GndNo_str", "author2_GndNo_str", "author_additional_GndNo_str_mv", "corporateAuthorGndNo_str", "corporateAuthor2GndNo_str_mv", "subjectGndNo_str");

		for (String authId : authIds) {
			query.setFilterQueries("author_GndNo_str:\""+authId+"\" || author2_GndNo_str:\""+authId+"\" || author_additional_GndNo_str_mv:\""+authId+"\" || corporateAuthorGndNo_str:\""+authId+"\" || corporateAuthor2GndNo_str_mv:\""+authId+"\" || subjectGndNo_str:\""+authId+"\"", "id:*");	
			try {
				// Execute query and get results
				SolrDocumentList queryResult = solrServerBiblio.query(query).getResults();
				if (queryResult != null && !queryResult.isEmpty()) {
					biblioRecords.addAll(queryResult);
				}
			} catch (SolrServerException e) {
				e.printStackTrace();
			}
		}

		return biblioRecords;
	}
	*/
}
