package betullam.akimporter.updater;

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
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import betullam.akimporter.main.Authority;
import betullam.akimporter.solrmab.SolrMabHelper;
import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlParser;

public class OaiUpdater {

	String timeStamp;
	SolrMabHelper smHelper = new SolrMabHelper();

	public void oaiUpdate(String oaiUrl, String format, String set, String destinationPath, String oaiDatefile, boolean useDefaultAuthProps, String customAuthProps, String solrServerAuth, String solrServerBiblio, boolean print, boolean optimize) {

		boolean isAuthorityUpdateSuccessful = false;
		this.timeStamp = String.valueOf(new Date().getTime());
		
		// Download and merge XML files from OAI interface:
		String mergedAuthFileName = oaiDownload(oaiUrl, format, set, destinationPath, oaiDatefile, print);

		if (mergedAuthFileName != null) {
			smHelper.print(print, "\nIndexing new authority data ...  ");
			// Index authority data from merged XML file:
			Authority auth = new Authority(
					mergedAuthFileName,
					useDefaultAuthProps,
					customAuthProps,
					solrServerAuth,
					solrServerBiblio,
					null,
					false,
					optimize
					);
			isAuthorityUpdateSuccessful = auth.indexAuthority();
			smHelper.print(print, "Done");
		}
		
		if (isAuthorityUpdateSuccessful) {
			smHelper.print(print, "\nDONE UPDATING AUTHORITY DATA. EVERYTHING WAS SUCCESSFUL");
		} else {
			System.err.println("\nERROR WHILE UPDATING AUTHORITY DATA!");
		}
		
	}

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
						String fileName = this.timeStamp + "_" + counter + ".xml";
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


	private void writeOaiDateFile(String oaiDatefilePath, String dateTime) {
		File oaiDateFile = new File(oaiDatefilePath);
		String content = "from: " + dateTime;
		try {
			FileUtils.writeStringToFile(oaiDateFile, content);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}


	private boolean mergeXmlFiles(String sourcePath, String destinationPath) {
		XmlMerger xmlm = new XmlMerger();
		boolean isMergeSuccessful = xmlm.mergeMultipleElementNodes(sourcePath, destinationPath, "collection", "slim:record");
		return isMergeSuccessful;
	}


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
}
