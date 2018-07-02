package main.java.betullam.akimporter.solrmab.indexing;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import ak.xmlhelper.XmlParser;
import main.java.betullam.akimporter.rules.PropertyBag;
import main.java.betullam.akimporter.rules.Rules;

public class XmlContentHandler implements ContentHandler {

	private HttpSolrServer solrServer;
	private String timeStamp;
	//private boolean print;
	private String recordName;
	private String elementContent;
	private String xmlRecord;
	private boolean isRecord = false;
	private List<Map<String, List<String>>> xmlSolrRecords = null;
	List<String> includes;
	List<String> excludes;
	private List<PropertyBag> propertyBags;
	private int recordCounter = 0;
	private int NO_OF_DOCS = 500;


	public XmlContentHandler(HttpSolrServer solrServer, String recordName, List<String> includes, List<String> excludes, String oaiPropertiesFile, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.recordName = recordName;
		this.timeStamp = timeStamp;
		//this.print = print;
		this.includes = includes;
		this.excludes = excludes;
		this.propertyBags = Rules.getPropertyBags(oaiPropertiesFile);
		Rules.setOaiPropertiesFilePath(new File(oaiPropertiesFile).getParent());
	}


	@Override
	public void startDocument() throws SAXException {
		xmlSolrRecords = new ArrayList<Map<String, List<String>>>();
	}


	@Override
	public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
		// Clear element content for fresh start
		elementContent = "";
		String startElement = "";
		
		if (qName.equals(recordName)) {
			// Start fresh record
			isRecord = true;
			xmlRecord = "";
			xmlRecord += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
			
		}

		if (isRecord) {
			startElement += "<" + qName;			
			
			for (int i = 0; i < atts.getLength(); i++) {
				String attQName = atts.getQName(i);
				String attValue = atts.getValue(i);
				
				/*
				// Add the xsi namespace if appropriate:
				// Not necessary as we set namespace awareness for SAX parser (see class "OaiUpdater")
				if (attQName.contains("xsi:")) {
					startElement += " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
				}
				*/
				
				startElement += " " + attQName + "=\"" + StringEscapeUtils.escapeXml10(attValue).trim() + "\"";
			}
			startElement += ">";
			xmlRecord += startElement;
		}
	}


	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		elementContent = Normalizer.normalize(elementContent, Form.NFC);
		
		if (isRecord) {
			String endElement = "";
			endElement += StringEscapeUtils.escapeXml10(elementContent).trim();
			endElement += "</" + qName + ">";
			xmlRecord += endElement;
		}

		if (qName.equals(recordName)) {
			// End of record
			recordCounter = recordCounter + 1;
			isRecord = false;

			Map<String, List<String>> xmlSolrRecord = getXmlSolrRecord(xmlRecord);
			if (xmlSolrRecord != null) {
				xmlSolrRecords.add(xmlSolrRecord);
				xmlSolrRecord = null;
			}

			// This generates too much output when using cron jobs or nohup:
			//AkImporterHelper.print(print, "Indexing record no. " + recordCounter + "                                                        \r");

			// Every n-th record (= NO_OF_DOCS), add the generic XML records to Solr. Then we will empty all objects (set to "null") to save memory
			// and go on with the next n records. If there is a rest at the end of the file, do the same thing in the endDocument() method. E. g. NO_OF_DOCS 
			// is set to 100 and we have 733 records, but at this point, only 700 are indexed. The 33 remaining records will be indexed in endDocument() method.
			if (recordCounter % NO_OF_DOCS == 0) {
				addRecordsToSolr(this.solrServer, this.xmlSolrRecords);
				xmlSolrRecords = null;
				xmlSolrRecords = new ArrayList<Map<String, List<String>>>();
			}
		}

		// Clear element content for fresh start
		elementContent = "";
	}


	@Override
	public void endDocument() throws SAXException {
		addRecordsToSolr(this.solrServer, this.xmlSolrRecords);
		xmlSolrRecords = null;
		propertyBags = null;
	}


	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		elementContent += new String(ch, start, length).replaceAll("\\s+", " ");
	}


	/**
	 * Actually adds our data to the Solr server.
	 * @param solrServer		HttpSolrServer: The Solr server to which the data should be added
	 * @param xmlSolrRecords	List&lt;Map&lt;String, List&lt;String&gt;&gt;&gt;: A list of Maps, each representing a record that should be added to solr.
	 */
	private void addRecordsToSolr(HttpSolrServer solrServer, List<Map<String, List<String>>> xmlSolrRecords) {
		// Create a collection of all documents
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		for (Map<String, List<String>> xmlSolrRecord : xmlSolrRecords) {
			// Create a Solr input document
			SolrInputDocument doc = new SolrInputDocument();

			// Add fields to Solr document
			for (Entry<String, List<String>> dataField : xmlSolrRecord.entrySet()) {
				String solrFieldName = dataField.getKey();
				List<String> solrFieldValue = dataField.getValue();
				if (solrFieldValue != null && !solrFieldValue.isEmpty()) {
					doc.addField(solrFieldName, solrFieldValue);
				}
			}

			// Add the Solr document to a Solr document collection if it is not empty
			if (!doc.isEmpty()) {
				docs.add(doc);
			}
		}

		// If the Solr document collection is not empty, add the Solr documents to Solr
		if (!docs.isEmpty()) {
			// Now add the collection of documents to Solr:
			try {
				solrServer.add(docs);
			} catch (SolrServerException e) {
				System.err.println("Error in XML content handler.");
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Error in XML content handler.");
				e.printStackTrace();
			}

			// Set "docs" to "null" (save memory):
			docs = null;
		}
	}


	/**
	 * Converts a DOM document (from an XML string) with the help of xPath to a Map that can be added to Solr.
	 * @param xmlRecord		String: The XML record as a String. It will be converted to a DOM document that can be queried with xPath.
	 * @return				Map&lt;String, List&lt;String&gt;&gt;: A Map that is used to add the data from the XML to Solr.
	 * 						The key (String) is the Solr fieldname, the value (List&lt;String&gt;) are the values that should be indexed into this field.
	 */
	private Map<String, List<String>> getXmlSolrRecord(String xmlRecord) {
		Map<String, List<String>> xmlSolrRecord = new TreeMap<String, List<String>>();
		Document document = getDomDocument(xmlRecord);
		Rules.setDocument(document);
		boolean isInclude = false;

		XmlParser xmlParser = new XmlParser();

		// Check for includes and excludes (filtes to define in AkImporter.properties, if the document should be indexed or not)
		try {
			if (includes != null) {
				for (String includeXpath : includes) {
					List<String> includeValues = xmlParser.getXpathResult(document, includeXpath, false);
					if (includeValues != null && includeValues.size() > 0) {
						isInclude = true;
					}
				}
			} else {
				// If there is no include rule at all, set isInclude to true by default
				isInclude = true;
			}

			if (excludes != null) {
				for (String excludeXpath : excludes) {
					List<String> excludeValues = xmlParser.getXpathResult(document, excludeXpath, true);
					if (excludeValues != null && excludeValues.size() > 0) {
						isInclude = false;
					}
				}
			}
		} catch (XPathExpressionException e) {
			System.err.println("Error in XML content handler.");
			e.printStackTrace();
		}

		// Do not index the record
		if (!isInclude) {
			return null;
		}

		for (PropertyBag propertyBag : propertyBags) {
			String solrField = propertyBag.getSolrField();
			List<String> dataFields = propertyBag.getDataFields();
			List<String> dataRules = propertyBag.getDataRules();
			List<String> dataFieldValues = new ArrayList<String>();
			List<String> treatedValues = new ArrayList<String>();

			for (String dataField : dataFields) {
				if (dataRules.contains("customText") || dataRules.contains("getAllFields") || dataRules.contains("getFullRecordAsXML")) {
					// It's not an xPath but a rule with other funcionalities (e. g. customText)
					dataFieldValues.add(dataField);
				} else {
					// It should be an xPath expression, so we try to execute it and get a result from it
					try {
						List<String> values = xmlParser.getXpathResult(document, dataField, false);
						if (values != null && !values.isEmpty()) {
							dataFieldValues.addAll(values);
						}
					} catch (XPathExpressionException e) {
						System.err.println("Error in XML content handler.");
						e.printStackTrace();
					}
				}
			}

			if (dataFieldValues != null && !dataFieldValues.isEmpty()) {
				// Apply rules specified in .properties file
				/*if (solrField.equals("author")) {
					System.out.println(dataFieldValues + ": " + dataRules);
				}*/
				treatedValues = Rules.applyDataRules(dataFieldValues, dataRules);

				// Add the Solr field name and the values for the Solr field to a Map
				xmlSolrRecord.put(solrField, treatedValues);
			}
		}

		// Add indexing timestamp to record
		if (!xmlSolrRecord.isEmpty()) {
			List<String> timeStampAsList = new ArrayList<String>();
			timeStampAsList.add(timeStamp);
			xmlSolrRecord.put("indexTimestamp_str", timeStampAsList);
		}

		return xmlSolrRecord;
	}


	/**
	 * Get a DOM Document from a String representing an XML record.
	 * @param xmlRecord		String: The XML record as a String. It will be converted to a DOM document.
	 * @return				Document: A DOM Document
	 */
	private static Document getDomDocument(String xmlRecord) {
		Document domDocument = null;
		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			documentBuilderFactory.setNamespaceAware(true); // Set namespace awareness for DOM builder factory. Important if we want to use namspaces in xPath!
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			InputSource inputSource = new InputSource(new StringReader(xmlRecord));
			domDocument = documentBuilder.parse(inputSource);
		} catch (ParserConfigurationException e) {
			System.err.println("Error in XML content handler.");
			e.printStackTrace();
		} catch (SAXException e) {
			System.err.println("Error in XML content handler.");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error in XML content handler.");
			e.printStackTrace();
		}

		return domDocument;
	}

	

	// Methods from ContentHandler that are not used at the moment:
	@Override
	public void startPrefixMapping(String prefix, String uri) throws SAXException {}
	@Override
	public void endPrefixMapping(String prefix) throws SAXException {}
	@Override
	public void setDocumentLocator(Locator locator) {}
	@Override
	public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}
	@Override
	public void processingInstruction(String target, String data) throws SAXException {}
	@Override
	public void skippedEntity(String name) throws SAXException {}
}