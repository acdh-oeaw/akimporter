package main.java.betullam.akimporter.solrmab.indexing;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
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
import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.rules.PropertyBag;
import main.java.betullam.akimporter.rules.Rules;

public class XmlContentHandler implements ContentHandler {

	private HttpSolrServer solrServer;
	private String timeStamp;
	private boolean print;
	private String recordName;
	private String elementContent;
	private String xmlRecord;
	private boolean isRecord = false;
	private List<String> xmlRecords;
	private List<Map<String, List<String>>> xmlSolrRecords = null;
	private List<PropertyBag> propertyBags;

	
	public XmlContentHandler(HttpSolrServer solrServer, String recordName, String oaiPropertiesFile, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.recordName = recordName;
		this.timeStamp = timeStamp;
		this.print = print;
		this.propertyBags = Rules.getPropertyBags(oaiPropertiesFile);
		Rules.setOaiPropertiesFilePath(new File(oaiPropertiesFile).getParent());
	}

	
	@Override
	public void startDocument() throws SAXException {
		xmlRecords = new ArrayList<String>();
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
				// Add the xsi namespace if appropriate:
				if (attQName.contains("xsi:")) {
					startElement += " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
				}
				startElement += " " + attQName + "=\"" + StringEscapeUtils.escapeXml10(attValue).trim() + "\"";
			}
			startElement += ">";
			xmlRecord += startElement;
		}
	}


	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {

		if (isRecord) {
			String endElement = "";
			endElement += StringEscapeUtils.escapeXml10(elementContent).trim();
			endElement += "</" + qName + ">";
			xmlRecord += endElement;
		}

		if (qName.equals(recordName)) {
			// End of record
			isRecord = false;
			xmlRecords.add(xmlRecord);

			Map<String, List<String>> xmlSolrRecord = getXmlSolrRecord(xmlRecord);
			xmlSolrRecords.add(xmlSolrRecord);
		}

		// Clear element content for fresh start
		elementContent = "";
		
		// TODO: Every n-th, execute "addRecordsToSolr(this.solrServer, this.xmlSolrRecords);" and set variables to null.
		// If not: HEAP SPACE PROBLEM!
	}


	@Override
	public void endDocument() throws SAXException {
		AkImporterHelper.print(print, "\nIndexing documents to Solr ... ");
		addRecordsToSolr(this.solrServer, this.xmlSolrRecords);
		AkImporterHelper.print(print, "Done");
		this.xmlSolrRecords = null;
	}


	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		elementContent += new String(ch, start, length).replaceAll("\\s+", " ");
	}

	
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
				//solrServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// Set "docs" to "null" (save memory):
			docs = null;
		}
	}


	private Map<String, List<String>> getXmlSolrRecord(String xmlRecord) {
		Map<String, List<String>> xmlSolrRecord = new TreeMap<String, List<String>>();
		Document document = getDomDocument(xmlRecord);
		Rules.setDocument(document);

		XmlParser xmlParser = new XmlParser();

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
						List<String> values = xmlParser.getXpathResult(document, dataField);
						if (values != null) {
							dataFieldValues.addAll(values);
						}
					} catch (XPathExpressionException e) {
						e.printStackTrace();
					}
				}
			}

			treatedValues = Rules.applyDataRules(solrField, dataFieldValues, dataRules);
			xmlSolrRecord.put(solrField, treatedValues);
		}
		
		// Add indexing timestamp to record
		if (!xmlSolrRecord.isEmpty()) {
			List<String> timeStampAsList = new ArrayList<String>();
			timeStampAsList.add(timeStamp);
			xmlSolrRecord.put("indexTimestamp_str", timeStampAsList);
		}
		
		return xmlSolrRecord;
	}


	private static Document getDomDocument(String xmlRecord) {
		Document domDocument = null;

		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			InputSource inputSource = new InputSource(new StringReader(xmlRecord));
			domDocument = documentBuilder.parse(inputSource);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
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