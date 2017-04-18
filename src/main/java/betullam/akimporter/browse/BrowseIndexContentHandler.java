package main.java.betullam.akimporter.browse;

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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import ak.xmlhelper.XmlParser;
import main.java.betullam.akimporter.main.AkImporterHelper;

public class BrowseIndexContentHandler implements ContentHandler {

	private HttpSolrServer solrServer;
	private String timeStamp;
	private boolean print;
	private String recordToIndex;
	private String biIdXpath;
	private String elementContent;
	private String xmlRecord;
	private boolean isRecord = false;
	private List<Map<String, List<String>>> xmlSolrRecords = null;
	private int recordCounter = 0;
	private int NO_OF_DOCS = 500;


	public BrowseIndexContentHandler(HttpSolrServer solrServer, String recordToIndex, String biIdXpath, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.recordToIndex = recordToIndex;
		this.biIdXpath = biIdXpath;
		this.timeStamp = timeStamp;
		this.print = print;
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

		if (qName.equals(recordToIndex)) {
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

		if (qName.equals(recordToIndex)) {
			// End of record
			recordCounter = recordCounter + 1;
			isRecord = false;

			Map<String, List<String>> xmlSolrRecord = getXmlSolrRecord(xmlRecord);
			if (xmlSolrRecord != null) {
				xmlSolrRecords.add(xmlSolrRecord);
				xmlSolrRecord = null;
			}

			// Every n-th record (= NO_OF_DOCS), add the generic XML records to Solr. Then we will empty all objects (set to "null") to save memory
			// and go on with the next n records. If there is a rest at the end of the file, do the same thing in the endDocument() method. E. g. NO_OF_DOCS 
			// is set to 100 and we have 733 records, but at this point, only 700 are indexed. The 33 remaining records will be indexed in endDocument() method.
			if (recordCounter % NO_OF_DOCS == 0) {
				//addRecordsToSolr(solrServer, xmlSolrRecords);
				xmlSolrRecords = null;
				xmlSolrRecords = new ArrayList<Map<String, List<String>>>();
			}
		}

		// Clear element content for fresh start
		elementContent = "";
	}

	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		elementContent += new String(ch, start, length).replaceAll("\\s+", " ");
	}

	@Override
	public void endDocument() throws SAXException {
		//addRecordsToSolr(solrServer, xmlSolrRecords);
		xmlSolrRecords = null;
	}
	
	
	/**
	 * Converts an XML record with the help of xPath to a Map that can be added to Solr.
	 * @param xmlRecord		String: The XML record as a String. It will be converted to a DOM document that can be queried with xPath.
	 * @return				Map&lt;String, List&lt;String&gt;&gt;: A Map that is used to add the data from the XML to Solr.
	 * 						The key (String) is the Solr fieldname, the value (List&lt;String&gt;) are the values that should be indexed into this field.
	 */
	private Map<String, List<String>> getXmlSolrRecord(String xmlRecord) {
		Map<String, List<String>> xmlSolrRecord = new TreeMap<String, List<String>>();
		Document document = getDomDocument(xmlRecord);
		XmlParser xmlParser = new XmlParser();
		String recordId = null;
		
		// Get record ID
		try {
			List<String> recordIds = xmlParser.getXpathResult(document, biIdXpath, false);
			recordId = (recordIds != null) ? recordIds.get(0) : null;
			List<String> singleRecordId = new ArrayList<String>();
			singleRecordId.add(recordId);
			xmlSolrRecord.put("id", singleRecordId);
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		
		// Add all fields of the DOM document
		getAllFields(document.getDocumentElement());
		
		return xmlSolrRecord;
	}
	
	
	private void getAllFields(Node node) {
		
		String fieldName = null;
		
	    if (node.getNodeType() == Node.ELEMENT_NODE) {
	    	
	    	
	    	String nodeName = node.getNodeName();
		    AkImporterHelper.print(print, "\n" + nodeName);

	    	NamedNodeMap nnM = node.getAttributes();
		    if (nnM != null && nnM.getLength() > 0) {
		    	
		    	String tag = null;
		    	String ind1 = null;
		    	String ind2 = null;
		    	String code = null;
		    	
		    	for(int a = 0; a < nnM.getLength(); a++) {
			    	Node attributeNode = nnM.item(a);
			    	if (attributeNode.getNodeType() == Node.ATTRIBUTE_NODE) {
			    		String attrName = attributeNode.getNodeName();
			    		String attrValue = attributeNode.getNodeValue();
			    		if (nodeName.equals("datafield")) {
				    		tag = (attrName.equals("tag")) ? attrValue : null;
				    		ind1 = (attrName.equals("ind1")) ? attrValue : null;
				    		ind2 = (attrName.equals("ind2")) ? attrValue : null;
					    }
			    		if (nodeName.equals("subfield")) {
			    			code = (attrName.equals("code")) ? attrValue : null;
			    		}
			    		if (nodeName.equals("controlfield")) {
			    			tag = (attrName.equals("tag")) ? attrValue : null;
			    		}
			    	}
			    }
		    	
		    	if (nodeName.equals("datafield")) {
			    	fieldName = tag + "$" + ind1 + ind2;
			    }
	    		if (nodeName.equals("subfield")) {
	    			fieldName = tag + "$" + ind1 + ind2 + "$" + code;
	    		}
	    		if (nodeName.equals("controlfield")) {
	    			fieldName = tag;
	    		}
		    } 
	    }
	    
	    
	    if (node.getNodeType() == Node.TEXT_NODE) {
		    AkImporterHelper.print(print, "\n" + fieldName + ": " + node.getTextContent());
		    AkImporterHelper.print(print, "\n----------------------------------");
	    }
	    

	    // Repeat for child nodes:
	    NodeList nodeList = node.getChildNodes();
	    for (int i = 0; i < nodeList.getLength(); i++) {
	        Node currentNode = nodeList.item(i);
	        if (currentNode.getNodeType() == Node.ELEMENT_NODE || currentNode.getNodeType() == Node.TEXT_NODE) {
	        	getAllFields(currentNode);
	        }
	    }
	}
	
	/**
	 * Get a DOM Document from a String representing an XML record.
	 * @param xmlRecord		String: The XML record as a String. It will be converted to a DOM document.
	 * @return				Document: A DOM Document
	 */
	private Document getDomDocument(String xmlRecord) {
		Document domDocument = null;
		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			documentBuilderFactory.setNamespaceAware(true); // Set namespace awareness for DOM builder factory. Important if we want to use namspaces in xPath!
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
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// Set "docs" to "null" (save memory):
			docs = null;
		}
	}



	// Unused methods from parent class
	@Override
	public void setDocumentLocator(Locator locator) {}
	@Override
	public void startPrefixMapping(String prefix, String uri) throws SAXException {}
	@Override
	public void endPrefixMapping(String prefix) throws SAXException {}
	@Override
	public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}
	@Override
	public void processingInstruction(String target, String data) throws SAXException {}
	@Override
	public void skippedEntity(String name) throws SAXException {}
}