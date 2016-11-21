package main.java.betullam.akimporter.solrmab.indexing;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
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
	private boolean print;
	private String recordName;
	private String elementContent;
	private String xmlRecord;
	private boolean isRecord = false;
	private List<String> xmlRecords;
	private List<XmlSolrRecord> xmlSolrRecords = null;
	//private Rules rules;
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
		xmlSolrRecords = new ArrayList<XmlSolrRecord>();
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

			xmlSolrRecords.add(getXmlSolrRecord(xmlRecord));

		}

		// Clear element content for fresh start
		elementContent = "";
	}


	@Override
	public void endDocument() throws SAXException {
		//System.out.println(xmlRecords);
	}


	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		elementContent += new String(ch, start, length).replaceAll("\\s+", " ");
	}



	private XmlSolrRecord getXmlSolrRecord(String xmlRecord) {
		XmlSolrRecord xmlSolrRecord = new XmlSolrRecord();
		Document document = getDomDocument(xmlRecord);
		Rules.setDocument(document);

		XmlParser xmlParser = new XmlParser();

		//String id = xmlParser.getTextValue(document, "/record/header/identifier");
		//List<String> id = xmlParser.getXpathResult(document, "/record/header//setSpec");
		//List<String> id = xmlParser.getXpathResult(document, "/record/metadata//dcvalue[@element='date']");
		//List<String> id = xmlParser.getXpathResult(document, "//dcvalue[@element='contributor' and @qualifier='author'][position()>2]");

		for (PropertyBag propertyBag : propertyBags) {
			String solrField = propertyBag.getSolrField();
			List<String> dataFields = propertyBag.getDataFields();
			List<String> dataRules = propertyBag.getDataRules();

			for (String dataField : dataFields) {
				List<String> treatedValue = null;
				if (dataRules.contains("customText")) {
					// It's not an xPath but a custom text
					treatedValue = Rules.applyDataRules(dataField, dataRules);
				} else {
					// It should be an xPath
					try {
						List<String> values = xmlParser.getXpathResult(document, dataField);
						if (values != null) {
							for (String value : values) {
								treatedValue = Rules.applyDataRules(value, dataRules);
								//System.out.println("treatedValue: " + treatedValue);
								//String treatedValue = Rules.applyRules(value, rules);
								//System.out.println(solrField + ": " + treatedValue);
							}
						}
					} catch (XPathExpressionException e) {
						e.printStackTrace();
					}	
				}

				if (treatedValue != null) {
					System.out.println("treatedValue: " + treatedValue);
				}

			}

			//System.out.println(solrField + ": " + values);
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