/**
 * Parses the contents of the MarcXML files. The contens are handed over to
 * a class that applies the rules defined in mab.properties (could also be
 * called differently, but it must be a .properties file). There, the records
 * are processed and given back. Then they will be indexed to Solr.
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
package main.java.betullam.akimporter.solrmab.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;


public class MarcContentHandler implements ContentHandler {

	private RawRecord rawRecord;
	private ArrayList<RawRecord> rawRecords;
	private Leader leader;
	private Controlfield controlfield;
	private ArrayList<Controlfield> controlfields;
	private Datafield datafield;
	private ArrayList<Datafield> datafields;
	private Subfield subfield;
	private ArrayList<Subfield> subfields;
	private String nodeContent;
	private List<PropertiesObject> propertiesObjects;
	private SolrServer sServer;
	private String recordID;
	private String recordSYS;
	private boolean is001Controlfield;
	private boolean is001Datafield;
	private boolean isSYS;
	private boolean print = true;
	private int counter = 0;
	private String timeStamp;
	private int NO_OF_DOCS = 500;
	
	// Variables for allfields:
	private boolean hasAllFieldsField = false;
	private String allfieldsField = null;
	private List<String> allFieldsExceptions = new ArrayList<String>();

	// Variables for fullrecord
	private boolean getFullRecordAsXML = false;
	private String fullrecordField = null;
	private String fullrecordXmlString = null;
	
	// Variables for enrichment
	private boolean enrich = false;


	/**
	 * Constructor of MarcContentHandler.
	 * This is the starting point of reading and processing the XML file(s) containing MARC records.
	 * 
	 * @param propertiesObjects		List<PropertiesObject>. A PropertiesObject contains information about matching raw MarcXML fields to Solr fields.
	 * @param solrServer			SolrServer object that represents the Solr server to which the data should be indexed 
	 * @param timeStamp				String that specifies the starting time of the importing process
	 * @param print					boolean. True if status messages should be printed to the console.
	 */
	public MarcContentHandler(List<PropertiesObject> propertiesObjects, SolrServer solrServer, boolean enrich, String timeStamp, boolean print) {
		this.propertiesObjects = propertiesObjects;
		this.sServer = solrServer;
		this.enrich = enrich;
		this.timeStamp = timeStamp;
		this.print = print;

		for (PropertiesObject mo : propertiesObjects) {

			// Get allfields if it is set
			if (mo.isGetAllFields()) {
				hasAllFieldsField = true;
				allfieldsField = mo.getSolrFieldname(); // Get Solr field to which all fields should be indexed
				allFieldsExceptions = mo.getAllFieldsExceptions(); // Get list of fields that should not be indexed
			}

			// Get fullrecord field if it is set
			if (mo.isGetFullRecordAsXML()) {
				getFullRecordAsXML = true;
				fullrecordField = mo.getSolrFieldname(); // Get Solr field to which the full record (as XML) should be indexed
			}
		}
		
		// Process more documents at once for enrichment - it's a little bit faster
		// TODO: Set the no. of docs to precess in AkImporter.properties
		if (this.enrich) {
			NO_OF_DOCS = 1500;
		}
	}


	/**
	 * Executed when encountering the start element of the XML file.<br><br>
	 * {@inheritDoc}
	 */
	@Override
	public void startDocument() throws SAXException {
		// On document start, create a new list to hold all parsed XML records
		rawRecords = new ArrayList<RawRecord>();
	}


	/**
	 * Executed when encountering the start element of an XML tag.
	 * Reading and processing XML attributes is done here.
	 * Reading of element content (text) is done in endElement() method.<br><br>
	 * {@inheritDoc}
	 */
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attribs) throws SAXException {

		// Cleare the node content (= text of XML element). If not, there will be problems with html-encoded characters (&lt;) at character()-method:
		nodeContent = "";

		// If the parser encounters the start of the "record"-tag, create new List to hold the fields of
		// this record and a new record-object to add these list:
		if(localName.equals("record")) {
			rawRecord = new RawRecord(); // A new RawRecord object
			controlfields = new ArrayList<Controlfield>(); // All controlfields of the record
			datafields = new ArrayList<Datafield>(); // All datafields of the record
			fullrecordXmlString = null; // Reset the String for the fullRecord field for a new record
			if (getFullRecordAsXML) {
				fullrecordXmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><collection><record>"; // Begin new XML for the current record
			}
		}

		// Parser encounters the start of the "leader"-tag
		if(localName.equals("leader")) {
			leader = new Leader();
			leader.setTag("leader");
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<leader>";
			}
		}

		// If the parser encounters the start of a "controlfield"-tag, create a new Controlfield object, get the XML attributes and set them on the object
		if(localName.equals("controlfield")) {
			String tag = attribs.getValue("tag").trim();
			controlfield = new Controlfield();
			controlfield.setTag(tag);
			isSYS = (tag.equals("SYS")) ? true : false;
			is001Controlfield = (tag.equals("001")) ? true : false;
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<controlfield tag=\""+tag+"\">";
			}
		}

		// If the parser encounters the start of a "datafield"-tag, create a new Datafield object, get the XML attributes and set them on the object
		if(localName.equals("datafield")) {
			String tag = attribs.getValue("tag").trim();
			String ind1 = attribs.getValue("ind1").trim();
			String ind2 = attribs.getValue("ind2").trim();

			// Set empty tags and indicators to a character, so that the string to match against is always
			// of the same length, e. g. "311$ab$c" and "000$**$*". This prevents errors.
			tag = (tag != null && !tag.isEmpty()) ? tag : "000";
			ind1 = (ind1 != null && !ind1.isEmpty()) ? ind1 : "-";
			ind2 = (ind2 != null && !ind2.isEmpty()) ? ind2 : "-";

			datafield = new Datafield();
			datafield.setTag(tag);
			datafield.setInd1(ind1);
			datafield.setInd2(ind2);

			subfields = new ArrayList<Subfield>(); // List to hold all subfields of the datafield

			is001Datafield = (tag.equals("001")) ? true : false;
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<datafield tag=\""+tag+"\" ind1=\""+ind1+"\" ind2=\""+ind2+"\">";
			}
		}


		// If the parser encounters the start of a "subfield"-tag, create a new Subfield object, get the XML attributes and set them on the object
		if(localName.equals("subfield")) {
			String code = attribs.getValue("code").trim();
			code = (code != null && !code.isEmpty()) ? code : "-";
			subfield = new Subfield();
			subfield.setCode(code);
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<subfield code=\""+code+"\">";
			}
		}

	}


	/**
	 * Executed when encountering the end element of an XML tag.
	 * Reading of element content (text) is done here (see also characters() method).
	 * Reading and processing XML attributes is done in startElement() method.<br><br>
	 * {@inheritDoc}
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		String content = nodeContent.toString();

		// Parser encounters the end of the "leader"-tag
		if(localName.equals("leader")) {
			leader.setContent(content);
			if (getFullRecordAsXML) {
				fullrecordXmlString += StringEscapeUtils.escapeXml10(content)+"</leader>";
			}
		}

		// Parser encounters the end of a "controlfield"-tag, so our Controlfield object can be treated here
		if(localName.equals("controlfield") ) {
			controlfield.setContent(content);
			controlfields.add(controlfield);
			if (getFullRecordAsXML) {
				fullrecordXmlString += StringEscapeUtils.escapeXml10(content)+"</controlfield>";
			}
			if (isSYS == true) {
				recordSYS = content;
			}
			if (is001Controlfield == true && is001Datafield == false) {
				recordID = content;
			}
		}


		// Parser encounters the end of a "subfield"-tag, so our Subfield object can be treated here
		if(localName.equals("subfield")) {
			subfield.setContent(content);
			subfields.add(subfield);
			if (getFullRecordAsXML) {
				fullrecordXmlString += StringEscapeUtils.escapeXml10(content).trim()+"</subfield>";
			}
			if (is001Datafield == true && is001Controlfield == false) {
				recordID = content;
			}
		}


		// Parser encounters the end of a "datafield"-tag, so our Datafield object can be treated here
		if(localName.equals("datafield")) {
			datafield.setSubfields(subfields);
			datafields.add(datafield);
			if (getFullRecordAsXML) {
				fullrecordXmlString += "</datafield>";
			}
		}

		// If the parser encounters the end of the "record"-tag, add all controlfield-objects, datafield-objects and some other information
		// to the rawRecord object and add the it to the list of all raw records
		if(localName.equals("record")) {

			counter = counter + 1;	
			rawRecord.setRecordID(recordID);
			rawRecord.setRecordSYS(recordSYS);
			rawRecord.setIndexTimestamp(timeStamp);
			rawRecord.setLeader(leader);
			rawRecord.setControlfields(controlfields);
			rawRecord.setDatafields(datafields);
			
			if (getFullRecordAsXML) {
				fullrecordXmlString += "</record></collection>";
				rawRecord.setFullRecord(fullrecordXmlString);
			}
			
			rawRecords.add(rawRecord);

			print(this.print, "Indexing record " + ((recordID != null) ? recordID : recordSYS) + ", No. indexed: " + counter + "                 \r");

			// Every n-th record, match the XML records to the Solr records. We then get an appropirate List of SolrRecord object and can index
			// it's values to Solr. Then we will empty all objects (clear and set to "null") to save memory and go on with the next n records.
			// If there is a rest at the end of the file, do the same thing in the endDocument() method. E. g. modulo (NO_OF_DOCS) is set to 100
			// and we have 733 records, but at this point, only 700 are indexed. The 33 remaining records will be indexed in endDocument() method.
			if (counter % NO_OF_DOCS == 0) {

				// Do the matching and rewriting (see class "MatchingOperations"):
				MatchingOperations matchingOperations = new MatchingOperations();
				matchingOperations.setRawRecords(rawRecords);
				matchingOperations.setPropertiesObjects(propertiesObjects);
				List<SolrRecord> solrRecords = matchingOperations.getSolrRecords();

				// Add to Solr index or enrich existing index:
				if (!this.enrich) {
					this.solrAddRecordSet(sServer, solrRecords);
				} else {
					this.solrEnrichExistingRecords(sServer, solrRecords);
				}
				
				// Set all relevant Objects to "null" to save memory
				matchingOperations = null;
				rawRecords.clear();
				rawRecords = null;
				rawRecords = new ArrayList<RawRecord>();
				solrRecords.clear();
				solrRecords = null;
			}
		}
	}


	@Override
	public void endDocument() throws SAXException {

		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
		//+++++++++++++++ Add the remaining rest of the records to the index (see modulo-operation with "%"-operator in endElement()) +++++++++++++++//
		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

		// Do the matching and rewriting (see class "MatchingOperations"):
		MatchingOperations matchingOperations = new MatchingOperations();
		matchingOperations.setRawRecords(rawRecords);
		matchingOperations.setPropertiesObjects(this.propertiesObjects);
		List<SolrRecord> solrRecords = matchingOperations.getSolrRecords();

		// Add to Solr-Index:
		//this.solrAddRecordSet(sServer, solrRecords);
		
		// Add to Solr index or enrich existing index:
		if (!this.enrich) {
			this.solrAddRecordSet(sServer, solrRecords);
		} else {
			this.solrEnrichExistingRecords(sServer, solrRecords);
		}

		// Set all relevant Objects to "null" to save memory
		matchingOperations = null;
		rawRecords.clear();
		rawRecords = null;
		solrRecords.clear();
		solrRecords = null;
		propertiesObjects.clear();
		propertiesObjects = null;
	}


	/**
	 * Reads the content of the current XML element.<br><br>
	 * {@inheritDoc}
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		nodeContent += new String(ch, start, length);
	}

	
	public void solrEnrichExistingRecords(SolrServer sServer, List<SolrRecord> solrRecordSet) {
		try {
			Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			
			for (SolrRecord solrRecord : solrRecordSet) {
				//System.out.println(solrRecord);
				String docId = solrRecord.getRecordID();
				
				// Prepare record for atomic updates:
				SolrInputDocument enrichSolrDoc = null;
				enrichSolrDoc = new SolrInputDocument();
				enrichSolrDoc.setField("id", docId);
				
				for (SolrField solrField : solrRecord.getSolrFields()) {
					
					String atomicUpdateMode = null;
					if (solrField.isEnrichSet()) {
						atomicUpdateMode = "set";
					} else if (solrField.isEnrichAdd()) {
						atomicUpdateMode = "add";
					}
					
					String fieldName = solrField.getFieldname();
					
					Object fieldValue = null;
					if (solrField.isMultivalued()) {
						fieldValue = solrField.getFieldvalues();
					} else {
						fieldValue = solrField.getFieldvalues().get(0);
					}

					// Add values to record with atomic update:
					Map<String, Object> mapRecordField = new HashMap<String, Object>();
					mapRecordField.put(atomicUpdateMode, fieldValue);
					if (solrField.isEnrichSet()) {
						enrichSolrDoc.setField(fieldName, mapRecordField);
					} else if (solrField.isEnrichAdd()) {
						enrichSolrDoc.addField(fieldName, mapRecordField);
					}
				}
				docsForAtomicUpdates.add(enrichSolrDoc);
			}
			
			if (!docsForAtomicUpdates.isEmpty()) {

				// Now add the collection of documents to Solr:
				sServer.add(docsForAtomicUpdates);

				// Set "docs" to "null" (save memory):
				docsForAtomicUpdates = null;
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * This method contains the code that actually adds a set of SolrRecord objects
	 * (see SolrRecord class) to the specified Solr server.
	 *
	 * @param sServer			SolrServer: The Solr server to which the data should be indexed.
	 * @param solrRecordSet		List<SolrRecord>: A list of SolrRecord objects that should be indexed.
	 */
	public void solrAddRecordSet(SolrServer sServer, List<SolrRecord> solrRecordSet) {
		try {

			// Create a collection of all documents:
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			for (SolrRecord solrRecord : solrRecordSet) {

				// Create a document:
				SolrInputDocument doc = new SolrInputDocument();

				// Create a HashSet for the allfields field if one is defined.
				// We use a Set to prevent duplicate values
				Set<String> allfieldsSet = null;
				if (hasAllFieldsField) {
					allfieldsSet = new HashSet<String>();
				}

				// Add values to the Solr record
				for (SolrField sf : solrRecord.getSolrFields()) {

					String fieldName = sf.getFieldname();
					List<String> fieldValues = sf.getFieldvalues();
					doc.addField(fieldName, fieldValues);

					// Add values to the "allfields" field, except for the exception values defined in mab.properties:
					if (hasAllFieldsField) {
						if (!allFieldsExceptions.contains(fieldName)) {
							allfieldsSet.addAll(fieldValues);
						}
					}
				}

				// Add the timestamp of indexing (it is the timstamp of the beginning of the indexing process):
				// TODO: Change hardecoded fieldname "indexTimestamp_str" to fieldname specified in .properties file.
				doc.addField("indexTimestamp_str", solrRecord.getIndexTimestamp());
				
				// Add the allfields field to the document if it is used
				if (hasAllFieldsField) {
					doc.addField(allfieldsField, allfieldsSet);
				}
				
				// Add the fullRecord field to the document if it is used
				if (getFullRecordAsXML) {
					doc.addField(fullrecordField, solrRecord.getFullRecord());
				}
				
				// Add the document to the collection of documents:
				docs.add(doc);
			}

			if (!docs.isEmpty()) {

				// Now add the collection of documents to Solr:
				sServer.add(docs);

				// Set "docs" to "null" (save memory):
				docs = null;
			}

		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Prints the specified text to the console if "print" is true.
	 * 
	 * @param print		boolean: true if the text should be print
	 * @param text		String: a text message to print.
	 */
	private void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}

	
	@Override
	public void endPrefixMapping(String arg0) throws SAXException {}

	@Override
	public void ignorableWhitespace(char[] arg0, int arg1, int arg2) throws SAXException {}

	@Override
	public void processingInstruction(String arg0, String arg1) throws SAXException {}

	@Override
	public void setDocumentLocator(Locator arg0) {}

	@Override
	public void skippedEntity(String arg0) throws SAXException {}

	@Override
	public void startPrefixMapping(String arg0, String arg1) throws SAXException {}
}