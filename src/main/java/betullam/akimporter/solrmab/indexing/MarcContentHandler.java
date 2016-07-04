/**
 * Handles the contents of the MarcXML files.
 * This is where some of the data processing is done to
 * get the values in shape before indexing them to Solr.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;


public class MarcContentHandler implements ContentHandler {

	MatchingOperations matchingOps = new MatchingOperations();
	List<Mabfield> allFields;
	List<Record> allRecords;
	private String nodeContent;
	private Record record;
	private Mabfield controlfield;
	private Mabfield datafield; // Datafield is conentenated of datafield and subfield
	private List<MatchingObject> listOfMatchingObjs;
	private SolrServer sServer;
	private String recordID;
	private String recordSYS;
	private boolean is001Controlfield;
	private boolean is001Datafield;
	private boolean isSYS;
	private boolean print = true;
	int counter = 0;
	long startTime;
	long endTime;
	String timeStamp;
	int NO_OF_DOCS = 500;

	String controlfieldTag;
	String datafieldTag;
	String datafieldInd1;
	String datafieldInd2;
	String subfieldCode;

	// Variables for connected fields:
	private LinkedHashSet<Connectedfield> connectedFields = new LinkedHashSet<Connectedfield>();
	private boolean datafieldContainsConnectedFields = false;
	Connectedfield currentConnectedField = null;
	LinkedHashMap<Integer, Map<String, List<String>>> currentConnectedSubfields = null;
	private boolean connectedValueRequired = false;
	List<String> currentMasterSubfields = new ArrayList<String>();
	Map<String, String> currentMasterSubfieldsValues = new HashMap<String, String>();
	List<String> connectedValuesToUse = new ArrayList<String>();
	Map<String, String> subfieldsInDatafield = new HashMap<String, String>();
	
	// Variables for concatenated fields:
	LinkedHashSet<ConcatenatedField> concatenatedFields = new LinkedHashSet<ConcatenatedField>();
	private boolean datafieldContainsConcatenatedFields = false;
	ConcatenatedField currentConcatenatedField = null;
	List<String> currentConcatenatedSubfields = null;
	List<String> currentConcatenatedMasterSubfields = new ArrayList<String>();
	Map<String, String> currentConcatenatedMasterSubfieldsValues = new HashMap<String, String>();
	private boolean concatenatedValueRequired = false;
	List<String> concatenatedValuesToUse = new ArrayList<String>();
	Map<String, String> concatenatedSubfieldsInDatafield = new HashMap<String, String>();
	

	// Variables for allfields:
	private boolean hasAllFieldsField = false;
	private String allfieldsField = null;
	private List<String> allFieldsExceptions = new ArrayList<String>();

	// Variable for fullrecord
	private boolean getFullRecordAsXML = false;
	private String fullrecordField = null;
	private String fullrecordXmlString = null;


	/**
	 * Constructor of MarcContentHandler.
	 * This is the starting point of reading and processing the XML file(s) containing MARC records.
	 * 
	 * @param listOfMatchingObjs	List<MatchingObject>. A MatchingObject contains information about matching MAB fields to Solr fields.
	 * @param solrServer			SolrServer object that represents the Solr server to which the data should be indexed 
	 * @param timeStamp				String that specifies the starting time of the importing process
	 * @param print					boolean. True if status messages should be printed to the console.
	 */
	public MarcContentHandler(List<MatchingObject> listOfMatchingObjs, SolrServer solrServer, String timeStamp, boolean print) {
		this.listOfMatchingObjs = listOfMatchingObjs;
		this.sServer = solrServer;
		this.timeStamp = timeStamp;
		this.print = print;

		for (MatchingObject mo : listOfMatchingObjs) {
			
			// Get list of connected fields. We need to check them while parsing the XML.
			if (mo.hasConnectedSubfields()) {
				
				LinkedHashMap<Integer, String> connectedSubfields = mo.getConnectedSubfields();
				LinkedHashMap<Integer, Map<String, List<String>>> mapConnectedSubfields = new LinkedHashMap<Integer, Map<String, List<String>>>();

				for (Entry<Integer, String> connectedSubfield : connectedSubfields.entrySet()) {

					Set<String> connectedMasterFields = new HashSet<String>();
					String connectedDefaultValue = null;
					List<String> immutableList = Arrays.asList(connectedSubfield.getValue().split("\\s*:\\s*"));
					List<String> mutableList = new ArrayList<String>();
					mutableList.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
					int lastListElement = (mutableList.size()-1); // Get index of last List element
					boolean isTranslateConnectedSubfields = mo.isTranslateConnectedSubfields();
					Map<String, String> translateSubfieldsProperties = mo.getTranslateSubfieldsProperties();

					// Get all master fields:
					for (Entry<String, List<String>> mabfieldName : mo.getMabFieldnames().entrySet()) {
						String completeFieldname = mabfieldName.getKey().toString();
						String connectedMasterDatafield = completeFieldname.substring(0,3); // Get e. g. "655" out of "655$e*$u"
						String connectedMasterSubfield = completeFieldname.substring(completeFieldname.length() - 1); // Get last character which should be the subfield code, e. g. "u" out of "655$e*$u"
						connectedMasterFields.add(connectedMasterDatafield+connectedMasterSubfield);
					}
					connectedDefaultValue = mutableList.get(lastListElement); // Last value is always the default value to use
					mutableList.remove(lastListElement); // Remove the default value so that only the subfield codes will remain
					
					Map<String, List<String>> mapSubfieldValues = new HashMap<String, List<String>>();
					mapSubfieldValues.put(connectedDefaultValue, mutableList);
					mapConnectedSubfields.put(connectedSubfield.getKey(), mapSubfieldValues);	
				
					Connectedfield newConnectedfield = new Connectedfield(connectedMasterFields, mapConnectedSubfields, isTranslateConnectedSubfields, translateSubfieldsProperties);
					connectedFields.add(newConnectedfield);
				}
			}
			
			if (mo.hasConcatenatedSubfields()) {
				Set<String> concatenatedMasterFields = new HashSet<String>();
				LinkedHashMap<Integer, String> concatenatedSubfields = mo.getConcatenatedSubfields();
				
				for (Entry<Integer, String> concatenatedSubfield : concatenatedSubfields.entrySet()) {
					
					List<String> immutableList = Arrays.asList(concatenatedSubfield.getValue().split("\\s*:\\s*"));
					List<String> mutableList = new ArrayList<String>();
					mutableList.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
					int lastListElement = (mutableList.size()-1); // Get index of last List element (= separator)
					
					// Get all master fields:
					for (Entry<String, List<String>> mabfieldName : mo.getMabFieldnames().entrySet()) {
						String completeFieldname = mabfieldName.getKey().toString();
						String concatenatedMasterDatafield = completeFieldname.substring(0,3); // Get e. g. "655" out of "655$e*$u"
						String concatenatedMasterSubfield = completeFieldname.substring(completeFieldname.length() - 1); // Get last character which should be the subfield code, e. g. "u" out of "655$e*$u"
						concatenatedMasterFields.add(concatenatedMasterDatafield+concatenatedMasterSubfield);
					}
					
					String concatenatedFieldsSeparator = mutableList.get(lastListElement); // Last value is always the separator to use
					mutableList.remove(lastListElement); // Remove the separator value so that only the subfield codes will remain
					
					ConcatenatedField newConcatenatedField = new ConcatenatedField(concatenatedMasterFields, mutableList, concatenatedFieldsSeparator);
					concatenatedFields.add(newConcatenatedField);
				}
				
			}
			

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
		
	}



	/**
	 * Executed when encountering the start element of the XML file.
	 */
	@Override
	public void startDocument() throws SAXException {

		// For tracking the elapsed time:
		startTime = System.currentTimeMillis();

		// On document-start, crate new list to hold all parsed AlephMARCXML-records:
		allRecords = new ArrayList<Record>();
	}


	/**
	 * Executed when encountering the start element of an XML tag.
	 * 
	 * Reading and processing XML attributes is done here.
	 * Reading of element content (text) is processed in endElement().
	 */
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attribs) throws SAXException {

		// Empty node content (text), else, there will be problems with html-encoded characters (&lt;) at
		// character()-method:
		nodeContent = "";

		// If the parser encounters the start of the "record"-tag, create new List to hold the fields of
		// this record and a new record-object to add these list:
		if(localName.equals("record")) {
			allFields = new ArrayList<Mabfield>();
			record = new Record();
			fullrecordXmlString = null; // Reset for new record
			if (getFullRecordAsXML) {
				fullrecordXmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><collection><record>"; // Begin new XML for the current record
			}
		}

		// Parser encounters the start of the "leader"-tag (only necessary if we need to get the full record as XML)
		if (getFullRecordAsXML) {
			if(localName.equals("leader")) {
				fullrecordXmlString += "<leader>";
			}
		}

		// If the parser encounters the start of a "controlfield"-tag, create a new mabfield-object, get the XML-attributes, set them on the object and add it to the "record"-object:
		if(localName.equals("controlfield")) {
			controlfieldTag = attribs.getValue("tag");
			isSYS = (controlfieldTag.equals("SYS")) ? true : false;
			controlfield = new Mabfield();
			controlfield.setFieldname(controlfieldTag);
			is001Controlfield = (controlfieldTag.equals("001")) ? true : false;
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<controlfield tag=\""+controlfieldTag+"\">";
			}
		}

		if(localName.equals("datafield")) {
			datafieldTag = attribs.getValue("tag").trim();
			datafieldInd1 = attribs.getValue("ind1").trim();
			datafieldInd2 = attribs.getValue("ind2").trim();

			// Set empty tags and indicators to a character, so that the string to match against is always
			// of the same length, e. g. "311$ab$c" and "000$**$*". This prevents errors.
			datafieldTag = (datafieldTag != null && !datafieldTag.isEmpty()) ? datafieldTag : "000";
			datafieldInd1 = (datafieldInd1 != null && !datafieldInd1.isEmpty()) ? datafieldInd1 : "*";
			datafieldInd2 = (datafieldInd2 != null && !datafieldInd2.isEmpty()) ? datafieldInd2 : "*";
			is001Datafield = (datafieldTag.equals("001")) ? true : false;
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<datafield tag=\""+datafieldTag+"\" ind1=\""+datafieldInd1+"\" ind2=\""+datafieldInd2+"\">";
			}

			// Connected fields
			for (Connectedfield connectedField : connectedFields) {
				
				for (String masterField : connectedField.getConnectedMasterFields()) {
					String masterFieldName = masterField.substring(0,3);
					String masterFieldSubfield = masterField.substring(masterField.length() - 1);
					
					if (masterFieldName.equals(datafieldTag)) {
						datafieldContainsConnectedFields = true;
						currentMasterSubfields.add(masterFieldSubfield); // TODO: Could currentMasterSubfields be a Set to avoid duplicates?
						currentConnectedField = connectedField;
						currentConnectedSubfields = currentConnectedField.getConnectedSubfields();
					}
				}
			}
			
			// Concatenated fields
			for (ConcatenatedField concatenatedField : concatenatedFields) {
				
				for (String concatenatedMasterField : concatenatedField.getConcatenatedMasterFields()) {
					String concatenatedMasterFieldName = concatenatedMasterField.substring(0,3);
					String concatenatedMasterFieldSubfield = concatenatedMasterField.substring(concatenatedMasterField.length() - 1);
					
					if (concatenatedMasterFieldName.equals(datafieldTag)) {
						datafieldContainsConcatenatedFields = true;
						currentConcatenatedMasterSubfields.add(concatenatedMasterFieldSubfield); // TODO: Could currentMasterSubfields be a Set to avoid duplicates?
						currentConcatenatedField = concatenatedField;
						currentConcatenatedSubfields = currentConcatenatedField.getConcatenatedSubfields();
					}
				}
			}
			
		}


		if(localName.equals("subfield")) {
			subfieldCode = attribs.getValue("code").trim();
			subfieldCode = (subfieldCode != null && !subfieldCode.isEmpty()) ? subfieldCode : "-";
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<subfield code=\""+subfieldCode+"\">";
			}
			String datafieldName = datafieldTag + "$" + datafieldInd1 + datafieldInd2 + "$" + subfieldCode;

			// Create a new mabfield so that we can concentenate a datafield and a subfield to a mabfield
			// E. g.: Datafield = 100$**, Subfield = r, Subfield content = AC123456789
			//        Result: mabfield name = 100$**$r, mabfield value = AC123456789
			datafield = new Mabfield();
			datafield.setFieldname(datafieldName);
		}

	}



	/**
	 * Executed when encountering the end element of an XML tag.
	 * 
	 * Reading and processing XML attributes is done here.
	 * Reading of element content (text) is done here (see also characters() method).
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {

		// Parser encounters the start of the "leader"-tag (only necessary if we need to get the full record as XML)
		if (getFullRecordAsXML) {
			if(localName.equals("leader")) {
				String leaderText = nodeContent.toString();
				fullrecordXmlString += StringEscapeUtils.escapeXml10(leaderText)+"</leader>";
			}
		}

		if(localName.equals("controlfield") ) {
			String controlfieldText = nodeContent.toString();
			controlfield.setFieldvalue(controlfieldText);
			allFields.add(controlfield);
			if (getFullRecordAsXML) {
				fullrecordXmlString += StringEscapeUtils.escapeXml10(controlfieldText)+"</controlfield>";
			}
			if (isSYS == true) {
				recordSYS = controlfieldText;
			}
			if (is001Controlfield == true && is001Datafield == false) {
				recordID = controlfieldText;
			}
		}


		if(localName.equals("subfield")) {

			String subfieldText = nodeContent.toString();
			if (getFullRecordAsXML) {
				fullrecordXmlString += StringEscapeUtils.escapeXml10(subfieldText).trim()+"</subfield>";
			}

			
			if (datafieldContainsConnectedFields) { // If there could be a connected subfield within the datafield, but we don't know yet if it really exists. For that, we have to wait for the end of the datafield tag in endElement().
				if (currentMasterSubfields.contains(subfieldCode)) { // If it is one of the connected master subfields
					connectedValueRequired = true;
					currentMasterSubfieldsValues.put(subfieldCode, subfieldText); // Add subfield code and text to an intermediate Map (a Map because there could be more master subfields)
				} else { // Do the default operation
					datafield.setFieldvalue(subfieldText);
					allFields.add(datafield);
					subfieldsInDatafield.put(subfieldCode, subfieldText);
				}
			} else if (datafieldContainsConcatenatedFields) { // If there could be a concatenated subfield within the datafield, but we don't know yet if it really exists.
				if (currentConcatenatedMasterSubfields.contains(subfieldCode)) { // If it is one of the concatenated master subfields
					concatenatedValueRequired = true;
					currentConcatenatedMasterSubfieldsValues.put(subfieldCode, subfieldText); // Add subfield code and text to an intermediate Map (a Map because there could be more master subfields)
				} else { // Do the default operation
					concatenatedSubfieldsInDatafield.put(subfieldCode, subfieldText);
				}
			} else { // Default operation - no connected or concatenated value
				datafield.setFieldvalue(subfieldText);
				allFields.add(datafield);
			}
			
			

			if (is001Datafield == true && is001Controlfield == false) {
				recordID = subfieldText;
			}
		}

		
		
		if(localName.equals("datafield")) {

			if (getFullRecordAsXML) {
				fullrecordXmlString += "</datafield>";
			}
			
			// Set the connected datafields:
			if (datafieldContainsConnectedFields && connectedValueRequired) {
								
				for (Entry<String, String> currentMasterSubfieldsEntry : currentMasterSubfieldsValues.entrySet()) {
					String currentMasterSubfieldCode = currentMasterSubfieldsEntry.getKey();
					String currentMasterSubfieldText = currentMasterSubfieldsEntry.getValue();

					String datafieldName = datafieldTag + "$" + datafieldInd1 + datafieldInd2 + "$" + currentMasterSubfieldCode;
					Mabfield connectedDatafield = new Mabfield();
					connectedDatafield.setFieldname(datafieldName);
					connectedDatafield.setFieldvalue(currentMasterSubfieldText);					
						
					for (Entry<Integer, Map<String, List<String>>> entry1 : currentConnectedSubfields.entrySet()) {
						Map<String, List<String>> entrySet2 = entry1.getValue();
						
						for (Entry<String, List<String>> entry2 : entrySet2.entrySet()) {
							String defaultValue = entry2.getKey();
							List<String> bracketValues = entry2.getValue();
							String subfieldText = null;
							String textToUse = null;
							
							// Add connected value if it exists
							for (String bracketValue : bracketValues) {
								subfieldText = subfieldsInDatafield.get(bracketValue);
								if (subfieldText != null) {
									if (textToUse == null) {
										textToUse = subfieldText;
										connectedValuesToUse.add(textToUse);
									}
								}
							}
							
							// Add default connected value
							if (textToUse == null) {
								textToUse = defaultValue;
								connectedValuesToUse.add(textToUse);
							}
						}
					}
					
					connectedDatafield.setConnectedValues(connectedValuesToUse);
					connectedDatafield.setTranslateConnectedSubfields(currentConnectedField.isTranslateConnectedSubfields());
					connectedDatafield.setTranslateSubfieldsProperties(currentConnectedField.getTranslateSubfieldsProperties());
					allFields.add(connectedDatafield);
					connectedDatafield = null;
				}
			}
			
			// Set the concatenated datafields:
			if (datafieldContainsConcatenatedFields && concatenatedValueRequired) {
				for (Entry<String, String> currentConcatenatedMasterSubfieldsEntry : currentConcatenatedMasterSubfieldsValues.entrySet()) {
					String currentConcatenatedMasterSubfieldCode = currentConcatenatedMasterSubfieldsEntry.getKey();
					String currentConcatenatedMasterSubfieldText = currentConcatenatedMasterSubfieldsEntry.getValue();
					String subfieldText = null;
					List<String> concatenatedSubfieldValues = new ArrayList<String>();
					String datafieldTextToUse = null;
					
					String datafieldName = datafieldTag + "$" + datafieldInd1 + datafieldInd2 + "$" + currentConcatenatedMasterSubfieldCode;
					Mabfield concatenatedDatafield = new Mabfield();
					concatenatedDatafield.setFieldname(datafieldName);
					
					for (String currentConcatenatedSubfield : currentConcatenatedSubfields) {
						subfieldText = concatenatedSubfieldsInDatafield.get(currentConcatenatedSubfield);
						if (subfieldText != null && subfieldText.trim() != "") {
							concatenatedSubfieldValues.add(subfieldText);
						}
					}
					
					// Concatenate fields
					if (!concatenatedSubfieldValues.isEmpty()) {
						String separatorToUse = currentConcatenatedField.getConcatenatedFieldsSeparator();
						datafieldTextToUse = currentConcatenatedMasterSubfieldText + separatorToUse + StringUtils.join(concatenatedSubfieldValues, separatorToUse);
					} else {
						datafieldTextToUse = currentConcatenatedMasterSubfieldText;
					}
					
					concatenatedDatafield.setFieldvalue(datafieldTextToUse);
					allFields.add(concatenatedDatafield);
					concatenatedDatafield = null;
				}
			}
			
			
			// Reset values for connected fields:
			datafieldContainsConnectedFields = false;
			connectedValueRequired = false;
			currentConnectedField = null;
			currentConnectedSubfields = null;
			currentMasterSubfields = new ArrayList<String>();
			currentMasterSubfieldsValues = new HashMap<String, String>();
			connectedValuesToUse = new ArrayList<String>();
			subfieldsInDatafield = new HashMap<String, String>();
			
			// Reset values for concatenated fields:
			datafieldContainsConcatenatedFields = false;
			concatenatedValueRequired = false;
			currentConcatenatedField = null;
			currentConcatenatedSubfields = null;
			currentConcatenatedMasterSubfields = new ArrayList<String>();
			currentConcatenatedMasterSubfieldsValues = new HashMap<String, String>();
			concatenatedValuesToUse = new ArrayList<String>();
			concatenatedSubfieldsInDatafield = new HashMap<String, String>();
			
			
			
		}

		// If the parser encounters the end of the "record"-tag, add all
		// leader-, controlfield- and datafield-objects to the record-object and add the
		// record-object to the list of all records:
		if(localName.equals("record")) {

			// End XML representation of the current record for "fullrecord" Solr field:
			if (getFullRecordAsXML) {
				fullrecordXmlString += "</record></collection>";
				Mabfield fullRecordAsXML = new Mabfield();
				fullRecordAsXML.setFieldname(fullrecordField);
				fullRecordAsXML.setFieldvalue(fullrecordXmlString);
				allFields.add(fullRecordAsXML);
			}

			counter = counter + 1;
			record.setMabfields(allFields);
			record.setRecordID(recordID);
			record.setRecordSYS(recordSYS);
			record.setIndexTimestamp(timeStamp);

			allRecords.add(record);

			print(this.print, "Indexing record " + ((recordID != null) ? recordID : recordSYS) + ", No. indexed: " + counter + "                 \r");

			/** Every n-th record, match the Mab-Fields to the Solr-Fields, write an appropirate object, loop through the object and index
			 * it's values to Solr, then empty all objects (clear and set to "null") to save memory and go on with the next n records.
			 * If there is a rest, do it in the endDocument()-Method. E. g. modulo is set to 100 and we have 733 records, but at this point,
			 * only 700 are indexed. The 33 remaining records will be indexed in endDocument() function.
			 */
			if (counter % NO_OF_DOCS == 0) {

				// Do the Matching and rewriting (see class "MatchingOperations"):
				List<Record> newRecordSet = matchingOps.matching(allRecords, listOfMatchingObjs);

				// Add to Solr-Index:
				this.solrAddRecordSet(sServer, newRecordSet);

				// Set all Objects to "null" to save memory
				allRecords.clear();
				allRecords = null;
				allRecords = new ArrayList<Record>();
				allFields.clear();
				allFields = null;
				newRecordSet = null;
			}
		}
	}


	/**
	 * Executed when encountering the end element of the XML file.
	 */
	@Override
	public void endDocument() throws SAXException {

		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
		//+++++++++++++++ Add the remaining rest of the records to the index (see modulo-operation with "%"-operator in endElement()) +++++++++++++++//
		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

		// Do the Matching and rewriting (see class "MatchingOperations"):
		List<Record> newRecordSet = matchingOps.matching(allRecords, listOfMatchingObjs);

		// Add to Solr-Index:
		this.solrAddRecordSet(sServer, newRecordSet);

		// Clear Objects to save memory
		allRecords.clear();
		allRecords = null;
		newRecordSet.clear();
		newRecordSet = null;
		listOfMatchingObjs.clear();
		listOfMatchingObjs = null;

	}


	/**
	 * Reads the content of the current XML element:
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		nodeContent += new String(ch, start, length);
	}


	/**
	 * This method contains the code that actually adds a set of Record objects
	 * (see Record class) to the specified Solr server.
	 */
	public void solrAddRecordSet(SolrServer sServer, List<Record> recordSet) {
		try {

			// Create a collection of all documents:
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			for (Record record : recordSet) {

				// Create a document:
				SolrInputDocument doc = new SolrInputDocument();

				
				// Create a HashSet for the allfields field if one is defined.
				// We do that do prevent duplicated values
				Set<String> allfieldsSet = null;
				if (hasAllFieldsField) {
					allfieldsSet = new HashSet<String>();
				}
				

				for (Mabfield mf : record.getMabfields()) {

					String fieldName = mf.getFieldname();
					String fieldValue = mf.getFieldvalue();
					List<String> connValues = mf.getConnectedValues();
					
					// Add the fieldname and fieldvalue to the document:
					doc.addField(fieldName, fieldValue);

					// Add the connected value to the document if one exists:
					if (connValues != null && !connValues.isEmpty()) {
						for (String connValue : connValues) {
							doc.addField(fieldName, connValue);
						}
					}

					// Add values to the "allfields" field, except for the exception values defined in mab.properties:
					if (hasAllFieldsField) {
						if (!allFieldsExceptions.contains(fieldName)) {
							allfieldsSet.add(fieldValue);
							if (connValues != null && !connValues.isEmpty()) {
								for (String connValue : connValues) {
									allfieldsSet.add(connValue);
								}
							}
						}
					}
				}

				// Add the timestamp of indexing (it is the timstamp of the beginning of the indexing process):
				doc.addField("indexTimestamp_str", record.getIndexTimestamp());

				// Add the allfields field to the document if it is used
				if (hasAllFieldsField) {
					doc.addField(allfieldsField, allfieldsSet);
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
	 * Unused methods of ContentHandler class.
	 * We just define them without any content.
	 */
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


	/**
	 * Prints the specified text to the console if "print" is true.
	 * 
	 * @param print	boolean: true if the text should be print
	 * @param text	String: a text message to print.
	 */
	private void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}

}