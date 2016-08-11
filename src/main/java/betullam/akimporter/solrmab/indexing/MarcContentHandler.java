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
import java.util.Collections;
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

	// ################ OLD #################
	MatchingOperations matchingOps = new MatchingOperations();
	List<Mabfield> allFields;
	List<Record> allRecords;
	private String nodeContent;
	private Record record;
	private Mabfield mabfieldControlfield;
	private Mabfield mabfield; // Mabfield is concatenated datafield and subfield
	private List<Mabfield> allSubfieldsInDatafield;
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
	Map<String, String> connectedSubfieldsInDatafield = new HashMap<String, String>();

	// Variables for concatenated fields:
	LinkedHashSet<ConcatenatedField> concatenatedFields = new LinkedHashSet<ConcatenatedField>();
	private boolean datafieldContainsConcatenatedFields = false;
	ConcatenatedField currentConcatenatedField = null;
	List<String> currentConcatenatedSubfields = null;
	List<String> currentConcatenatedMasterSubfields = new ArrayList<String>();
	Map<String, String> currentConcatenatedMasterSubfieldsValues = new HashMap<String, String>();
	private boolean concatenatedValueRequired = false;
	List<String> concatenatedValuesToUse = new ArrayList<String>();
	String concatenatedValuesSeparatorToUse = null;
	Map<String, String> concatenatedSubfieldsInDatafield = new HashMap<String, String>();

	// Variables for subfield exists / not exists:
	boolean isSubfieldExists = false;
	boolean isSubfieldNotExists = false;
	LinkedHashSet<ExistsField> existsFields = new LinkedHashSet<ExistsField>();
	private boolean datafieldContainsExistsFields = false;
	ExistsField currentExistsField = null;
	List<String> currentExistsSubfields = null;
	List<String> currentExistsMasterSubfields = new ArrayList<String>();
	Map<String, String> currentExistsMasterSubfieldsValues = new HashMap<String, String>();
	private boolean existsValueRequired = false;
	String existsOperatorToUse = null;
	Set<String> existsSubfieldsInDatafield = new HashSet<String>();
	List<String> existsSolrFieldnames = new ArrayList<String>();
	boolean skip = false;
	
	// Variables for allfields:
	private boolean hasAllFieldsField = false;
	private String allfieldsField = null;
	private List<String> allFieldsExceptions = new ArrayList<String>();

	// Variable for fullrecord
	private boolean getFullRecordAsXML = false;
	private String fullrecordField = null;
	private String fullrecordXmlString = null;
	// ################ OLD #################
	
	
	// ################ NEW #################
	private Controlfield controlfield;
	private ArrayList<Controlfield> controlfields;
	private Datafield datafield;
	private ArrayList<Datafield> datafields;
	private Subfield subfield;
	private ArrayList<Subfield> subfields;
	// ################ NEW #################


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

			// Concatenated subfields
			if (mo.hasConcatenatedSubfields()) {

				Set<String> concatenatedMasterFields = new HashSet<String>();
				LinkedHashMap<Integer, String> concatenatedSubfields = mo.getConcatenatedSubfields();

				for (Entry<Integer, String> concatenatedSubfield : concatenatedSubfields.entrySet()) {

					List<String> immutableList = Arrays.asList(concatenatedSubfield.getValue().split("\\s*:\\s*"));
					List<String> mutableList = new ArrayList<String>();
					mutableList.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
					int lastListElement = (mutableList.size()-1); // Get index of last List element (= separator)
					boolean isTranslateConcatenatedSubfields = mo.isTranslateConcatenatedSubfields();
					Map<String, String> translateConcatenatedSubfieldsProperties = mo.getTranslateConcatenatedSubfieldsProperties();

					// Get all master fields:
					for (Entry<String, List<String>> mabfieldName : mo.getMabFieldnames().entrySet()) {
						String completeFieldname = mabfieldName.getKey().toString();
						String concatenatedMasterDatafield = completeFieldname.substring(0,3); // Get e. g. "655" out of "655$e*$u"
						String concatenatedMasterSubfield = completeFieldname.substring(completeFieldname.length() - 1); // Get last character which should be the subfield code, e. g. "u" out of "655$e*$u"
						concatenatedMasterFields.add(concatenatedMasterDatafield+concatenatedMasterSubfield);
					}

					String concatenatedFieldsSeparator = mutableList.get(lastListElement); // Last value is always the separator to use
					mutableList.remove(lastListElement); // Remove the separator value so that only the subfield codes will remain

					ConcatenatedField newConcatenatedField = new ConcatenatedField(concatenatedMasterFields, mutableList, concatenatedFieldsSeparator, isTranslateConcatenatedSubfields, translateConcatenatedSubfieldsProperties);					
					concatenatedFields.add(newConcatenatedField);
				}
			}

			// Subfield exists / not exists
			if (mo.hasSubfieldExists() || mo.hasSubfieldNotExists()) {

				isSubfieldExists = mo.hasSubfieldExists();
				isSubfieldNotExists = mo.hasSubfieldNotExists();
				LinkedHashMap<Integer, String> subfieldExists = null;
				Set<String> subfieldExistsMasterFields = new HashSet<String>();
				String subfieldExistsOpearator = null;
				
				existsSolrFieldnames.add(mo.getSolrFieldname());
				
				if (isSubfieldExists) {
					subfieldExists = mo.getSubfieldExists();
				} else if (isSubfieldNotExists) {
					subfieldExists = mo.getSubfieldNotExists();
				}

				for (Entry<Integer, String> entry : subfieldExists.entrySet()) {
					List<String> immutableList = Arrays.asList(entry.getValue().split("\\s*:\\s*"));
					List<String> mutableList = new ArrayList<String>();

					if (immutableList.size() == 1) {
						// If only one subfield is in the list, get that one. There should not be an operator (AND or OR)
						mutableList.add(immutableList.get(0));
						subfieldExistsOpearator = "AND"; // Default operator is AND
					} else {
						mutableList.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
						int lastListElement = (mutableList.size()-1); // Get index of last List element (= operator (AND or OR))
						subfieldExistsOpearator = mutableList.get(lastListElement); // Last value is always the operator (AND or OR) to use

						if (subfieldExistsOpearator.equals("AND") || subfieldExistsOpearator.equals("OR")) {
							mutableList.remove(lastListElement); // Remove the operator (AND or OR) so that only the subfield codes will remain
						} else {
							subfieldExistsOpearator = "AND"; // Default operator is AND.
							// We do not remove the last list element here because it is not AND or OR and therfore it could be a subfield code that we should use.
						}
					}

					// Get all master fields:
					for (Entry<String, List<String>> mabfieldName : mo.getMabFieldnames().entrySet()) {
						String completeFieldname = mabfieldName.getKey().toString(); // E. g. "655$e*$u"
						String subfieldExistsMasterDatafield = completeFieldname.substring(0,3); // Get e. g. "655" out of "655$e*$u"
						String subfieldExistsMasterSubfield = completeFieldname.substring(completeFieldname.length() - 1); // Get last character which should be the subfield code, e. g. "u" out of "655$e*$u"
						subfieldExistsMasterFields.add(subfieldExistsMasterDatafield+subfieldExistsMasterSubfield);
					}

					ExistsField newExistsField = new ExistsField(subfieldExistsMasterFields, mutableList, subfieldExistsOpearator, isSubfieldExists, isSubfieldNotExists, existsSolrFieldnames);
										
					existsFields.add(newExistsField);
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
			// ################ OLD #################
			/*
			allFields = new ArrayList<Mabfield>();
			record = new Record();
			fullrecordXmlString = null; // Reset for new record
			if (getFullRecordAsXML) {
				fullrecordXmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><collection><record>"; // Begin new XML for the current record
			}
			*/
			// ################ OLD #################
			
			
			// ################ NEW #################
			allFields = new ArrayList<Mabfield>();
			record = new Record(); // A new record
			controlfields = new ArrayList<Controlfield>(); // All controlfields of the record
			datafields = new ArrayList<Datafield>(); // All datafields of the record
			fullrecordXmlString = null; // Reset for new record
			if (getFullRecordAsXML) {
				fullrecordXmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><collection><record>"; // Begin new XML for the current record
			}
			// ################ NEW #################
		}

		// Parser encounters the start of the "leader"-tag (only necessary if we need to get the full record as XML)
		if (getFullRecordAsXML) {
			if(localName.equals("leader")) {
				fullrecordXmlString += "<leader>";
			}
		}

		// If the parser encounters the start of a "controlfield"-tag, create a new mabfield-object, get the XML-attributes, set them on the object and add it to the "record"-object:
		if(localName.equals("controlfield")) {
			// ################ OLD #################
			/*
			controlfieldTag = attribs.getValue("tag");
			isSYS = (controlfieldTag.equals("SYS")) ? true : false;
			mabfieldControlfield = new Mabfield();
			mabfieldControlfield.setFieldname(controlfieldTag);
			is001Controlfield = (controlfieldTag.equals("001")) ? true : false;
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<controlfield tag=\""+controlfieldTag+"\">";
			}
			*/
			// ################ OLD #################
			
			
			// ################ NEW #################
			String tag = attribs.getValue("tag").trim();
			controlfield = new Controlfield();
			controlfield.setTag(tag);
			isSYS = (tag.equals("SYS")) ? true : false;
			is001Controlfield = (tag.equals("001")) ? true : false;
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<controlfield tag=\""+tag+"\">";
			}
			// ################ NEW #################
		}

		if(localName.equals("datafield")) {
			// ################ OLD #################
			allSubfieldsInDatafield = new ArrayList<Mabfield>();
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
						currentConcatenatedMasterSubfields.add(concatenatedMasterFieldSubfield); // TODO: Could currentConcatenatedMasterSubfields be a Set to avoid duplicates?
						currentConcatenatedField = concatenatedField;
						currentConcatenatedSubfields = currentConcatenatedField.getConcatenatedSubfields();
					}
				}
			}

			// Subfield exists
			for (ExistsField existsField : existsFields) {
				for (String existsMasterField : existsField.getExistsMasterFields()) {
					String existsMasterFieldName = existsMasterField.substring(0,3);
					String existsMasterFieldSubfield = existsMasterField.substring(existsMasterField.length() - 1);

					if (existsMasterFieldName.equals(datafieldTag)) {
						datafieldContainsExistsFields = true;
						currentExistsMasterSubfields.add(existsMasterFieldSubfield); // TODO: Could currentExistsMasterSubfields be a Set to avoid duplicates?
						currentExistsField = existsField;						
						currentExistsSubfields = currentExistsField.getExistsSubfields();
					}
				}
			}
			// ################ OLD #################
			
			
			// ################ NEW #################
			String tag = attribs.getValue("tag").trim();
			String ind1 = attribs.getValue("ind1").trim();
			String ind2 = attribs.getValue("ind2").trim();

			// Set empty tags and indicators to a character, so that the string to match against is always
			// of the same length, e. g. "311$ab$c" and "000$**$*". This prevents errors.
			tag = (tag != null && !tag.isEmpty()) ? tag : "000";
			ind1 = (ind1 != null && !ind1.isEmpty()) ? ind1 : "*";
			ind2 = (ind2 != null && !ind2.isEmpty()) ? ind2 : "*";
			
			datafield = new Datafield();
			datafield.setTag(tag);
			datafield.setInd1(ind1);
			datafield.setInd2(ind2);
			
			subfields = new ArrayList<Subfield>(); // All subfields of the datafield
			
			is001Datafield = (tag.equals("001")) ? true : false;
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<datafield tag=\""+tag+"\" ind1=\""+ind1+"\" ind2=\""+ind2+"\">";
			}
			// ################ NEW #################

		}


		if(localName.equals("subfield")) {
			// ################ OLD #################
			subfieldCode = attribs.getValue("code").trim();
			subfieldCode = (subfieldCode != null && !subfieldCode.isEmpty()) ? subfieldCode : "-";
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<subfield code=\""+subfieldCode+"\">";
			}
			String datafieldName = datafieldTag + "$" + datafieldInd1 + datafieldInd2 + "$" + subfieldCode;

			// Create a new mabfield so that we can concentenate a datafield and a subfield to a mabfield
			// E. g.: Datafield = 100$**, Subfield = r, Subfield content = AC123456789
			//        Result: mabfield name = 100$**$r, mabfield value = AC123456789
			mabfield = new Mabfield();
			mabfield.setFieldname(datafieldName);
			// ################ OLD #################
			
			
			// ################ NEW #################
			String code = attribs.getValue("code").trim();
			code = (code != null && !code.isEmpty()) ? code : "-";
			subfield = new Subfield();
			subfield.setCode(code);
			if (getFullRecordAsXML) {
				fullrecordXmlString += "<subfield code=\""+code+"\">";
			}
			// ################ NEW #################
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

		// ################ NEW #################
		String content = nodeContent.toString();
		// ################ NEW #################
		
		// Parser encounters the start of the "leader"-tag (only necessary if we need to get the full record as XML)
		if (getFullRecordAsXML) {
			if(localName.equals("leader")) {
				String leaderText = nodeContent.toString();
				fullrecordXmlString += StringEscapeUtils.escapeXml10(leaderText)+"</leader>";
			}
		}

		
		if(localName.equals("controlfield") ) {
			// ################ OLD #################
			/*
			String controlfieldText = nodeContent.toString();
			mabfieldControlfield.setFieldvalue(controlfieldText);
			allFields.add(mabfieldControlfield);
			if (getFullRecordAsXML) {
				fullrecordXmlString += StringEscapeUtils.escapeXml10(controlfieldText)+"</controlfield>";
			}
			if (isSYS == true) {
				recordSYS = controlfieldText;
			}
			if (is001Controlfield == true && is001Datafield == false) {
				recordID = controlfieldText;
			}
			*/
			// ################ OLD #################
			
			// ################ NEW #################
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
			// ################ NEW #################
		}
		

		if(localName.equals("subfield")) {

			String subfieldText = nodeContent.toString();
			if (getFullRecordAsXML) {
				fullrecordXmlString += StringEscapeUtils.escapeXml10(subfieldText).trim()+"</subfield>";
			}


			if (datafieldContainsConnectedFields) { // There could be a connected subfield within the datafield, but we don't know yet if it really exists. For that, we have to wait for the end of the datafield tag in endElement().
				if (currentMasterSubfields.contains(subfieldCode)) { // If it is one of the connected master subfields
					connectedValueRequired = true;
					currentMasterSubfieldsValues.put(subfieldCode, subfieldText); // Add subfield code and text to an intermediate Map (a Map because there could be multiple master subfields)
				} else { // Do the default operation
					mabfield.setFieldvalue(subfieldText);
					allSubfieldsInDatafield.add(mabfield);
					connectedSubfieldsInDatafield.put(subfieldCode, subfieldText);
				}


			} else if (datafieldContainsConcatenatedFields) { // There could be a concatenated subfield within the datafield, but we don't know yet if it really exists.
				if (currentConcatenatedMasterSubfields.contains(subfieldCode)) { // If it is one of the concatenated master subfields
					concatenatedValueRequired = true;
					currentConcatenatedMasterSubfieldsValues.put(subfieldCode, subfieldText); // Add subfield code and text to an intermediate Map (a Map because there could be multiple master subfields)
				} else { // Do the default operation
					mabfield.setFieldvalue(subfieldText);
					allSubfieldsInDatafield.add(mabfield);
					concatenatedSubfieldsInDatafield.put(subfieldCode, subfieldText);
				}
			}

			if (datafieldContainsExistsFields) { // There could be a subfield within the datafield that we need to check for existance (subfieldExists, subfieldNotExists), but we don't know yet if it really exists.
				if (currentExistsMasterSubfields.contains(subfieldCode)) { // If it is one of the master subfields
					existsValueRequired = true;
					currentExistsMasterSubfieldsValues.put(subfieldCode, subfieldText); // Add subfield code and text to an intermediate Map (a Map because there could be multiple master subfields)
				} else { // Do the default operation
					existsSubfieldsInDatafield.add(subfieldCode);
				}
			}

			// Default operation - no connected, concatenated or exists value
			if (!datafieldContainsConnectedFields && !datafieldContainsConcatenatedFields && !datafieldContainsExistsFields) {
				mabfield.setFieldvalue(subfieldText);
				allSubfieldsInDatafield.add(mabfield);
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
					String currentConnectedMasterSubfieldCode = currentMasterSubfieldsEntry.getKey();
					String currentConnectedMasterSubfieldText = currentMasterSubfieldsEntry.getValue();

					String datafieldName = datafieldTag + "$" + datafieldInd1 + datafieldInd2 + "$" + currentConnectedMasterSubfieldCode;
					Mabfield connectedDatafield = new Mabfield();
					connectedDatafield.setFieldname(datafieldName);
					connectedDatafield.setFieldvalue(currentConnectedMasterSubfieldText);					

					for (Entry<Integer, Map<String, List<String>>> entry1 : currentConnectedSubfields.entrySet()) {
						Map<String, List<String>> entrySet2 = entry1.getValue();

						for (Entry<String, List<String>> entry2 : entrySet2.entrySet()) {
							String defaultValue = entry2.getKey();
							List<String> bracketValues = entry2.getValue();
							String subfieldText = null;
							String textToUse = null;

							// Add connected value if it exists
							for (String bracketValue : bracketValues) {
								subfieldText = connectedSubfieldsInDatafield.get(bracketValue);
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

					// Check if field should be skiped or not (subfieldExists/subfieldNotExists):
					if (datafieldContainsExistsFields && existsValueRequired) {
						skip = this.skipField(currentExistsField, existsSubfieldsInDatafield, currentExistsMasterSubfields, currentConnectedMasterSubfieldCode);
					}
					connectedDatafield.setSkip(skip); // Set if field should be skiped or not
					if (skip) {
						connectedDatafield.setSolrFieldnames(currentExistsField.getSolrFieldnames()); // Set Solr fieldname to be sure that only the right field gets skiped.
					}
					
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

					String datafieldName = datafieldTag + "$" + datafieldInd1 + datafieldInd2 + "$" + currentConcatenatedMasterSubfieldCode;
					Mabfield concatenatedDatafield = new Mabfield();
					concatenatedDatafield.setFieldname(datafieldName);
					concatenatedDatafield.setFieldvalue(currentConcatenatedMasterSubfieldText);

					for (String currentConcatenatedSubfield : currentConcatenatedSubfields) {
						subfieldText = concatenatedSubfieldsInDatafield.get(currentConcatenatedSubfield);
						if (subfieldText != null && subfieldText.trim() != "") {
							concatenatedSubfieldValues.add(subfieldText);
						}
					}

					if (!concatenatedSubfieldValues.isEmpty()) {
						concatenatedValuesSeparatorToUse = (currentConcatenatedField.getConcatenatedFieldsSeparator() != "") ? currentConcatenatedField.getConcatenatedFieldsSeparator() : ", ";
						concatenatedValuesToUse = concatenatedSubfieldValues;
					}

					if (!concatenatedValuesToUse.isEmpty()) {
						concatenatedDatafield.setConcatenatedValues(concatenatedValuesToUse);
						concatenatedDatafield.setConcatenatedSeparator(concatenatedValuesSeparatorToUse);
						concatenatedDatafield.setTranslateConcatenatedSubfields(currentConcatenatedField.isTranslateConcatenatedSubfields());
						concatenatedDatafield.setTranslateConcatenatedSubfieldsProperties(currentConcatenatedField.getTranslateConcatenatedSubfieldsProperties());
					}					

					// Check if field should be skiped or not (subfieldExists/subfieldNotExists):
					if (datafieldContainsExistsFields && existsValueRequired) {
						skip = this.skipField(currentExistsField, existsSubfieldsInDatafield, currentExistsMasterSubfields, currentConcatenatedMasterSubfieldCode);
					}
					concatenatedDatafield.setSkip(skip); // Set if field should be skiped or not
					if (skip) {
						concatenatedDatafield.setSolrFieldnames(currentExistsField.getSolrFieldnames()); // Set Solr fieldname to be sure that only the right field gets skiped.
					}
					
					allFields.add(concatenatedDatafield);

					concatenatedDatafield = null;
				}
			}

			// Set all other datafields:
			for (Mabfield subfieldInDatafield : allSubfieldsInDatafield) {
				
				// Check if field should be skiped or not (subfieldExists/subfieldNotExists): 100$ab$c
				if (datafieldContainsExistsFields && existsValueRequired) {
					String masterSubfield = subfieldInDatafield.getFieldname().substring(7, 8); // Get subfield code
					skip = this.skipField(currentExistsField, existsSubfieldsInDatafield, currentExistsMasterSubfields, masterSubfield);
					
					if (skip) {
						subfieldInDatafield.setSolrFieldnames(currentExistsField.getSolrFieldnames()); // Set Solr fieldname to be sure that only the right field gets skiped.
					}
				}
				subfieldInDatafield.setSkip(skip);
				
				
				allFields.add(subfieldInDatafield);
			}


			// Reset values for connected fields:
			datafieldContainsConnectedFields = false;
			connectedValueRequired = false;
			currentConnectedField = null;
			currentConnectedSubfields = null;
			currentMasterSubfields = new ArrayList<String>();
			currentMasterSubfieldsValues = new HashMap<String, String>();
			connectedValuesToUse = new ArrayList<String>();
			connectedSubfieldsInDatafield = new HashMap<String, String>();

			// Reset values for concatenated fields:
			datafieldContainsConcatenatedFields = false;
			concatenatedValueRequired = false;
			currentConcatenatedField = null;
			currentConcatenatedSubfields = null;
			currentConcatenatedMasterSubfields = new ArrayList<String>();
			currentConcatenatedMasterSubfieldsValues = new HashMap<String, String>();
			concatenatedValuesToUse = new ArrayList<String>();
			concatenatedValuesSeparatorToUse = null;
			concatenatedSubfieldsInDatafield = new HashMap<String, String>();

			// Reset values for "exists" fields:
			datafieldContainsExistsFields = false;
			existsValueRequired = false;
			currentExistsField = null;
			currentExistsSubfields = null;
			currentExistsMasterSubfields = new ArrayList<String>();
			currentExistsMasterSubfieldsValues = new HashMap<String, String>();
			existsOperatorToUse = null;
			existsSubfieldsInDatafield = new HashSet<String>();
			isSubfieldExists = false;
			isSubfieldNotExists = false;
			skip = false;
			
			// Reset general variables:
			allSubfieldsInDatafield = null;
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
					List<String> concatValues = mf.getConcatenatedValues();					

					// Add the concatenated value(s) to the document if one exists:
					if (concatValues != null && !concatValues.isEmpty()) {
						String separator = mf.getConcatenatedSeparator();
						String concatValue = StringUtils.join(concatValues, separator); // Join concatenated value(s) with the given separator character
						String valueToAdd = fieldValue + separator + concatValue; // Add the standard field value in front of the concatenated value(s), separated by the given separator character
						doc.addField(fieldName, valueToAdd);
					} else {
						// Add fieldname and standard field value to the document, but only if we do not have a concatenated value.
						// If we would do that, we would add the standard field value AND the concatenated value (but the standard field
						// value is already included there). This is the difference to the connected value(s):
						doc.addField(fieldName, fieldValue);

						// Add the connected value(s) additionally to the standard field value (this is the difference to the concatenated values):
						if (connValues != null && !connValues.isEmpty()) {
							for (String connValue : connValues) {
								doc.addField(fieldName, connValue);
							}
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


	// Check if field should be skiped or not (subfieldExists/subfieldNotExists): 
	private boolean skipField(ExistsField currentExistsField, Set<String> existsSubfieldsInDatafield, List<String> currentExistsMasterSubfields, String currentMasterSubfieldCode) {
		
		boolean skip = false;
				
		if (currentExistsMasterSubfields.contains(currentMasterSubfieldCode)) {	
			
			String existsOperator = currentExistsField.getExistsOperator();
			List<String> existsSubfields = currentExistsField.getExistsSubfields();
	
			if (existsOperator.equals("AND")) {
				// Set existsSubfieldsInDatafield contains all elements from list existsSubfields
				if (currentExistsField.isSubfieldExists() && !existsSubfieldsInDatafield.containsAll(existsSubfields)) {
					//System.out.println("Exists, AND: " + existsSubfieldsInDatafield + " contains not all " + existsSubfields);
					skip = true;
				} else if (currentExistsField.isSubfieldNotExists() && existsSubfieldsInDatafield.containsAll(existsSubfields)) { // OK
					//System.out.println("Exists Not, AND: " + existsSubfieldsInDatafield + " contains all " + existsSubfields);
					skip = true;
				}
			} else if (existsOperator.equals("OR")) {
				// Set existsSubfieldsInDatafield contains no element from list existsSubfields
				if (currentExistsField.isSubfieldExists() && Collections.disjoint(existsSubfieldsInDatafield, existsSubfields)) {
					//System.out.println("Exists, OR: " + existsSubfieldsInDatafield + " has not elements of " + existsSubfields);
					skip = true;
				} else if (currentExistsField.isSubfieldNotExists() && !Collections.disjoint(existsSubfieldsInDatafield, existsSubfields)) {
					//System.out.println("Exists Not, OR: " + existsSubfieldsInDatafield + " has elements of " + existsSubfields);
					skip = true;
				}
			}
		}
				
		return skip;
	}

}