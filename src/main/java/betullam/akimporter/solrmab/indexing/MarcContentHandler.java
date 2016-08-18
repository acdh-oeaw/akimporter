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
import java.util.HashSet;
import java.util.List;
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
<<<<<<< HEAD
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
	LinkedHashSet<ExistsField> currentExistsFields = new LinkedHashSet<ExistsField>();
	List<String> currentExistsSubfields = null;
	List<String> currentExistsMasterSubfields = new ArrayList<String>();
	Map<String, String> currentExistsMasterSubfieldsValues = new HashMap<String, String>();
	private boolean existsValueRequired = false;
	String existsOperatorToUse = null;
	Set<String> existsSubfieldsInDatafield = new HashSet<String>();
	List<String> existsSolrFieldnames = new ArrayList<String>();
	boolean skip = false;
=======
	private int counter = 0;
	private String timeStamp;
	private int NO_OF_DOCS = 500;
>>>>>>> refs/remotes/origin/SubfieldExists2-NewDatafieldConcept
	
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
	 * @param propertiesObjects		List<PropertiesObject>. A PropertiesObject contains information about matching raw MarcXML fields to Solr fields.
	 * @param solrServer			SolrServer object that represents the Solr server to which the data should be indexed 
	 * @param timeStamp				String that specifies the starting time of the importing process
	 * @param print					boolean. True if status messages should be printed to the console.
	 */
	public MarcContentHandler(List<PropertiesObject> propertiesObjects, SolrServer solrServer, String timeStamp, boolean print) {
		this.propertiesObjects = propertiesObjects;
		this.sServer = solrServer;
		this.timeStamp = timeStamp;
		this.print = print;

<<<<<<< HEAD
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
				Set<String> subfieldExistsMasterTags = new HashSet<String>();
				Set<String> subfieldExistsMasterSubfields = new HashSet<String>();
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
						String subfieldExistsMasterTag = completeFieldname.substring(0,3); // Get e. g. "655" out of "655$e*$u"
						String subfieldExistsMasterSubfield = completeFieldname.substring(completeFieldname.length() - 1); // Get last character which should be the subfield code, e. g. "u" out of "655$e*$u"
						subfieldExistsMasterFields.add(subfieldExistsMasterTag+subfieldExistsMasterSubfield);
						subfieldExistsMasterTags.add(subfieldExistsMasterTag);
						subfieldExistsMasterSubfields.add(subfieldExistsMasterSubfield);
					}

					ExistsField newExistsField = new ExistsField(subfieldExistsMasterFields, subfieldExistsMasterTags, subfieldExistsMasterSubfields, mutableList, subfieldExistsOpearator, isSubfieldExists, isSubfieldNotExists, existsSolrFieldnames);
										
					existsFields.add(newExistsField);
				}
			}

=======
		for (PropertiesObject mo : propertiesObjects) {
>>>>>>> refs/remotes/origin/SubfieldExists2-NewDatafieldConcept

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

		// Parser encounters the start of the "leader"-tag (only necessary if we need to get the full record as XML)
		if (getFullRecordAsXML) {
			if(localName.equals("leader")) {
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
<<<<<<< HEAD

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

=======
>>>>>>> refs/remotes/origin/SubfieldExists2-NewDatafieldConcept
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

		// Parser encounters the end of the "leader"-tag (only necessary if we need to get the full record as XML)
		if (getFullRecordAsXML) {
			if(localName.equals("leader")) {
				String leaderText = nodeContent.toString();
				fullrecordXmlString += StringEscapeUtils.escapeXml10(leaderText)+"</leader>";
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
<<<<<<< HEAD


			if (datafieldContainsConnectedFields) { // There could be a connected subfield within the datafield, but we don't know yet if it really exists. For that, we have to wait for the end of the datafield tag in endElement().
				if (currentMasterSubfields.contains(subfieldCode)) { // If it is one of the connected master subfields
					connectedValueRequired = true;
					currentMasterSubfieldsValues.put(subfieldCode, subfieldText); // Add subfield code and text to an intermediate Map (a Map because there could be multiple master subfields)
				} else { // Do the default operation
					datafield.setFieldvalue(subfieldText);
					allSubfieldsInDatafield.add(datafield);
					connectedSubfieldsInDatafield.put(subfieldCode, subfieldText);
				}


			} else if (datafieldContainsConcatenatedFields) { // There could be a concatenated subfield within the datafield, but we don't know yet if it really exists.
				if (currentConcatenatedMasterSubfields.contains(subfieldCode)) { // If it is one of the concatenated master subfields
					concatenatedValueRequired = true;
					currentConcatenatedMasterSubfieldsValues.put(subfieldCode, subfieldText); // Add subfield code and text to an intermediate Map (a Map because there could be multiple master subfields)
				} else { // Do the default operation
					datafield.setFieldvalue(subfieldText);
					allSubfieldsInDatafield.add(datafield);
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
				
				for (ExistsField ef : existsFields) {
					if (ef.getExistsMasterSubfields().contains(subfieldCode)) {
						currentExistsFields.add(ef);
					}
				}
				
			}

			// Default operation - no connected, concatenated or exists value
			if (!datafieldContainsConnectedFields && !datafieldContainsConcatenatedFields && !datafieldContainsExistsFields) {
				datafield.setFieldvalue(subfieldText);
				allSubfieldsInDatafield.add(datafield);
			}

=======
>>>>>>> refs/remotes/origin/SubfieldExists2-NewDatafieldConcept
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
<<<<<<< HEAD


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
				
				for (ExistsField exf : currentExistsFields) {
					System.out.println(subfieldInDatafield.getFieldname() + " (" + subfieldInDatafield.getFieldvalue() + "): " + exf.getExistsMasterSubfields());
				}
				
				// Check if field should be skiped or not (subfieldExists/subfieldNotExists): 100$ab$c
				if (datafieldContainsExistsFields && existsValueRequired) {
					
					//System.out.println(subfieldInDatafield + ": " + currentExistsField);
					
					String masterSubfield = subfieldInDatafield.getFieldname().substring(7, 8); // Get subfield code
					skip = this.skipField(currentExistsField, existsSubfieldsInDatafield, currentExistsMasterSubfields, masterSubfield);
					
					if (skip) {
						subfieldInDatafield.setSolrFieldnames(currentExistsField.getSolrFieldnames()); // Set Solr fieldname to be sure that only the right field gets skiped.
					}
				}
				subfieldInDatafield.setSkip(skip);
				
				
				allFields.add(subfieldInDatafield);
			}
			
			if (datafieldContainsExistsFields && existsValueRequired) {
				
				System.out.println("--------------------------- DATAFIELD END ------------------------");
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
			currentExistsFields = new LinkedHashSet<ExistsField>();
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

=======
>>>>>>> refs/remotes/origin/SubfieldExists2-NewDatafieldConcept
		}

		// If the parser encounters the end of the "record"-tag, add all controlfield-objects, datafield-objects and some other information
		// to the rawRecord object and add the it to the list of all raw records
		if(localName.equals("record")) {

			counter = counter + 1;	
			rawRecord.setRecordID(recordID);
			rawRecord.setRecordSYS(recordSYS);
			rawRecord.setIndexTimestamp(timeStamp);
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

				// Add to Solr-Index:
				this.solrAddRecordSet(sServer, solrRecords);

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
		this.solrAddRecordSet(sServer, solrRecords);

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
				doc.addField("indexTimestamp_str", solrRecord.getIndexTimestamp());

				// Add the allfields field to the document if it is used
				if (hasAllFieldsField) {
					doc.addField(allfieldsField, allfieldsSet);
				}
				
				// Add the fullRecord field to the document if it is used
				if (getFullRecordAsXML) {
					doc.addField(fullrecordField, fullrecordXmlString);
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
	 * @param print		boolean\t true if the text should be print
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