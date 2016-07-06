/**
 * Matching MAB fields to respective Solr fields.
 * 
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.java.betullam.akimporter.solrmab.Index;

public class MatchingOperations {

	private List<Mabfield> listOfMatchedFields;
	private List<Record> newListOfRecords;
	private List<List<Mabfield>> listOfNewSolrfieldLists;
	private List<Mabfield> listOfNewFieldsForNewRecord;
	private Record newRecord;
	List<String> multiValuedFields = Index.multiValuedFields;
	List<Mabfield> customTextFields = Index.customTextFields;
	Map<String, List<String>> translateFields = Index.translateFields;
	String fullRecordFieldname = Index.fullrecordFieldname;


	/**
	 * This re-works a MarcXML record to records we can use for indexing to Solr.
	 * The matching between MAB fields and Solr fields is handled here.
	 * 
	 * @param oldRecordSet			List<Record>: A set of "old" MarcXML records
	 * @param listOfMatchingObjs	List<MatchingObject>: A list of rules that define how a MAB field should be matched with a Solr field.
	 * 								The rules are defined in the mab.properties file.
	 * @return						List<Record>: A set of "new" records we can use for indexing to Solr
	 */
	public List<Record> matching(List<Record> oldRecordSet, List<MatchingObject> listOfMatchingObjs) {

		newListOfRecords = new ArrayList<Record>();		

		for (Record oldRecord : oldRecordSet) {			

			String fullRecordAsXml = null;
			listOfNewSolrfieldLists = new ArrayList<List<Mabfield>>();
			listOfNewFieldsForNewRecord = new ArrayList<Mabfield>();
			newRecord = new Record();			

			for (Mabfield oldMabField : oldRecord.getMabfields()) {
				if (!oldMabField.getFieldname().equals(fullRecordFieldname)) {
					// Match each Mabfield in the Record to the defined Solrfields. As one old Mabfield could match multiple Solrfields, we get back a List of Fields:
					List<Mabfield> listOfSolrfieldsForMabfield = this.matchField(oldMabField, listOfMatchingObjs);
					listOfNewSolrfieldLists.add(listOfSolrfieldsForMabfield);
				} else {
					// Get full record as XML
					fullRecordAsXml = oldMabField.getFieldvalue();
				}
			}

			for (List<Mabfield> newSolrFields : listOfNewSolrfieldLists) {
				for (Mabfield newSolrField : newSolrFields) {
					listOfNewFieldsForNewRecord.add(newSolrField);
				}
			}

			for (Mabfield customTextField : customTextFields) {
				listOfNewFieldsForNewRecord.add(customTextField);
			}

			// DeDuplication of none-multivalued SolrFields:
			HashSet<Mabfield> dedupSolrSet = new HashSet<Mabfield>();
			List<Mabfield> dedupSolrList = new ArrayList<Mabfield>();
			List<String> fieldNamesInDedupSolrSet = new ArrayList<String>();
			for (Mabfield solrfield : listOfNewFieldsForNewRecord) {
				if (multiValuedFields.contains(solrfield.getFieldname()) == false) { // If its a single valued SolrField!

					// Make a list of all fieldvalues that are already added, so we can compare to it in the next step:
					for (Mabfield mabfield : dedupSolrSet) {
						fieldNamesInDedupSolrSet.add(mabfield.getFieldname());
					}

					// If there is not yet a SolrField with that fieldname, add it, but just one!
					if (fieldNamesInDedupSolrSet.contains(solrfield.getFieldname()) == false) {
						dedupSolrSet.add(solrfield);
					}

				} else { // Multi valued SolrField					
					if (solrfield.isAllowDuplicates()) { // Allow duplicates - necessary for connected fields
						dedupSolrList.add(solrfield); // Add directly to the list to allow duplicates
					} else {
						dedupSolrSet.add(solrfield); // Add to a set to avoid duplicates in multivalued fields
					}
				}
			}

			// Add Set<Mabfield> (deduplicated mulitvalued fields) to a List<Mabfield> which we have to use as type for "mabfields" variable
			// in Record object, because when getting MAB-Codes from XML file, "mabfields" variable should be able to contain duplicates.
			dedupSolrList.addAll(dedupSolrSet);

			// Add full record as XML
			if (fullRecordFieldname != null && !fullRecordFieldname.isEmpty()) {
				Mabfield fullrecord = new Mabfield();
				fullrecord.setFieldname(fullRecordFieldname);
				fullrecord.setFieldvalue(fullRecordAsXml);
				dedupSolrList.add(fullrecord);
			}

			// Sort by fieldName (better readibility in Solr admin console):
			Collections.sort(dedupSolrList); 

			// Set variables of new record object:
			newRecord.setMabfields(dedupSolrList);
			newRecord.setRecordID(oldRecord.getRecordID());
			newRecord.setIndexTimestamp(oldRecord.getIndexTimestamp());

			newListOfRecords.add(newRecord);
		}


		return newListOfRecords;
	}



	/**
	 * Matching a MAB field to the Solr field according to the rules in mab.properties.
	 * 
	 * @param mabField				The MAB field from the MarcXML record
	 * @param listOfMatchingObjects	The rules from the mab.properties file
	 * @return						A list of matched fields for indexing to Solr.
	 */
	public List<Mabfield> matchField(Mabfield mabField, List<MatchingObject> listOfMatchingObjects) {		

		String mabFieldnameXml = mabField.getFieldname();
		listOfMatchedFields = new ArrayList<Mabfield>();

		// Remove unnecessary MatchingObjects so that we don't have to iterate over all of them. This saves a lot of time!
		// A MatchingObject is not necessary if it does not contain the MAB-fieldnumber from the parst MarcXML record.
		List<MatchingObject> listOfRelevantMatchingObjects = new ArrayList<MatchingObject>();
		for (MatchingObject matchingObject : listOfMatchingObjects) {
			String mabFieldNo = mabFieldnameXml.substring(0, 3);
			boolean containsMabFieldNo = matchingObject.getMabFieldnames().keySet().toString().contains(mabFieldNo);

			if (containsMabFieldNo) {
				listOfRelevantMatchingObjects.add(matchingObject);
			}
		}

		// Connected subfields
		List<String> connectedSubfieldValues = null;
		if (mabField.getConnectedValues() != null && !mabField.getConnectedValues().isEmpty()) {
			connectedSubfieldValues = mabField.getConnectedValues();
			if (mabField.isTranslateConnectedSubfields()) {
				// Treat translate values for connected subfields
				Map<String, String> translateSubfieldsProperties = null;
				String defaultConnectedValue = null;
				if (mabField.isTranslateConnectedSubfields()) {
					translateSubfieldsProperties = mabField.getTranslateSubfieldsProperties();
					List<String> translatedConnectedSubfieldValues = new ArrayList<String>();
					for (String connectedSubfieldValue : connectedSubfieldValues) {
						defaultConnectedValue = connectedSubfieldValue;
						Mabfield mfForTranslation = new Mabfield();
						mfForTranslation.setFieldname(mabFieldnameXml);
						mfForTranslation.setFieldvalue(connectedSubfieldValue);
						String translatedConnectedSubfieldValue = getTranslatedValue(mabFieldnameXml, mfForTranslation, translateSubfieldsProperties, "all", "all", defaultConnectedValue, true, false, false, true, false, null, false, null, false, null, null);
						translatedConnectedSubfieldValues.add(translatedConnectedSubfieldValue);
					}
					connectedSubfieldValues.clear();
					connectedSubfieldValues.addAll(translatedConnectedSubfieldValues);
				}	
			}
		}

		// Concatenated subfields
		List<String> concatenatedSubfieldValues = null;
		if (mabField.getConcatenatedValues() != null && !mabField.getConcatenatedValues().isEmpty()) {
			concatenatedSubfieldValues = mabField.getConcatenatedValues();
			if (mabField.isTranslateConcatenatedSubfields()) {
				// Treat translate values for concatenated subfields
				Map<String, String> translateConcatenatedSubfieldsProperties = null;
				String defaultConcatenatedValue = null;
				if (mabField.isTranslateConcatenatedSubfields()) {
					translateConcatenatedSubfieldsProperties = mabField.getTranslateConcatenatedSubfieldsProperties();
					List<String> translatedConcatenatedSubfieldValues = new ArrayList<String>();
					for (String concatenatedSubfieldValue : concatenatedSubfieldValues) {
						defaultConcatenatedValue = concatenatedSubfieldValue;
						Mabfield mfForTranslation = new Mabfield();
						mfForTranslation.setFieldname(mabFieldnameXml);
						mfForTranslation.setFieldvalue(concatenatedSubfieldValue);
						String translatedConcatenatedSubfieldValue = getTranslatedValue(mabFieldnameXml, mfForTranslation, translateConcatenatedSubfieldsProperties, "all", "all", defaultConcatenatedValue, true, false, false, false, false, null, false, null, false, null, null);
						translatedConcatenatedSubfieldValues.add(translatedConcatenatedSubfieldValue);
					}
					concatenatedSubfieldValues.clear();
					concatenatedSubfieldValues.addAll(translatedConcatenatedSubfieldValues);
				}
			}
		}

		
		for (MatchingObject matchingObject : listOfRelevantMatchingObjects) {

			String solrFieldname = matchingObject.getSolrFieldname();
			Map<String, List<String>> valuesToMatchWith = matchingObject.getMabFieldnames();
			boolean hasRegex = matchingObject.hasRegex();
			String regexValue = matchingObject.getRegexValue();
			boolean hasRegexStrict = matchingObject.hasRegexStrict();
			String regexStrictValue = matchingObject.getRegexStrictValue();
			boolean hasRegexReplace = matchingObject.hasRegExReplace();
			String regexReplacePattern = matchingObject.getRegexReplaceValues().get(1);
			String regexReplaceValue = matchingObject.getRegexReplaceValues().get(2);
			boolean allowDuplicates = matchingObject.isAllowDuplicates();
			boolean isTranslateValue = false;
			boolean isTranslateValueContains = false;
			boolean isTranslateValueRegex = false;
			boolean hasConnectedSubfields = false;
			if (matchingObject.hasConnectedSubfields()) {
				hasConnectedSubfields = true;
			}
			boolean hasConcatenatedSubfields = false;
			String concatenatedSeparator = null;
			if (matchingObject.hasConcatenatedSubfields()) {
				hasConcatenatedSubfields = true;
				concatenatedSeparator = mabField.getConcatenatedSeparator();
			}


			if (matchingObject.isTranslateValue() || matchingObject.isTranslateValueContains() || matchingObject.isTranslateValueRegex()) {

				// Treat "normal" translate values
				Map<String, String> translateProperties = matchingObject.getTranslateProperties();
				String defaultValue = matchingObject.getDefaultValue();
				if (matchingObject.isTranslateValue()) {
					isTranslateValue = true;
				} else if (matchingObject.isTranslateValueContains()) {
					isTranslateValueContains = true;
				} else if (matchingObject.isTranslateValueRegex()) {
					isTranslateValueRegex = true;
				}

				for (Entry<String, List<String>> valueToMatchWith : valuesToMatchWith.entrySet()) {

					String mabFieldnameProps = valueToMatchWith.getKey(); // = MAB-Fieldname from mab.properties
					String fromCharacter = valueToMatchWith.getValue().get(0);
					String toCharacter = valueToMatchWith.getValue().get(1);
					String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacter, toCharacter, defaultValue, isTranslateValue, isTranslateValueContains, isTranslateValueRegex, false, hasRegex, regexValue, hasRegexStrict, regexStrictValue, hasRegexReplace, regexReplacePattern, regexReplaceValue);

					if (mabFieldnameProps.length() == 3) {
						// Match controlfields. For example for "LDR" (= leader), "AVA", "FMT" (= MH or MU), etc.
						if (matchControlfield(mabFieldnameXml, mabFieldnameProps)) {
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$*
						if (matchInd1(mabFieldnameXml, mabFieldnameProps) == true) {
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$*
						if (matchInd2(mabFieldnameXml, mabFieldnameProps) == true) {
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					} else if (Pattern.matches("\\$[\\w-]{2}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$aa$*
						if (matchInd1AndInd2(mabFieldnameXml, mabFieldnameProps) == true) {
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$a
						if (matchInd1AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$a
						if (matchInd2AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					} else if (mabFieldnameProps.substring(3, 6).equals("$**")) { // 100$**$a
						if (matchSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					} else { 
						if (mabFieldnameProps.equals(mabFieldnameXml)) { // Match against the value as it is. E. g. 100$a1$z matches only against 100$a1$z
							if (translatedValue != null) {
								addMabfield(solrFieldname, translatedValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					}
				}
			} else {

				for (Entry<String, List<String>> valueToMatchWith : valuesToMatchWith.entrySet()) {

					String mabFieldnameProps = valueToMatchWith.getKey(); // = MAB-Fieldname from mab.properties
					String mabFieldValue = mabField.getFieldvalue();

					// Use regex if user has defined one:
					if (hasRegex && regexValue != null) {
						Pattern pattern = java.util.regex.Pattern.compile(regexValue); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcher = pattern.matcher(mabFieldValue);
						String regexedMabFieldValue = "";
						while (matcher.find()) {
							regexedMabFieldValue = regexedMabFieldValue.concat(matcher.group());
						}
						if (!regexedMabFieldValue.trim().isEmpty()) {
							mabFieldValue = regexedMabFieldValue.trim();
						}
					}

					// Use regex strict if user has defined one:
					if (hasRegexStrict && regexStrictValue != null) {
						Pattern pattern = java.util.regex.Pattern.compile(regexStrictValue); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcher = pattern.matcher(mabFieldValue);
						String regexedStrictMabFieldValue = "";
						while (matcher.find()) {
							regexedStrictMabFieldValue = regexedStrictMabFieldValue.concat(matcher.group());
						}
						if (!regexedStrictMabFieldValue.trim().isEmpty()) {
							mabFieldValue = regexedStrictMabFieldValue.trim();
						} else {
							// Return null ("strict" regex)
							mabFieldValue = null;
						}
					}

					// Use regex replace if user has defined one:
					if (hasRegexReplace && regexReplacePattern != null && !regexReplacePattern.isEmpty()) {
						mabFieldValue = mabFieldValue.replaceAll(regexReplacePattern, regexReplaceValue).trim();
					}

					if (mabFieldnameProps.length() == 3) {
						// Match controlfields. For example for "LDR" (= leader), "AVA", "FMT" (= MH or MU), etc.
						if (matchControlfield(mabFieldnameXml, mabFieldnameProps)) {
							addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
						}
					} else if (mabFieldnameProps.length() == 8) {

						if (mabFieldnameProps.substring(3).equals("$**$*")) { // Match against all indicators and subfields. E. g. 100$**$* matches 100$a1$b, 100$b3$z, 100$z*$-, etc.
							if (allFields(mabFieldnameXml, mabFieldnameProps) == true) {	
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$*
							if (matchInd1(mabFieldnameXml, mabFieldnameProps) == true) {
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$*
							if (matchInd2(mabFieldnameXml, mabFieldnameProps) == true) {
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						} else if (Pattern.matches("\\$[\\w-]{2}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$aa$*
							if (matchInd1AndInd2(mabFieldnameXml, mabFieldnameProps) == true) {
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$a
							if (matchInd1AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$a
							if (matchInd2AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						} else if (mabFieldnameProps.substring(3, 6).equals("$**")) { // 100$**$a
							if (matchSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						} else { 
							if (mabFieldnameProps.equals(mabFieldnameXml)) { // Match against the value as it is. E. g. 100$a1$z matches only against 100$a1$z
								addMabfield(solrFieldname, mabFieldValue, allowDuplicates, hasConnectedSubfields, connectedSubfieldValues, hasConcatenatedSubfields, concatenatedSubfieldValues, concatenatedSeparator);
							}
						}
					}
				}
			}
		}

		listOfRelevantMatchingObjects = null;

		return listOfMatchedFields;
	}



	/**
	 * Matching operation: All given indicators and subfields must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean allFields(String in, String matchValue) {

		String match = matchValue.substring(0, 3);

		// in = Values from XML-File
		// matchValue = Values from mab.properties file
		// match = Only first 3 characters from matchValue (e. g. "311" from "311$a1$p")

		// Match 4 or 5 characters (exports from ALEPH Publisher could have 2 indicators, so the input Value could e. g. be 331$a1$b. So "$a1$b" has 5 characters. In contrary,
		// the "normal" ALEPH export (with service print_03) does not have a second indicator. There we only have e. g. 331$a$b, so "$a$b" are 4 characters to match against.
		// Match 3 characters of fieldnumber plus 4 or 5 random characters ( match+".{4,5} ) against the input value. Only the "match"-value must fit.
		boolean matches = Pattern.matches(match+".{4,5}", in);

		return matches;
	}


	/**
	 * Matching operation: Indicator 1 must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean matchInd1(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator1 = matchValue.substring(4, 5);
		boolean matches = Pattern.matches(fieldNo + "\\$" + indicator1 + ".\\$.", in); // Fieldnumber and indicator1 must match (100$a*$*)
		return matches;
	}

	/**
	 * Matching operation: Indicator 2 must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean matchInd2(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator2 = matchValue.substring(5, 6);
		boolean matches = Pattern.matches(fieldNo + "\\$." + indicator2 + "\\$.", in); // Fieldnumber and indicator2 must match (100$*a$*)
		return matches;
	}

	/**
	 * Matching operation: Indicator 1 and 2 must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean matchInd1AndInd2(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator1 = matchValue.substring(4, 5);
		String indicator2 = matchValue.substring(5, 6);
		boolean matches = Pattern.matches(fieldNo + "\\$" + indicator1 + indicator2 + "\\$.", in); // Fieldnumber, indicator1 and indicator2 must match (100$aa$*)
		return matches;
	}

	/**
	 * Matching operation: Indicator 1 and subfield must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean matchInd1AndSubfield(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator1 = matchValue.substring(4, 5);
		String subfield = matchValue.substring(7, 8);
		boolean matches = Pattern.matches(fieldNo + "\\$" + indicator1 + ".\\$" + subfield, in); // Fieldnumber, indicator1 and subfield must match (100$a*$a)
		return matches;
	}

	/**
	 * Matching operation: Indicator 2 and subfield must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean matchInd2AndSubfield(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator2 = matchValue.substring(5, 6);
		String subfield = matchValue.substring(7, 8);
		boolean matches = Pattern.matches(fieldNo + "\\$." + indicator2 + "\\$" + subfield, in); // Fieldnumber, indicator1 and subfield must match (100$*a$a)
		return matches;
	}

	/**
	 * Matching operation: Subfield must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean matchSubfield(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String subfield = matchValue.substring(7, 8);
		boolean matches = Pattern.matches(fieldNo + "\\$..\\$" + subfield, in); // Fieldnumber and subfield must match (100$**$a)
		return matches;
	}

	/**
	 * Matching operation: Controlfield must match.
	 * 
	 * @param in			String: Fieldname from MarcXML file
	 * @param matchValue	String: Fieldname from mab.properties file
	 * @return				boolean: true if the fieldnames are matching according to the rules
	 */
	public boolean matchControlfield(String in, String matchValue) {
		String fieldName = matchValue;
		boolean matches = Pattern.matches(fieldName, in);		
		return matches;
	}


	/**
	 * Getting the value of a translation file.
	 * @param solrFieldname								String: Name of Solr field 
	 * @param mabField									Mabfield: Mabfield object
	 * @param translateProperties						Map<String, String>: Contents of a translation.properties file
	 * @param fromCount									String: Index of first character to match or "all"
	 * @param toCount									String: Index of last character to match or "all"
	 * @param translateDefaultValue						String
	 * @param isTranslateValue							boolean
	 * @param isTranslateValueContains					boolean
	 * @param isTranslateValueRegex						boolean
	 * @param hasRegex									boolean
	 * @param isTranslateConnectedSubfields				boolean
	 * @param regexValue								String
	 * @param hasRegexStrict							hasRegexStrict
	 * @param regexStrictValue							String
	 * @param hasRegexReplace							boolean
	 * @param regexReplacePattern						String
	 * @param regexReplaceValue							String
	 * @return											String containing the translated value
	 */
	private String getTranslatedValue(String solrFieldname, Mabfield mabField, Map<String, String> translateProperties, String fromCount, String toCount, String translateDefaultValue, boolean isTranslateValue, boolean isTranslateValueContains, boolean isTranslateValueRegex, boolean isTranslateConnectedSubfields, boolean hasRegex, String regexValue, boolean hasRegexStrict, String regexStrictValue, boolean hasRegexReplace, String regexReplacePattern, String regexReplaceValue) {
		String translateValue = null;
		String matchedValueXml = null;
		String fieldNameXml = mabField.getFieldname();
		String fieldValueXml = mabField.getFieldvalue();

		// Use regex if user has defined one:
		if (hasRegex && regexValue != null) {
			Pattern pattern = java.util.regex.Pattern.compile(regexValue);
			Matcher matcher = pattern.matcher(fieldValueXml);
			String regexedMabFieldValue = "";
			while (matcher.find()) {
				regexedMabFieldValue = regexedMabFieldValue.concat(matcher.group());
			}
			if (!regexedMabFieldValue.trim().isEmpty()) {
				fieldValueXml = regexedMabFieldValue.trim();
			}
		}

		// Use regex strict if user has defined one:
		if (hasRegexStrict && regexStrictValue != null) {
			Pattern pattern = java.util.regex.Pattern.compile(regexStrictValue);
			Matcher matcher = pattern.matcher(fieldValueXml);
			String regexedStrictMabFieldValue = "";
			while (matcher.find()) {
				regexedStrictMabFieldValue = regexedStrictMabFieldValue.concat(matcher.group());
			}
			if (!regexedStrictMabFieldValue.trim().isEmpty()) {
				fieldValueXml = regexedStrictMabFieldValue.trim();
			} else {
				// Return null ("strict" regex):
				fieldValueXml = null;
			}
		}

		// Use regex replace if user has defined one:
		if (hasRegexReplace && regexReplacePattern != null && !regexReplacePattern.isEmpty()) {
			fieldValueXml = fieldValueXml.replaceAll(regexReplacePattern, regexReplaceValue).trim();
		}


		// Get characters from the positions the user defined in mab.properties file:
		// E. g. get "Full" from "Fulltext" if character positions [1-4] was defined.
		if (fromCount.equals("all") && toCount.equals("all")) {
			matchedValueXml = fieldValueXml.substring(0, fieldValueXml.length());

		} else {

			try {
				int intFromCount = Integer.valueOf(fromCount); // Beginning Index of value to match
				intFromCount = (intFromCount-1);
				int intToCount = Integer.valueOf(toCount); // Ending Index of value to match

				if (intToCount <= fieldValueXml.length()) {
					matchedValueXml = fieldValueXml.substring(intFromCount, intToCount);
				} else if (intFromCount <= fieldValueXml.length() && intToCount >= fieldValueXml.length()) {
					matchedValueXml = fieldValueXml.substring(intFromCount, fieldValueXml.length());
				}
			} catch (NumberFormatException nfe) {
				System.err.println("ERROR: Please make sure that you defined [n-n] or [all] for MAB field \"" + mabField.getFieldname() + "\" in your mab.properties for field \"" + solrFieldname +"\". This is normally necessary for translate values.\n");
				System.exit(1);
			}
		}

		if (matchedValueXml != null) {
			for (Entry<String, String> translateProperty : translateProperties.entrySet()) {
				if (isTranslateValue && matchedValueXml.equals(translateProperty.getKey())) {
					translateValue = translateProperty.getValue();
				} else if (isTranslateValueContains && matchedValueXml.contains(translateProperty.getKey())) {
					translateValue = translateProperty.getValue();
				} else if (isTranslateValueRegex) {
					String propKey = translateProperty.getKey();
					String translatePropertyFieldname = propKey.substring(0, propKey.indexOf("|")).trim();
					String translatePropertyRegex = propKey.substring(propKey.indexOf("|")+1).trim();
					Pattern pattern = java.util.regex.Pattern.compile(translatePropertyRegex);
					Matcher matcher = pattern.matcher(matchedValueXml);
					if (translatePropertyFieldname.equals("any") || fieldNameXml.equals(translatePropertyFieldname)) {
						if (matcher.find()) {
							translateValue = translateProperty.getValue();
						}
					}
				}
			}
		}

		// Set default value if user specified one and if no other translate value was found
		if (translateValue == null && translateDefaultValue != null) {
			translateValue = translateDefaultValue;
		}

		return translateValue;
	}


	/**
	 * Add values to a Mabfield object.
	 * @param solrFieldname		String: Name of the solr field
	 * @param solrFieldvalue	String: Value of the solr field
	 * @param allowDuplicates	boolean: Indicate if duplicate values are allowed in Solr multivalued fields (default is false).
	 */
	private void addMabfield(String solrFieldname, String solrFieldvalue, boolean allowDuplicates, boolean hasConnectedSubfields, List<String> connectedSubfieldValues, boolean hasConcatenatedSubfields, List<String> concatenatedSubfieldValues, String concatenatedSeparator) {
		Mabfield mf = new Mabfield(solrFieldname, solrFieldvalue);
		mf.setAllowDuplicates(allowDuplicates);
		if (hasConnectedSubfields && connectedSubfieldValues != null && !connectedSubfieldValues.isEmpty()) {
			mf.setConnectedValues(connectedSubfieldValues);
		}
		
		if (hasConcatenatedSubfields && concatenatedSubfieldValues != null && !concatenatedSubfieldValues.isEmpty()) {
			mf.setConcatenatedValues(concatenatedSubfieldValues);
			mf.setConcatenatedSeparator(concatenatedSeparator);
		}
		listOfMatchedFields.add(mf);
	}

}