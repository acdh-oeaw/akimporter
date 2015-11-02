/**
 * Matching MAB fields to respective Solr fields.
 * 
 * This is where some of the data processing is done to
 * get the values in shape before indexing them to Solr.
 * 
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
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
package betullam.akimporter.solrmab.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import betullam.akimporter.solrmab.Index;

public class MatchingOperations {

	private List<Mabfield> listOfMatchedFields;
	private List<Record> newListOfRecords;
	private List<List<Mabfield>> listOfNewSolrfieldLists;
	private List<Mabfield> listOfNewFieldsForNewRecord;
	private Record newRecord;
	List<String> multiValuedFields = Index.multiValuedFields;
	List<Mabfield> customTextFields = Index.customTextFields;
	HashMap<String, List<String>> translateFields = Index.translateFields;


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

			listOfNewSolrfieldLists = new ArrayList<List<Mabfield>>();
			listOfNewFieldsForNewRecord = new ArrayList<Mabfield>();
			newRecord = new Record();			

			for (Mabfield oldMabField : oldRecord.getMabfields()) {
				//System.out.println("oldMabField: " + oldMabField.getFieldname() + ": " + oldMabField.getFieldvalue());
				// Match each Mabfield in the Record to the defined Solrfields. As one old Mabfield could match multiple Solrfields, we get back a List of Fields:
				List<Mabfield> listOfSolrfieldsForMabfield = this.matchField(oldMabField, listOfMatchingObjs);
				listOfNewSolrfieldLists.add(listOfSolrfieldsForMabfield);
			}

			for (List<Mabfield> newSolrFields : listOfNewSolrfieldLists) {
				for (Mabfield newSolrField : newSolrFields) {
					//System.out.println(newSolrField.getFieldname() + ": " + newSolrField.getFieldvalue());
					listOfNewFieldsForNewRecord.add(newSolrField);
				}
			}
			

			for (Mabfield customTextField : customTextFields) {
				//System.out.println("2. Solr Fieldname: " + customTextField.getFieldname() + ": " + customTextField.getFieldvalue());
				listOfNewFieldsForNewRecord.add(customTextField);
			}

			// TODO: Is there an easier way to deduplicate certain fields?
			// DeDuplication of none-multivalued SolrFields:
			List<Mabfield> finalDedupSolrlist = new ArrayList<Mabfield>();
			List<String> fieldsInFinalDedupList = new ArrayList<String>();


			for (Mabfield solrfield : listOfNewFieldsForNewRecord) {
				if (multiValuedFields.contains(solrfield.getFieldname()) == false) { // If its a single valued SolrField!

					// Make a list of all fieldvalues that are already added, so we can compare to it in the next step:
					for (Mabfield mabfield : finalDedupSolrlist) {
						fieldsInFinalDedupList.add(mabfield.getFieldname());
					}

					if (fieldsInFinalDedupList.contains(solrfield.getFieldname()) == false) { // If there is not yet a SolrField with that fieldname, add it, but just one!
						finalDedupSolrlist.add(solrfield);
					}
				} else { // Multi valued SolrField
					finalDedupSolrlist.add(solrfield);
				}
			}

			newRecord.setMabfields(finalDedupSolrlist);
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
		List<MatchingObject> listOfRelevantMatchingObjects = new ArrayList<MatchingObject>();
		for (MatchingObject matchingObject : listOfMatchingObjects) {
			String mabFieldNo = mabFieldnameXml.substring(0, 3);
			boolean containsMabFieldNo = matchingObject.getMabFieldnames().keySet().toString().contains(mabFieldNo);
			
			if (containsMabFieldNo) {
				listOfRelevantMatchingObjects.add(matchingObject);
			}
		}

		for (MatchingObject matchingObject : listOfRelevantMatchingObjects) {
			String solrFieldname = matchingObject.getSolrFieldname();
			
			HashMap<String, List<String>> valuesToMatchWith = matchingObject.getMabFieldnames();


			if (matchingObject.isTranslateValue()) {

				HashMap<String, String> translateProperties = matchingObject.getTranslateProperties();

				for (Entry<String, List<String>> valueToMatchWith : valuesToMatchWith.entrySet()) {
					String mabFieldnameProps = valueToMatchWith.getKey(); // = MAB-Fieldname from mab.properties
					int fromCharacterCount = Integer.valueOf(valueToMatchWith.getValue().get(0)); // Beginning Index of value to match
					int toCharacterCount = Integer.valueOf(valueToMatchWith.getValue().get(1)); // Ending Index of value to match
					fromCharacterCount = (fromCharacterCount-1);

					if (mabFieldnameProps.length() == 3) {
						// Match controlfields. For example for "LDR" (= leader), "AVA", "FMT" (= MH or MU), etc.
						if (matchControlfield(mabFieldnameXml, mabFieldnameProps)) {
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$*
						if (matchInd1(mabFieldnameXml, mabFieldnameProps) == true) {
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$*
						if (matchInd2(mabFieldnameXml, mabFieldnameProps) == true) {
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					} else if (Pattern.matches("\\$[\\w-]{2}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$aa$*
						if (matchInd1AndInd2(mabFieldnameXml, mabFieldnameProps) == true) {
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$a
						if (matchInd1AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$a
						if (matchInd2AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					} else if (mabFieldnameProps.substring(3, 6).equals("$**")) { // 100$**$a
						if (matchSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					} else { 
						if (mabFieldnameProps.equals(mabFieldnameXml)) { // Match against the value as it is. E. g. 100$a1$z matches only against 100$a1$z
							String translatedValue = getTranslatedValue(solrFieldname, mabField, translateProperties, fromCharacterCount, toCharacterCount);
							if (translatedValue != null) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, translatedValue));
							}
						}
					}

				}

			} else {

				for (Entry<String, List<String>> valueToMatchWith : valuesToMatchWith.entrySet()) {

					String mabFieldnameProps = valueToMatchWith.getKey(); // = MAB-Fieldname from mab.properties
					
					if (mabFieldnameProps.length() == 3) {
						// Match controlfields. For example for "LDR" (= leader), "AVA", "FMT" (= MH or MU), etc.
						if (matchControlfield(mabFieldnameXml, mabFieldnameProps)) {
							listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
						}
					} else if (mabFieldnameProps.length() == 8) {

						if (mabFieldnameProps.substring(3).equals("$**$*")) { // Match against all indicators and subfields. E. g. 100$**$* matches 100$a1$b, 100$b3$z, 100$z*$-, etc.
							if (allFields(mabFieldnameXml, mabFieldnameProps) == true) {	
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
							}
						} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$*
							if (matchInd1(mabFieldnameXml, mabFieldnameProps) == true) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
							}
						} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$*
							if (matchInd2(mabFieldnameXml, mabFieldnameProps) == true) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
							}
						} else if (Pattern.matches("\\$[\\w-]{2}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$aa$*
							if (matchInd1AndInd2(mabFieldnameXml, mabFieldnameProps) == true) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
							}
						} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$a
							if (matchInd1AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
							}
						} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$a
							if (matchInd2AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
							}
						} else if (mabFieldnameProps.substring(3, 6).equals("$**")) { // 100$**$a
							if (matchSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
							}
						} else { 
							if (mabFieldnameProps.equals(mabFieldnameXml)) { // Match against the value as it is. E. g. 100$a1$z matches only against 100$a1$z
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
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
	 * 
	 * @param solrFieldname			String: Name of Solr field 
	 * @param mabField				Mabfield: Mabfield object
	 * @param translateProperties	HashMap<String, String>: Contents of a translation.properties file
	 * @param fromCount				int: Index of first character to match
	 * @param toCount				int: Index of last character to match
	 * @return						String containing the translated value
	 */
	private String getTranslatedValue(String solrFieldname, Mabfield mabField, HashMap<String, String> translateProperties, int fromCount, int toCount) {
		String translateValue = null;
		String fieldValueXml = mabField.getFieldvalue();

		if (fieldValueXml.length() >= toCount) { // Avoid "Index out of range" exceptions
			String matchedValueXml = fieldValueXml.substring(fromCount, toCount);
			
			for (Entry<String, String> translateProperty : translateProperties.entrySet()) {
				if (matchedValueXml.equals(translateProperty.getKey())) {
					translateValue = translateProperty.getValue();
				}
			}
		}
		return translateValue;
	}

}