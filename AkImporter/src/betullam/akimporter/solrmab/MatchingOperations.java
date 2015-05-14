package betullam.akimporter.solrmab;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class MatchingOperations {

	private List<Mabfield> listOfMatchedFields;
	private RecordSet newRecordSet;
	private List<Record> newListOfRecords;
	private List<List<Mabfield>> listOfNewSolrfieldLists;
	private List<Mabfield> listOfNewFieldsForNewRecord;
	private Record newRecord;
	List<String> multiValuedFields = SolrMab.multiValuedFields;
	List<Mabfield> customTextFields = SolrMab.customTextFields;
	HashMap<String, List<String>> translateFields = SolrMab.translateFields;

	/**
	 * 1. Match old Mab-Field to Solrfields. As the old field could match two or more Solrfields, you may get several Solrfields for 1 old Mabfield
	 *    Therefore, we need to return a List of the new Solrfields.
	 * 2. Add the List of the new Solrfields (same Datastructure as Mabfields) to a List of Lists (List<List<Mabfield>>), but only if it's not empty (that's
	 *    how we get rid of none-matching Mabfields).
	 * 3. For each List in the List of Mabfields (which are now Solrfields), add each Solrfield to a new List<Mabfield> and add that List to a Record.
	 * 4. Add the Record to a List<Record>.
	 * 5. Add the List<Record> to a RecordSet and return it.
	 */


	public RecordSet matching(RecordSet oldRecordSet, List<MatchingObject> listOfMatchingObjs) {

		newRecordSet = new RecordSet();
		newListOfRecords = new ArrayList<Record>();		


		for (Record oldRecord : oldRecordSet.getRecords()) {

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
			newRecord.setSatztyp(oldRecord.getSatztyp());
			newRecord.setRecordID(oldRecord.getRecordID());
			newListOfRecords.add(newRecord);
		}

		newRecordSet.setRecords(newListOfRecords);

		return newRecordSet;
	}



	public List<Mabfield> matchField(Mabfield mabField, List<MatchingObject> listOfMatchingObjects) {		


		String mabFieldnameXml = mabField.getFieldname();
		listOfMatchedFields = new ArrayList<Mabfield>();

		//System.out.println("mabFieldname: " + mabFieldname);

		for (MatchingObject matchingObject : listOfMatchingObjects) {
			String solrFieldname = matchingObject.getSolrFieldname();
			HashMap<String, List<String>> valuesToMatchWith = matchingObject.getMabFieldnames();


			if (matchingObject.isTranslateValue()) {

				//System.out.println("mabFieldname: " + matchingObject.getTranslateProperties());
				//System.out.println("Translate Properties: " + matchingObject.getTranslateProperties());
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
							
							// WORX:
							/*
							String fieldValueXml = mabField.getFieldvalue();

							if (fieldValueXml.length() >= toCharacterCount) { // Avoid "Index out of range" exceptions
								String matchedValueXml = fieldValueXml.substring(fromCharacterCount, toCharacterCount);
								String valueToUse = null;
								for (Entry<String, String> translateProperty : translateProperties.entrySet()) {
									if (matchedValueXml.equals(translateProperty.getKey())) {
										valueToUse = translateProperty.getValue();
									}
								}
								if (valueToUse != null) {
									listOfMatchedFields.add(new Mabfield(solrFieldname, valueToUse));
								}
							}
							*/
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
						//System.out.println("valueToMatchWith: " + valueToMatchWith);
						//System.out.println("valueToMatchWith: " + valueToMatchWith + "; substring: " + valueToMatchWith.substring(7, 8));
						//System.out.println("valueToMatchWith: " + valueToMatchWith + "; valueToMatchWith.substring(3, 8): " + valueToMatchWith.substring(3, 8));
						//System.out.println("valueToMatchWith: " + valueToMatchWith + "; valueToMatchWith.substring(4, 8): " + valueToMatchWith.substring(3, 8));

						if (mabFieldnameProps.substring(3).equals("$**$*")) { // Match against all indicators and subfields. E. g. 100$**$* matches 100$a1$b, 100$b3$z, 100$z*$-, etc.
							if (allFields(mabFieldnameXml, mabFieldnameProps) == true) {	
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								// System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$*
							if (matchInd1(mabFieldnameXml, mabFieldnameProps) == true) {
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								//System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$*
							if (matchInd2(mabFieldnameXml, mabFieldnameProps) == true) {
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								//System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						} else if (Pattern.matches("\\$[\\w-]{2}\\$\\*", mabFieldnameProps.substring(3, 8)) == true) { // 100$aa$*
							if (matchInd1AndInd2(mabFieldnameXml, mabFieldnameProps) == true) {
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								//System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						} else if (Pattern.matches("\\$[\\w-]{1}\\*\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$a*$a
							if (matchInd1AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								//System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						} else if (Pattern.matches("\\$\\*[\\w-]{1}\\$\\w{1}", mabFieldnameProps.substring(3, 8)) == true) { // 100$*a$a
							if (matchInd2AndSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								//System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						} else if (mabFieldnameProps.substring(3, 6).equals("$**")) { // 100$**$a
							// System.out.println("valueToMatchWith: " + valueToMatchWith + "; valueToMatchWith.substring(3, 6): " + valueToMatchWith.substring(3, 6));
							if (matchSubfield(mabFieldnameXml, mabFieldnameProps) == true) {
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								//System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						} else { 
							if (mabFieldnameProps.equals(mabFieldnameXml)) { // Match against the value as it is. E. g. 100$a1$z matches only against 100$a1$z
								// Create new Mabfield-Object, which has the same data-structure as a Solrfield (Sting fieldname, String fieldvalue), and add it to the list of matched fields:
								listOfMatchedFields.add(new Mabfield(solrFieldname, mabField.getFieldvalue()));
								//System.out.println(solrFieldname + ": " + mabField.getFieldvalue());
							}
						}
					}
				}
			}
		}
		return listOfMatchedFields;
	}



	public boolean allFields(String in, String matchValue) {

		String match = matchValue.substring(0, 3);

		// in = Values from XML-File
		// matchValue = Values from mab.properties file
		// match = Only first 3 characters from matchValue (e. g. "311" from "311$a1$p")
		//System.out.println("in: " + in + ", matchValue: " + matchValue + "; match: " + match);

		// Match 4 or 5 characters (exports from ALEPH Publisher could have 2 indicators, so the input Value could e. g. be 331$a1$b. So "$a1$b" has 5 characters. In contrary,
		// the "normal" ALEPH export (with service print_03) does not have a second indicator. There we only have e. g. 331$a$b, so "$a$b" are 4 characters to match against.
		// Match 3 characters of fieldnumber plus 4 or 5 random characters ( match+".{4,5} ) against the input value. Only the "match"-value must fit.
		boolean matches = Pattern.matches(match+".{4,5}", in);

		return matches;
	}


	public boolean matchInd1(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator1 = matchValue.substring(4, 5);
		boolean matches = Pattern.matches(fieldNo + "\\$" + indicator1 + ".\\$.", in); // Fieldnumber and indicator1 must match (100$a*$*)
		return matches;
	}

	public boolean matchInd2(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator2 = matchValue.substring(5, 6);
		boolean matches = Pattern.matches(fieldNo + "\\$." + indicator2 + "\\$.", in); // Fieldnumber and indicator2 must match (100$*a$*)
		return matches;
	}

	public boolean matchInd1AndInd2(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator1 = matchValue.substring(4, 5);
		String indicator2 = matchValue.substring(5, 6);
		boolean matches = Pattern.matches(fieldNo + "\\$" + indicator1 + indicator2 + "\\$.", in); // Fieldnumber, indicator1 and indicator2 must match (100$aa$*)
		return matches;
	}

	public boolean matchInd1AndSubfield(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator1 = matchValue.substring(4, 5);
		String subfield = matchValue.substring(7, 8);
		boolean matches = Pattern.matches(fieldNo + "\\$" + indicator1 + ".\\$" + subfield, in); // Fieldnumber, indicator1 and subfield must match (100$a*$a)
		return matches;
	}

	public boolean matchInd2AndSubfield(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String indicator2 = matchValue.substring(5, 6);
		String subfield = matchValue.substring(7, 8);
		boolean matches = Pattern.matches(fieldNo + "\\$." + indicator2 + "\\$" + subfield, in); // Fieldnumber, indicator1 and subfield must match (100$*a$a)
		return matches;
	}

	public boolean matchSubfield(String in, String matchValue) {
		String fieldNo = matchValue.substring(0, 3);
		String subfield = matchValue.substring(7, 8);
		boolean matches = Pattern.matches(fieldNo + "\\$..\\$" + subfield, in); // Fieldnumber and subfield must match (100$**$a)
		return matches;
	}

	public boolean matchControlfield(String in, String matchValue) {
		String fieldName = matchValue;
		boolean matches = Pattern.matches(fieldName, in);
		return matches;
	}
	
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
