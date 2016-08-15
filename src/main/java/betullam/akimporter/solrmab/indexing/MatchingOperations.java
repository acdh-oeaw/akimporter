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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.java.betullam.akimporter.solrmab.Index;

public class MatchingOperations {

	// ################ OLD #################
	//private List<Mabfield> listOfMatchedFields;
	//private List<Record> newListOfRecords;
	//private List<List<Mabfield>> listOfNewSolrfieldLists;
	//private List<Mabfield> listOfNewFieldsForNewRecord;
	//private Record newRecord;
	List<String> multiValuedFields = Index.multiValuedFields;
	List<Mabfield> customTextFields = Index.customTextFields;
	Map<String, List<String>> translateFields = Index.translateFields;
	String fullRecordFieldname = Index.fullrecordFieldname;
	// ################ OLD #################


	// ################ NEW #################
	private List<Record> rawRecords;
	private List<PropertiesObject> allPropertiesObjects;
	private List<Record> matchingResult;

	public MatchingOperations() {}

	public MatchingOperations(List<Record> rawRecords, List<PropertiesObject> allPropertiesObjects) {
		this.rawRecords = rawRecords;
		this.allPropertiesObjects = allPropertiesObjects;
	}
	// ################ NEW #################

	private List<Record> matching() {
		for (Record rawRecord : rawRecords) {
			for (Controlfield rawControlfield : rawRecord.getControlfields()) {
				this.matchField(rawControlfield, allPropertiesObjects);
			}
			for (Datafield rawDatafield : rawRecord.getDatafields()) {
				this.matchField(rawDatafield, allPropertiesObjects);
			}
		}

		return matchingResult;
	}



	/**
	 * Matching a raw field from the MarcXML file to the Solr field according to the rules in mab.properties.
	 * 
	 * @param rawField				Object: The raw field (Datafield or Controlfield) from the MarcXML record
	 * @param allPropertiesObjects	List<PropertiesObject>: All rules from the mab.properties file as a list of PropertiesObject objects
	 * @return						??? A list of matched fields for indexing to Solr
	 */
	public List<SolrField> matchField(Object rawField, List<PropertiesObject> allPropertiesObjects) {

		// List of SolrField objects for the matching result. This is the return value of this method. It will
		// contain all the data that we need for the indexing process to Solr.
		List<SolrField> solrFields = new ArrayList<SolrField>();

		Controlfield controlfield = null;
		Datafield datafield = null;
		String type = null;
		if (rawField instanceof Controlfield) {
			type = "controlfield";
			controlfield = (Controlfield) rawField;
		} else if (rawField instanceof Datafield) {
			type = "datafield";
			datafield = (Datafield) rawField;
		}

		// Remove unnecessary PropertiesObjects so that we don't have to iterate over all of them. This saves a lot of time!
		// A PropertiesObject is not necessary if it does not contain at least one field from the parsed MarcXML record.
		List<PropertiesObject> relevantPropertiesObjects = getRelevantPropertiesObjects(type, rawField, allPropertiesObjects);

		if (relevantPropertiesObjects == null) {
			return null;
		}

		/*
		for (PropertiesObject relevantPropertiesObject : relevantPropertiesObjects) {
			for (Datafield propertiesDatafield : relevantPropertiesObject.getDatafields()) {
				if (datafield.match(propertiesDatafield)) {
					System.out.println(datafield);
				}
			}
		}
		 */

		/*
		for (PropertiesObject relevantPropertiesObject : relevantPropertiesObjects) {
			for (Datafield df : relevantPropertiesObject.getDatafields()) {
				if (df.getTag().equals("902")) {
					System.out.println(relevantPropertiesObject.toString());
				}
			}
		}
		System.out.println("------------------------------------------------------------------------------------------");

		listOfMatchedFields = new ArrayList<Mabfield>();
		 */

		// TODO: Apply rules (translate, regex, subfieldExists, concatenate, etc.) from mab.properties here and return the result
		// List of rules:
		// OK: translateValue
		// OK: translateValueContains
		// OK: translateValueRegex
		// OK: defaultValue[VALUE]
		// regEx[REGEX]
		// regExStrict[REGEX]
		// regExReplace[REGEX][REPLACE]
		// allowDuplicates
		// connectedSubfields[subfield:subfield:subfield:...:DefaultText]
		// translateConnectedSubfields[translate.properties]
		// concatenatedSubfields[subfield:subfield:subfield:...:Separator]
		// translateConcatenatedSubfields[translate.properties]
		// subfieldExists[subfield:subfield:subfield:...:AND|OR]
		// subfieldNotExists[subfield:subfield:subfield:...:AND|OR]

		// TODO: These rules need to be handled elsewhere because they have nothing to do with the raw MarcXML fields:
		// multiValued
		// customText

		/*
		if (datafield != null && datafield.getTag().equals("100")) {
			System.out.println("Orig: " + datafield.toString());
			System.out.println("---------------------------------------------------");
		}
		 */


		for (PropertiesObject relevantPropertiesObject : relevantPropertiesObjects) {

			// Check if the raw subfield, for which we want to treat it's content, is listed in the properties object (which represents a line in mab.properties).
			// Example:
			// 	We have an XML (raw datafield) like this:
			//		<datafield ind1="b" ind2="1" tag="100">
			//			<subfield code="p">Berri, Claude</subfield>
			//			<subfield code="d">1934-2009</subfield>
			//			<subfield code="9">(DE-588)119130823</subfield>
			//			<subfield code="b">[Drehbuch, Regie]</subfield>
			//		</datafield>
			//	We have a 2 lines in mab.properties like this:
			//		author: 100$**$p, 100$**$a, 200$**$p, 200$**$a
			//	We need to only treat subfield p of the raw datafield, because subfields d, 9 and b are not contained in the line of mab.properties
			Datafield copiedDatafield = null;
			if (datafield != null) {
				copiedDatafield = Datafield.copy(datafield);
				List<Subfield> subfieldsToRemove = new ArrayList<Subfield>();
				for (Subfield subfield : datafield.getSubfields()) {
					if (!relevantPropertiesObject.containsSubfieldOfDatafield(datafield, subfield)) {
						// TODO: MOVE TO OTHER LIST INSTEAD OF REMOVING THE SUBFIELD. WE COULD NEED THE OTHER SUBFIELDS LATER ON
						copiedDatafield.removeSubfieldByCode(subfield.getCode());
					}
				}
			}


			String solrFieldname = relevantPropertiesObject.getSolrFieldname();
			List<String> fieldValues = new ArrayList<String>();
			Map<String, List<String>> propertiesFields = relevantPropertiesObject.getPropertiesFields();
			boolean hasRegex = relevantPropertiesObject.hasRegex();
			String regexPattern = relevantPropertiesObject.getRegexValue();
			boolean hasRegexStrict = relevantPropertiesObject.hasRegexStrict();
			String regexStrictPattern = relevantPropertiesObject.getRegexStrictValue();
			boolean hasRegexReplace = relevantPropertiesObject.hasRegExReplace();
			String regexReplacePattern = relevantPropertiesObject.getRegexReplaceValues().get(1);
			String regexReplaceValue = relevantPropertiesObject.getRegexReplaceValues().get(2);
			boolean allowDuplicates = relevantPropertiesObject.isAllowDuplicates();
			boolean isTranslateValue = false;
			boolean isTranslateValueContains = false;
			boolean isTranslateValueRegex = false;
			boolean hasConnectedSubfields = false;
			if (relevantPropertiesObject.hasConnectedSubfields()) {
				hasConnectedSubfields = true;
			}
			boolean hasConcatenatedSubfields = false;
			String concatenatedSeparator = null;
			if (relevantPropertiesObject.hasConcatenatedSubfields()) {
				hasConcatenatedSubfields = true;
				concatenatedSeparator = relevantPropertiesObject.getConcatenatedSubfieldsSeparator();
			}
			/*
			boolean skip = false;
			if (checkForSkip) {
				if (mabField.getSolrFieldnames().contains(relevantPropertiesObject.getSolrFieldname())) {
					skip = true;
				}
			}
			 */


			// Handle translateValue, translateValueContains and translateValueRegex (not translateConnectedSubfields and translateConcatenatedSubfields)
			if (relevantPropertiesObject.isTranslateValue() || relevantPropertiesObject.isTranslateValueContains() || relevantPropertiesObject.isTranslateValueRegex()) {

				// Treat "normal" translate values (there are also translateConnectedSubfields and translateConcatenatedSubfields which are handled elsewhere)
				Map<String, String> translateProperties = relevantPropertiesObject.getTranslateProperties();
				String defaultValue = relevantPropertiesObject.getDefaultValue();
				if (relevantPropertiesObject.isTranslateValue()) {
					isTranslateValue = true;
				} else if (relevantPropertiesObject.isTranslateValueContains()) {
					isTranslateValueContains = true;
				} else if (relevantPropertiesObject.isTranslateValueRegex()) {
					isTranslateValueRegex = true;
				}

				for (Entry<String, List<String>> propertiesField : relevantPropertiesObject.getPropertiesFields().entrySet()) {
					String fromCharacter = propertiesField.getValue().get(0);
					String toCharacter = propertiesField.getValue().get(1);
					if (type.equals("controlfield")) {
						String rawFieldname = controlfield.getTag();
						String rawFieldvalue = controlfield.getContent();
						String translatedValue = getTranslatedValue(solrFieldname, rawFieldname, rawFieldvalue, translateProperties, fromCharacter, toCharacter, defaultValue, isTranslateValue, isTranslateValueContains, isTranslateValueRegex, false, hasRegex, regexPattern, hasRegexStrict, regexStrictPattern, hasRegexReplace, regexReplacePattern, regexReplaceValue);
						//System.out.println("translatedValue: " + translatedValue);
						fieldValues.add(translatedValue);
					}
					if (type.equals("datafield")) {
						String tag = datafield.getTag();
						String ind1 = datafield.getInd1();
						String ind2 = datafield.getInd2();
						for (Subfield subfield : datafield.getSubfields()) {
							String rawFieldname = tag+"$"+ind1+ind2+"$"+subfield.getCode();
							String rawFieldvalue = subfield.getContent();
							String translatedValue = getTranslatedValue(solrFieldname, rawFieldname, rawFieldvalue, translateProperties, fromCharacter, toCharacter, defaultValue, isTranslateValue, isTranslateValueContains, isTranslateValueRegex, false, hasRegex, regexPattern, hasRegexStrict, regexStrictPattern, hasRegexReplace, regexReplacePattern, regexReplaceValue);
							//System.out.println("translatedValue: " + translatedValue);
							fieldValues.add(translatedValue);
						}
					}
				}
			}


			// Handle regEx, but only if we do not have a translate value (these are treated while translating)
			if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasRegex && regexPattern != null)) {
				if (type.equals("controlfield")) {
					String rawFieldvalue = controlfield.getContent();
					String regexedValue = getRegexValue(regexPattern, rawFieldvalue);
					//System.out.println("regexedValue: " + regexedValue);
					fieldValues.add(regexedValue);
				}
				if (type.equals("datafield")) {
					for (Subfield subfield : datafield.getSubfields()) {
						String rawFieldvalue = subfield.getContent();
						String regexedValue = getRegexValue(regexPattern, rawFieldvalue);
						//System.out.println("regexedValue: " + regexedValue);
						fieldValues.add(regexedValue);
					}
				}
			}


			// Handle regExStrict, but only if we do not have a translate value (these are treated while translating)
			if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasRegexStrict && regexStrictPattern != null)) {
				if (type.equals("controlfield")) {
					String rawFieldvalue = controlfield.getContent();
					String regexedStrictValue = getRegexStrictValue(regexStrictPattern, rawFieldvalue);
					//System.out.println("regexedStrictValue: " + regexedStrictValue);
					fieldValues.add(regexedStrictValue);
				}
				if (type.equals("datafield")) {
					for (Subfield subfield : datafield.getSubfields()) {
						String rawFieldvalue = subfield.getContent();
						String regexedStrictValue = getRegexStrictValue(regexStrictPattern, rawFieldvalue);
						//System.out.println("regexedStrictValue: " + regexedStrictValue);
						fieldValues.add(regexedStrictValue);
					}
				}
			}


			// Handle regExReplace, but only if we do not have a translate value (these are treated while translating)
			if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasRegexReplace && regexReplacePattern != null && !regexReplacePattern.isEmpty())) {
				if (type.equals("controlfield")) {
					String rawFieldvalue = controlfield.getContent();
					String regexedReplaceValue = rawFieldvalue.replaceAll(regexReplacePattern, regexReplaceValue).trim();
					//System.out.println("regexedReplaceValue: " + regexedReplaceValue);
					fieldValues.add(regexedReplaceValue);
				}

				if (type.equals("datafield")) {

					//System.out.println(relevantPropertiesObject.getSolrFieldname().toString());

					for (Subfield subfield : copiedDatafield.getSubfields()) {

						String rawFieldvalue = subfield.getContent();
						String regexedReplaceValue = rawFieldvalue.replaceAll(regexReplacePattern, regexReplaceValue).trim();

						//System.out.println(datafield.getTag() + datafield.getInd1() + datafield.getInd2() + subfield.getCode() + " - " + relevantPropertiesObject.getDatafields());
						System.out.println("regexedReplaceValue: " + regexedReplaceValue);

						fieldValues.add(regexedReplaceValue);
					}
				}

			}


			if (!fieldValues.isEmpty()) {
				for (String fieldValue : fieldValues) {
					solrFields.add(new SolrField(solrFieldname, fieldValue));
				}

			}
		}

		//System.out.println(solrFields.toString());

		return solrFields;
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
	private String getTranslatedValue(String solrFieldname, String rawFieldname, String rawFieldvalue, Map<String, String> translateProperties, String fromCount, String toCount, String translateDefaultValue, boolean isTranslateValue, boolean isTranslateValueContains, boolean isTranslateValueRegex, boolean isTranslateConnectedSubfields, boolean hasRegex, String regexValue, boolean hasRegexStrict, String regexStrictValue, boolean hasRegexReplace, String regexReplacePattern, String regexReplaceValue) {


		String translateValue = null;
		String matchedValueXml = null;


		// Use regex if user has defined one:
		if (hasRegex && regexValue != null) {
			Pattern pattern = java.util.regex.Pattern.compile(regexValue);
			Matcher matcher = pattern.matcher(rawFieldvalue);
			String regexedMabFieldValue = "";
			while (matcher.find()) {
				regexedMabFieldValue = regexedMabFieldValue.concat(matcher.group());
			}
			if (!regexedMabFieldValue.trim().isEmpty()) {
				rawFieldvalue = regexedMabFieldValue.trim();
			}
		}

		// Use regex strict if user has defined one:
		if (hasRegexStrict && regexStrictValue != null) {
			Pattern pattern = java.util.regex.Pattern.compile(regexStrictValue);
			Matcher matcher = pattern.matcher(rawFieldvalue);
			String regexedStrictMabFieldValue = "";
			while (matcher.find()) {
				regexedStrictMabFieldValue = regexedStrictMabFieldValue.concat(matcher.group());
			}
			if (!regexedStrictMabFieldValue.trim().isEmpty()) {
				rawFieldvalue = regexedStrictMabFieldValue.trim();
			} else {
				// Return null ("strict" regex):
				rawFieldvalue = null;
			}
		}

		// Use regex replace if user has defined one:
		if (hasRegexReplace && regexReplacePattern != null && !regexReplacePattern.isEmpty()) {
			rawFieldvalue = rawFieldvalue.replaceAll(regexReplacePattern, regexReplaceValue).trim();
		}


		// Get characters from the positions the user defined in mab.properties file:
		// E. g. get "Full" from "Fulltext" if character positions [1-4] was defined.
		if (fromCount.equals("all") && toCount.equals("all")) {
			matchedValueXml = rawFieldvalue.substring(0, rawFieldvalue.length());

		} else {

			try {
				int intFromCount = Integer.valueOf(fromCount); // Beginning Index of value to match
				intFromCount = (intFromCount-1);
				int intToCount = Integer.valueOf(toCount); // Ending Index of value to match

				if (intToCount <= rawFieldvalue.length()) {
					matchedValueXml = rawFieldvalue.substring(intFromCount, intToCount);
				} else if (intFromCount <= rawFieldvalue.length() && intToCount >= rawFieldvalue.length()) {
					matchedValueXml = rawFieldvalue.substring(intFromCount, rawFieldvalue.length());
				}
			} catch (NumberFormatException nfe) {
				System.err.println("ERROR: Please make sure that you defined [n-n] or [all] for MAB field \"" + rawFieldname + "\" in your mab.properties for field \"" + solrFieldname +"\". This is normally necessary for translate values.\n");
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
					if (translatePropertyFieldname.equals("any") || rawFieldname.equals(translatePropertyFieldname)) {
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

	private String getRegexValue(String regexPattern, String rawFieldvalue) {
		String returnValue = null;
		Pattern pattern = java.util.regex.Pattern.compile(regexPattern);
		Matcher matcher = pattern.matcher(rawFieldvalue);
		String regexedValue = "";
		while (matcher.find()) {
			regexedValue = regexedValue.concat(matcher.group());
		}
		if (!regexedValue.trim().isEmpty()) {
			returnValue = regexedValue.trim();
		} else {
			// Return original field value if regex does not match.
			// In getRegexStrictValue (see below), we return null instead of the original field value.
			returnValue = rawFieldvalue;
		}

		return returnValue;
	}

	private String getRegexStrictValue(String regexPattern, String rawFieldvalue) {
		Pattern pattern = java.util.regex.Pattern.compile(regexPattern);
		Matcher matcher = pattern.matcher(rawFieldvalue);
		String regexedStrictValue = "";
		while (matcher.find()) {
			regexedStrictValue = regexedStrictValue.concat(matcher.group());
		}
		if (!regexedStrictValue.trim().isEmpty()) {
			regexedStrictValue = regexedStrictValue.trim();
		} else {
			// Return null ("strict" regex)
			regexedStrictValue = null;
		}

		return regexedStrictValue;
	}


	/**
	 * Add values to a Mabfield object.
	 * @param solrFieldname		String: Name of the solr field
	 * @param solrFieldvalue	String: Value of the solr field
	 * @param allowDuplicates	boolean: Indicate if duplicate values are allowed in Solr multivalued fields (default is false).
	 */
	private void addMabfield(String solrFieldname, String solrFieldvalue, boolean allowDuplicates, boolean hasConnectedSubfields, List<String> connectedSubfieldValues, boolean hasConcatenatedSubfields, List<String> concatenatedSubfieldValues, String concatenatedSeparator, boolean skip) {

		if (!skip) { 
			Mabfield mf = new Mabfield(solrFieldname, solrFieldvalue);		

			mf.setAllowDuplicates(allowDuplicates);
			if (hasConnectedSubfields && connectedSubfieldValues != null && !connectedSubfieldValues.isEmpty()) {
				mf.setConnectedValues(connectedSubfieldValues);
			}

			if (hasConcatenatedSubfields && concatenatedSubfieldValues != null && !concatenatedSubfieldValues.isEmpty()) {
				mf.setConcatenatedValues(concatenatedSubfieldValues);
				mf.setConcatenatedSeparator(concatenatedSeparator);
			}

			mf.setSkip(skip);

			//listOfMatchedFields.add(mf);
		}
	}


	private List<PropertiesObject> getRelevantPropertiesObjects(String type, Object rawField, List<PropertiesObject> allPropertiesObjects) {

		List<PropertiesObject> relevantPropertiesObjects = new ArrayList<PropertiesObject>();

		// Remove unnecessary Controlfields from PropertiesObjects
		if (type.equals("controlfield")) {
			Controlfield rawControlfield = ((Controlfield) rawField);
			for (PropertiesObject propertiesObject : allPropertiesObjects) {
				if (rawControlfield.isContainedInPropertiesObject(propertiesObject)) {
					//System.out.println("TRUE : " + rawField.toString() + " is contained in " + propertiesObject.getControlfields().toString());
					relevantPropertiesObjects.add(propertiesObject);
				}
			}
		}

		// Remove unnecessary Datafields from PropertiesObjects
		if (type.equals("datafield")) {
			Datafield rawDatafield = ((Datafield) rawField);
			for (PropertiesObject propertiesObject : allPropertiesObjects) {
				if (rawDatafield.isContainedInPropertiesObject(propertiesObject)) {
					relevantPropertiesObjects.add(propertiesObject);
					//System.out.println(rawField.toString() + " is contained in " + propertiesObject.getDatafields().toString());
				}
			}
		}

		// If relevantPropertiesObjects is empty, this means that no corresponding PropertiesObject was found for the raw field from the MarcXML
		// In this case we will return null. This helps us for error management.
		if (relevantPropertiesObjects.isEmpty()) {
			relevantPropertiesObjects = null;
		}

		return relevantPropertiesObjects;
	}


	public List<Record> getRawRecords() {
		return rawRecords;
	}

	public void setRawRecords(List<Record> rawRecords) {
		this.rawRecords = rawRecords;
	}

	public List<PropertiesObject> getMatchingObjects() {
		return allPropertiesObjects;
	}

	public void setMatchingObjects(List<PropertiesObject> propertiesObjects) {
		this.allPropertiesObjects = propertiesObjects;
	}

	public List<Record> getMatchingResult() {
		return this.matching(); // The matching() method sets the matching result
	}

}