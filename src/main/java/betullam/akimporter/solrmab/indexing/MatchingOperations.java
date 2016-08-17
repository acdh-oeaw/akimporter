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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import main.java.betullam.akimporter.solrmab.Index;

public class MatchingOperations {

	// ################ OLD #################
	//private List<Mabfield> listOfMatchedFields;
	//private List<Record> newListOfRecords;
	//private List<List<Mabfield>> listOfNewSolrfieldLists;
	//private List<Mabfield> listOfNewFieldsForNewRecord;
	//private Record newRecord;
	List<String> multiValuedFields = Index.multiValuedFields;
	//List<SolrField> customTextFields = Index.customTextFields;
	//Map<String, List<String>> translateFields = Index.translateFields;
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
			List<SolrField> allSolrFieldsOfRecord = new ArrayList<SolrField>();
			List<SolrField> newSolrFields = new ArrayList<SolrField>(); 

			// Handle controlfields
			for (Controlfield rawControlfield : rawRecord.getControlfields()) {
				List<SolrField> solrFieldsFromControlfields = this.matchField(rawControlfield, allPropertiesObjects);
				if (solrFieldsFromControlfields != null) {
					allSolrFieldsOfRecord.addAll(solrFieldsFromControlfields);
				}
			}

			// Handle datafields
			for (Datafield rawDatafield : rawRecord.getDatafields()) {
				List<SolrField> solrFieldsFromDatafields = this.matchField(rawDatafield, allPropertiesObjects);
				if (solrFieldsFromDatafields != null) {
					allSolrFieldsOfRecord.addAll(solrFieldsFromDatafields);
				}
			}

			// Handle customText
			if (Index.customTextFields != null) {
				allSolrFieldsOfRecord.addAll(Index.customTextFields);
			}

			// Consolidate SolrFields (put values for same SolrField in one Map)
			Map<String, SolrField> consolidatedSolrFields = new TreeMap<String, SolrField>();
			for (SolrField solrfield : allSolrFieldsOfRecord) {
				if (!consolidatedSolrFields.containsKey(solrfield.getFieldname())) { // Its not already in our Map!
					consolidatedSolrFields.put(solrfield.getFieldname(), solrfield);
				} else { // Its already in our Map! Just add the value to the existing SolrField
					SolrField existingSolrField = consolidatedSolrFields.get(solrfield.getFieldname());
					for (String fieldvalueToAdd : solrfield.getFieldvalues()) {
						existingSolrField.getFieldvalues().add(fieldvalueToAdd);
					}
				}
			}
			
			// Handle non-multivalued fields
			for (Entry consolidatedSolrField : consolidatedSolrFields.entrySet()) {
				SolrField solrfield = (SolrField)consolidatedSolrField.getValue();
				if (!solrfield.isMultivalued()) {
					String strFirstValue = solrfield.getFieldvalues().get(0); // Get first value of SolrField values and use it as the value for indexing
					ArrayList<String> firstValue = new ArrayList<String>();
					firstValue.add(strFirstValue);
					solrfield.setFieldvalues(firstValue);
				}
			}
			
			// TODO: Handle allowDuplicates
			
			
			for (Entry<String, SolrField> entry : consolidatedSolrFields.entrySet()) {
				System.out.println(entry.toString());
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



		for (PropertiesObject relevantPropertiesObject : relevantPropertiesObjects) {

			String solrFieldname = relevantPropertiesObject.getSolrFieldname();
			ArrayList<String> fieldValues = new ArrayList<String>();
			boolean hasRegex = relevantPropertiesObject.hasRegex();
			String regexPattern = relevantPropertiesObject.getRegexValue();
			boolean hasRegexStrict = relevantPropertiesObject.hasRegexStrict();
			String regexStrictPattern = relevantPropertiesObject.getRegexStrictValue();
			boolean hasRegexReplace = relevantPropertiesObject.hasRegExReplace();
			String regexReplacePattern = relevantPropertiesObject.getRegexReplaceValues().get(1);
			String regexReplaceValue = relevantPropertiesObject.getRegexReplaceValues().get(2);
			boolean isTranslateValue = relevantPropertiesObject.isTranslateValue();
			boolean isTranslateValueContains = relevantPropertiesObject.isTranslateValueContains();
			boolean isTranslateValueRegex = relevantPropertiesObject.isTranslateValueRegex();
			boolean hasConnectedSubfields = relevantPropertiesObject.hasConnectedSubfields();
			boolean hasConcatenatedSubfields = relevantPropertiesObject.hasConcatenatedSubfields();
			boolean hasSubfieldExists = relevantPropertiesObject.hasSubfieldExists();
			boolean hasSubfieldNotExists = relevantPropertiesObject.hasSubfieldNotExists();
			boolean skipField = false;
			boolean isMultivalued = relevantPropertiesObject.isMultiValued();

			// Check if the raw subfield, for which we want to treat it's content (apply rules on it), is listed in the properties object
			// (which represents a line in mab.properties).
			// Example:
			// 	We have an XML (raw datafield) like this:
			//		<datafield ind1="b" ind2="1" tag="100">
			//			<subfield code="p">Berri, Claude</subfield>
			//			<subfield code="d">1934-2009</subfield>
			//			<subfield code="9">(DE-588)119130823</subfield>
			//			<subfield code="b">[Drehbuch, Regie]</subfield>
			//		</datafield>
			//	We have a line in mab.properties like this:
			//		author: 100$**$p, 100$**$a, 200$**$p, 200$**$a
			//	We see that we need to only treat subfield "p" (Berri, Claude) of the raw datafield, because subfields "d", "9" and "b" are
			//	not contained in the "author"-line of mab.properties. But maybe we need the information of these additional subfields later on
			//	(e. g. for concatenating, connections, etc.). So we move them to another List<Subfield> called "passiveSubfields". We can use
			//	the subfields from there if we need them.
			// As we do not want to change the original datafield, because multiple operations (for every relevant properties object) are applied
			// on it (and for that we always need the original as a starting point), we have to make a copy to which we can apply the rules of each
			// relevant properties object.
			Datafield copiedDatafield = null;
			if (datafield != null) {
				copiedDatafield = Datafield.copy(datafield);
				ArrayList<Subfield> subfieldsToMove = new ArrayList<Subfield>();
				for (Subfield copiedSubfield : copiedDatafield.getSubfields()) {
					if (!relevantPropertiesObject.containsSubfieldOfDatafield(copiedDatafield, copiedSubfield)) {
						// Move datafields that are not used directly for indexing to a List<Subfield> called passiveSubfields.
						// We can use these subfields then later on, e. g. for concatenating, connections, skipping, etc.
						subfieldsToMove.add(copiedSubfield);
					}
				}
				if (subfieldsToMove != null && !subfieldsToMove.isEmpty()) {
					copiedDatafield.moveToPassiveSubfields(subfieldsToMove);
				}
			}


			// Handle hasSubfieldExists. This can only apply to datafields, not to controlfields (they do not have any subfields)
			if (hasSubfieldExists) {
				if (type.equals("datafield")) {
					skipField = skipField(copiedDatafield, relevantPropertiesObject, true, false);
				}
			}

			// Handle hasSubfieldNotExists. This can only apply to datafields, not to controlfields (they do not have any subfields)
			if (hasSubfieldNotExists) {
				if (type.equals("datafield")) {
					skipField = skipField(copiedDatafield, relevantPropertiesObject, false, true);
				}
			}


			if (!skipField) {
				SolrField solrField = new SolrField();
				solrField.setFieldname(solrFieldname);
				solrField.setMultivalued(isMultivalued);
				

				// Check if an option should be applied to the current field. If yes, go on an apply the option(s). If no, just use the content from the raw field as it is.
				boolean hasPropertiesOption = false;
				if (isTranslateValue || isTranslateValueContains || isTranslateValueRegex || hasRegex || hasRegexStrict || hasRegexReplace || hasConnectedSubfields || hasConcatenatedSubfields) {
					hasPropertiesOption = true;
				}

				if (hasPropertiesOption) {

					// Handle translateValue, translateValueContains and translateValueRegex (not translateConnectedSubfields and translateConcatenatedSubfields)
					if (isTranslateValue || isTranslateValueContains || isTranslateValueRegex) {

						// Treat "normal" translate values (there are also translateConnectedSubfields and translateConcatenatedSubfields which are handled elsewhere)
						Map<String, String> translateProperties = relevantPropertiesObject.getTranslateProperties();
						String defaultValue = relevantPropertiesObject.getDefaultValue();

						// Get "from" and "to" values for translation
						String fromCharacter = "all"; // Default is "all"
						String toCharacter = "all"; // Default is "all"
						for (Entry<String, List<String>> propertiesField : relevantPropertiesObject.getPropertiesFields().entrySet()) {
							fromCharacter = propertiesField.getValue().get(0);
							toCharacter = propertiesField.getValue().get(1);
						}

						if (type.equals("controlfield")) {
							String rawFieldname = controlfield.getTag();
							String rawFieldvalue = controlfield.getContent();
							String translatedValue = getTranslatedValue(solrFieldname, rawFieldname, rawFieldvalue, translateProperties, fromCharacter, toCharacter, defaultValue, isTranslateValue, isTranslateValueContains, isTranslateValueRegex, hasRegex, regexPattern, hasRegexStrict, regexStrictPattern, hasRegexReplace, regexReplacePattern, regexReplaceValue, false);
							//System.out.println("translatedValue: " + translatedValue);
							if (translatedValue != null) {
								fieldValues.add(translatedValue);
							}
						}
						if (type.equals("datafield")) {
							String tag = copiedDatafield.getTag();
							String ind1 = copiedDatafield.getInd1();
							String ind2 = copiedDatafield.getInd2();
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String rawFieldname = tag+"$"+ind1+ind2+"$"+subfield.getCode();
								String rawFieldvalue = subfield.getContent();
								String translatedValue = getTranslatedValue(solrFieldname, rawFieldname, rawFieldvalue, translateProperties, fromCharacter, toCharacter, defaultValue, isTranslateValue, isTranslateValueContains, isTranslateValueRegex, hasRegex, regexPattern, hasRegexStrict, regexStrictPattern, hasRegexReplace, regexReplacePattern, regexReplaceValue, false);
								//System.out.println("translatedValue: " + translatedValue);
								if (translatedValue != null) {
									fieldValues.add(translatedValue);
								}
							}
						}
					}


					// Handle regEx, but only if we do not have a translate value. For values that needs to be translated, regexing is done within the translation process.
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasRegex && regexPattern != null)) {
						if (type.equals("controlfield")) {
							String rawFieldvalue = controlfield.getContent();
							String regexedValue = getRegexValue(regexPattern, rawFieldvalue);
							//System.out.println("regexedValue: " + regexedValue);
							fieldValues.add(regexedValue);
						}
						if (type.equals("datafield")) {
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String rawFieldvalue = subfield.getContent();
								String regexedValue = getRegexValue(regexPattern, rawFieldvalue);
								//System.out.println("regexedValue: " + regexedValue);
								fieldValues.add(regexedValue);
							}
						}
					}


					// Handle regExStrict, but only if we do not have a translate value. For values that needs to be translated, regexing is done within the translation process.
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasRegexStrict && regexStrictPattern != null)) {
						if (type.equals("controlfield")) {
							String rawFieldvalue = controlfield.getContent();
							String regexedStrictValue = getRegexStrictValue(regexStrictPattern, rawFieldvalue);
							//System.out.println("regexedStrictValue: " + regexedStrictValue);
							fieldValues.add(regexedStrictValue);
						}
						if (type.equals("datafield")) {
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String rawFieldvalue = subfield.getContent();
								String regexedStrictValue = getRegexStrictValue(regexStrictPattern, rawFieldvalue);
								//System.out.println("regexedStrictValue: " + regexedStrictValue);
								fieldValues.add(regexedStrictValue);
							}
						}
					}


					// Handle regExReplace, but only if we do not have a translate value. For values that needs to be translated, regexing is done within the translation process.
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasRegexReplace && regexReplacePattern != null && !regexReplacePattern.isEmpty())) {
						if (type.equals("controlfield")) {
							String rawFieldvalue = controlfield.getContent();
							String regexedReplaceValue = rawFieldvalue.replaceAll(regexReplacePattern, regexReplaceValue).trim();
							//System.out.println("regexedReplaceValue: " + regexedReplaceValue);
							fieldValues.add(regexedReplaceValue);
						}
						if (type.equals("datafield")) {
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String rawFieldvalue = subfield.getContent();
								String regexedReplaceValue = rawFieldvalue.replaceAll(regexReplacePattern, regexReplaceValue).trim();
								//System.out.println("regexedReplaceValue: " + regexedReplaceValue);
								fieldValues.add(regexedReplaceValue);
							}
						}
					}


					// Handle connectedSubfields, but only if we do not have a translate value. Translations are treated differently for connectedSubfields.
					// This can only apply to datafields, not to controlfields (they do not have any subfields).
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasConnectedSubfields)) {
						if (type.equals("datafield")) {
							ArrayList<String> connectedSubfields = getConnectedSubfields(copiedDatafield, relevantPropertiesObject);
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String rawFieldvalue = subfield.getContent();
								connectedSubfields.add(0, rawFieldvalue);
								//System.out.println("connectedSubfields: " + connectedSubfields);
								for (String connectedSubfieldValue : connectedSubfields) {
									fieldValues.add(connectedSubfieldValue);
								}

							}
						}
					}


					// Handle concatenatedSubfields, but only if we do not have a translate value. Translations are treated differently for concatenatedSubfields.
					// This can only apply to datafields, not to controlfields (they do not have any subfields).
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasConcatenatedSubfields)) {
						if (type.equals("datafield")) {
							ArrayList<String> concatenatedValues = getConcatenatedSubfields(copiedDatafield, relevantPropertiesObject);
							String concatenatedSubfieldsSeparator = relevantPropertiesObject.getConcatenatedSubfieldsSeparator();
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String valueToAdd = null;
								String rawFieldvalue = subfield.getContent();
								if (concatenatedValues != null) {
									String concatenatedValue = StringUtils.join(concatenatedValues, concatenatedSubfieldsSeparator); // Join concatenated value(s) with the given separator character
									valueToAdd = rawFieldvalue + concatenatedSubfieldsSeparator + concatenatedValue; // Add the standard field value in front of the concatenated value(s), separated by the given separator character
								} else {
									valueToAdd = rawFieldvalue; // If there are not values for concatenation, add the original subfield content.
								}
								fieldValues.add(valueToAdd);
							}
						}
					}
				} else { // Add all contents from the raw fields that don't need to be treated because no option is specified
					if (type.equals("controlfield")) {
						fieldValues.add(controlfield.getContent());
					}
					if (type.equals("datafield")) {
						for (Subfield subfield : copiedDatafield.getSubfields()) {
							fieldValues.add(subfield.getContent());
						}
					}
				}


				if (solrField != null && !fieldValues.isEmpty()) {
					solrField.setFieldvalues(fieldValues);
					solrFields.add(solrField);
					//System.out.println(solrField);
				}
			}

		}

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
	 * @param regexValue								String
	 * @param hasRegexStrict							hasRegexStrict
	 * @param regexStrictValue							String
	 * @param hasRegexReplace							boolean
	 * @param regexReplacePattern						String
	 * @param regexReplaceValue							String
	 * @param useRawFieldvalueIfNoMatch					boolean
	 * @return											String containing the translated value
	 */
	private String getTranslatedValue(String solrFieldname, String rawFieldname, String rawFieldvalue, Map<String, String> translateProperties, String fromCount, String toCount, String translateDefaultValue, boolean isTranslateValue, boolean isTranslateValueContains, boolean isTranslateValueRegex, boolean hasRegex, String regexValue, boolean hasRegexStrict, String regexStrictValue, boolean hasRegexReplace, String regexReplacePattern, String regexReplaceValue, boolean useRawFieldvalueIfNoMatch) {

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
		if (translateValue == null) {
			if (rawFieldvalue != null && useRawFieldvalueIfNoMatch) {
				translateValue = rawFieldvalue;
			} else if (translateDefaultValue != null && !useRawFieldvalueIfNoMatch) {
				translateValue = translateDefaultValue;
			}
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
	/*
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
	 */

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


	private ArrayList<String> getConnectedSubfields(Datafield datafield, PropertiesObject relevantPropertiesObject) {
		ArrayList<String> returnValue = new ArrayList<String>();
		String connectedDefaultValue = null;
		LinkedHashMap<Integer, String> connectedSubfields = relevantPropertiesObject.getConnectedSubfields();
		boolean isTranslateConnectedSubfields = relevantPropertiesObject.isTranslateConnectedSubfields();

		for (Entry<Integer, String> connectedSubfield : connectedSubfields.entrySet()) {
			List<String> immutableList = Arrays.asList(connectedSubfield.getValue().split("\\s*:\\s*"));
			List<String> connectedSubfieldsCodes = new ArrayList<String>();
			connectedSubfieldsCodes.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
			int lastListElement = (connectedSubfieldsCodes.size()-1); // Get index of last List element
			connectedDefaultValue = connectedSubfieldsCodes.get(lastListElement); // Last value is always the default value to use
			connectedSubfieldsCodes.remove(lastListElement); // Remove the default value so that only the subfield codes will remain

			String textToUse = null;
			for (String connectedSubfieldsCode : connectedSubfieldsCodes) {
				for (Subfield passiveSubfield : datafield.getPassiveSubfields()) {
					String passiveSubfieldCode = passiveSubfield.getCode();
					if (connectedSubfieldsCode.equals(passiveSubfieldCode)) {
						String subfieldContent = passiveSubfield.getContent();
						if (subfieldContent != null && !subfieldContent.isEmpty()) {
							// Set text only if textToUse is not null. Otherwise we would overwrite a value that was added in a loop before.
							if (textToUse == null) {
								textToUse = subfieldContent;
							}
						}
					}
				}
			}

			// Set default value if no other value was found
			if (textToUse == null) {
				textToUse = connectedDefaultValue;
			}
			
			if (isTranslateConnectedSubfields) {
				String solrFieldname = relevantPropertiesObject.getSolrFieldname();
				Map<String, String> translateConnectedSubfieldsProperties = relevantPropertiesObject.getTranslateConnectedSubfieldsProperties();
				textToUse = this.getTranslatedValue(solrFieldname, null, textToUse, translateConnectedSubfieldsProperties, "all", "all", connectedDefaultValue, true, false, false, false, null, false, null, false, null, null, true);

			}

			returnValue.add(textToUse);
		}

		return returnValue;
	}


	private ArrayList<String> getConcatenatedSubfields(Datafield datafield, PropertiesObject relevantPropertiesObject) {
		ArrayList<String> returnValue = new ArrayList<String>();
		LinkedHashMap<Integer, String> concatenatedSubfields = relevantPropertiesObject.getConcatenatedSubfields();
		boolean isTranslateConcatenatedSubfields = relevantPropertiesObject.isTranslateConcatenatedSubfields();

		for (Entry<Integer, String> concatenatedSubfield : concatenatedSubfields.entrySet()) {
			List<String> immutableList = Arrays.asList(concatenatedSubfield.getValue().split("\\s*:\\s*"));
			List<String> concatenatedSubfieldsCodes = new ArrayList<String>();
			concatenatedSubfieldsCodes.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
			int lastListElement = (concatenatedSubfieldsCodes.size()-1); // Get index of last List element
			concatenatedSubfieldsCodes.remove(lastListElement); // Remove the default value so that only the subfield codes will remain

			for (String concatenatedSubfieldsCode : concatenatedSubfieldsCodes) {
				for (Subfield passiveSubfield : datafield.getPassiveSubfields()) {
					String passiveSubfieldCode = passiveSubfield.getCode();
					if (concatenatedSubfieldsCode.equals(passiveSubfieldCode)) {
						String textToUse = passiveSubfield.getContent(); // This is the text for the concatenation
						if (textToUse != null && !textToUse.isEmpty()) {
							// Handle translation if needed
							if (isTranslateConcatenatedSubfields) {
								String solrFieldname = relevantPropertiesObject.getSolrFieldname();
								Map<String, String> translateConcatenatedSubfieldsProperties = relevantPropertiesObject.getTranslateConcatenatedSubfieldsProperties();
								textToUse = this.getTranslatedValue(solrFieldname, null, textToUse, translateConcatenatedSubfieldsProperties, "all", "all", textToUse, true, false, false, false, null, false, null, false, null, null, true);
							}
							returnValue.add(textToUse);
						}
					}
				}
			}
		}

		if (returnValue.isEmpty()) {
			returnValue = null;
		}

		return returnValue;
	}



	private boolean skipField(Datafield datafield, PropertiesObject relevantPropertiesObject, boolean isExists, boolean isNotExists) {
		boolean skipField = false;
		LinkedHashMap<Integer, String> subfieldsToCheck = null;

		if (isExists) {
			subfieldsToCheck = relevantPropertiesObject.getSubfieldExists();
		} else if (isNotExists) {
			subfieldsToCheck = relevantPropertiesObject.getSubfieldNotExists();
		}

		// Get all relevant data (subfields to check, operator) from the properties object
		String operator = "AND"; // Default operator
		String strSubfieldToCheck = subfieldsToCheck.get(1); // There is only one entry because we do not have multiple square brackets with subfieldExists
		List<String> immutableList = Arrays.asList(strSubfieldToCheck.split("\\s*:\\s*"));
		List<String> subfieldCodesToCheck = new ArrayList<String>();
		subfieldCodesToCheck.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
		int lastListElement = (subfieldCodesToCheck.size()-1); // Get index of last List element
		if (subfieldCodesToCheck.get(lastListElement).equals("AND") || subfieldCodesToCheck.get(lastListElement).equals("OR")) {
			operator = subfieldCodesToCheck.get(lastListElement); // Get the operator (should be last list element)
			subfieldCodesToCheck.remove(lastListElement); // Remove the operator so that only the subfield codes will remain
		}

		// Get all subfield codes from the passive subfields:
		ArrayList<String> passiveSubfieldCodes = new ArrayList<String>();
		for (Subfield passiveSubfield : datafield.getPassiveSubfields()) {
			passiveSubfieldCodes.add(passiveSubfield.getCode());
		}

		// Check if a passive subfields exists in the list of subfields from the user
		if (operator.equals("AND")) {
			if (isExists && !passiveSubfieldCodes.containsAll(subfieldCodesToCheck)) {
				skipField = true;
				//System.out.println("Exists, AND    : " + passiveSubfieldCodes + " (from XML) contains NOT all " + subfieldCodesToCheck + " (from props)");
			} else if (isNotExists && passiveSubfieldCodes.containsAll(subfieldCodesToCheck)) {
				skipField = true;
				//System.out.println("NOT Exists, AND: " + passiveSubfieldCodes + " (from XML) contains all " + subfieldCodesToCheck + " (from props)");
			}
		} else if (operator.equals("OR")) {
			if (isExists && Collections.disjoint(passiveSubfieldCodes, subfieldCodesToCheck)) {
				skipField = true;
				//System.out.println("Exists, OR     : " + passiveSubfieldCodes + " (from XML) contains no subfield from " + subfieldCodesToCheck + " (from props)");
			} else if (isNotExists && !Collections.disjoint(passiveSubfieldCodes, subfieldCodesToCheck)) {
				skipField = true;
				//System.out.println("NOT Exists, OR : " + passiveSubfieldCodes + " (from XML) contains at least one subfield from " + subfieldCodesToCheck + " (from props)");
			}
		}

		return skipField;
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