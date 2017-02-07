/**
 * Matching raw XML fields to respective Solr fields.
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

	private List<RawRecord> rawRecords;
	private List<PropertiesObject> allPropertiesObjects;


	public MatchingOperations() {}

	public MatchingOperations(List<RawRecord> rawRecords, List<PropertiesObject> allPropertiesObjects) {
		this.rawRecords = rawRecords;
		this.allPropertiesObjects = allPropertiesObjects;
	}


	/**
	 * Match a List of RawRecord objects to a List of of SolrRecord objects.
	 * 
	 * @return	List<SolrRecord>: The SolrRecord objects with the processed data.
	 */
	private List<SolrRecord> matching() {

		List<SolrRecord> matchingResult = new ArrayList<SolrRecord>();

		for (RawRecord rawRecord : rawRecords) {

			List<SolrField> allSolrFieldsOfRecord = new ArrayList<SolrField>();

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
			for (Entry<String, SolrField> consolidatedSolrField : consolidatedSolrFields.entrySet()) {
				SolrField solrfield = (SolrField)consolidatedSolrField.getValue();
				if (!solrfield.isMultivalued()) {
					String strFirstValue = solrfield.getFieldvalues().get(0); // Get first value of SolrField values and use it as the value for indexing
					ArrayList<String> firstValue = new ArrayList<String>();
					firstValue.add(strFirstValue);
					solrfield.setFieldvalues(firstValue);
				}
			}

			// Handle allowDuplicates
			for (Entry<String, SolrField> consolidatedSolrField : consolidatedSolrFields.entrySet()) {
				SolrField solrfield = (SolrField)consolidatedSolrField.getValue();
				ArrayList<String> solrfieldValues = solrfield.getFieldvalues();
				//LinkedHashSet<String> dedupSolrfieldValues = new LinkedHashSet<String>();
				ArrayList<String> dedupSolrfieldValues = new ArrayList<String>();
				if (solrfield.isMultivalued() && !solrfield.allowDuplicates() && solrfieldValues.size() > 1) {
					// Remove duplicate values because they are not allowed for these Solr fields
					for (String fieldValue : solrfieldValues) {
						if (!dedupSolrfieldValues.contains(fieldValue)) {
							dedupSolrfieldValues.add(fieldValue);
						}
					}
					solrfield.setFieldvalues(dedupSolrfieldValues);
				}
			}

			// Convert TreeMap<String, SolrField> to List<SolrField>
			ArrayList<SolrField> solrfields = new ArrayList<SolrField>();
			for (Entry<String, SolrField> consolidatedSolrField : consolidatedSolrFields.entrySet()) {
				SolrField solrfield = (SolrField)consolidatedSolrField.getValue();
				solrfields.add(solrfield);
			}


			SolrRecord solrRecord = new SolrRecord(rawRecord.getRecordID(), rawRecord.getRecordSYS(), rawRecord.getIndexTimestamp(), solrfields, rawRecord.getFullRecord());
			matchingResult.add(solrRecord);
		}

		return matchingResult;
	}



	/**
	 * Matching a raw field from the MarcXML file to a Solr field according to the rules in mab.properties (this file could be called
	 * differently but it must be a .properties file).
	 * 
	 * @param rawField				Datafield or Controlfield: The raw field (Datafield object or Controlfield object) that was parsed from the MarcXML record
	 * @param allPropertiesObjects	List<PropertiesObject>: All rules from the mab.properties file as a list of PropertiesObject objects
	 * @return						List<SolrField>: A list of one or multiple SolrField object(s). Each raw field could match to none, one or mutliple Solr field(s). 
	 */
	private List<SolrField> matchField(Object rawField, List<PropertiesObject> allPropertiesObjects) {

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
			boolean allowDuplicates = relevantPropertiesObject.isAllowDuplicates();
			boolean hasApplyToFields = (relevantPropertiesObject.getApplyToFields() != null && !relevantPropertiesObject.getApplyToFields().isEmpty()) ? true : false;

			
			
			
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
				solrField.setAllowDuplicates(allowDuplicates);

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
							if (regexedStrictValue != null) {
								fieldValues.add(regexedStrictValue);
							}
						}
						if (type.equals("datafield")) {
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String rawFieldvalue = subfield.getContent();
								String regexedStrictValue = getRegexStrictValue(regexStrictPattern, rawFieldvalue);
								//System.out.println("regexedStrictValue: " + regexedStrictValue);
								if (regexedStrictValue != null) {
									fieldValues.add(regexedStrictValue);
								}
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

					// Handle the combination between connectedSubfields and concatenatedSubfields, but only if we do not have a translate value. Translations are treated differently for connectedSubfields and concatenatedSubfields.
					// This can only apply to datafields, not to controlfields (they do not have any subfields).
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasConnectedSubfields && hasConcatenatedSubfields)) {
						if (type.equals("datafield")) {
														
							// Get concatenated and connected subfields and the separator for concatenated subfiels
							ArrayList<String> connectedSubfields = getConnectedSubfields(copiedDatafield, relevantPropertiesObject);
							String concatenatedSubfieldsSeparator = relevantPropertiesObject.getConcatenatedSubfieldsSeparator();
							ArrayList<String> concatenatedValues = getConcatenatedSubfields(copiedDatafield, relevantPropertiesObject);

							// Iterate over each subfield
							for (Subfield subfield : copiedDatafield.getSubfields()) {
								String valueToAdd = null;
								String rawFieldvalue = subfield.getContent(); // Get the raw field value

								if (concatenatedValues != null) { // Concatenate the subfields
									String concatenatedValue = StringUtils.join(concatenatedValues, concatenatedSubfieldsSeparator); // Join concatenated value(s) with the given separator character
									valueToAdd = rawFieldvalue + concatenatedSubfieldsSeparator + concatenatedValue; // Add the standard field value in front of the concatenated value(s), separated by the given separator character
								} else {
									valueToAdd = rawFieldvalue; // If there are not values for concatenation, add the original subfield content.
								}
								
								connectedSubfields.add(0, valueToAdd); // Add the concatenated value to the first position of the ArrayList							
								
								// Add the connected values (that contains the concatenated value) to Solr
								for (String connectedSubfieldValue : connectedSubfields) {
									fieldValues.add(connectedSubfieldValue);
								}
							}
						}
					}
					
					// Handle connectedSubfields (not combined with concatenatedSubfields), but only if we do not have a translate value. Translations are treated differently for connectedSubfields.
					// This can only apply to datafields, not to controlfields (they do not have any subfields).
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasConnectedSubfields && !hasConcatenatedSubfields)) {
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

					// Handle concatenatedSubfields (not combined with connectedSubfields), but only if we do not have a translate value. Translations are treated differently for concatenatedSubfields.
					// This can only apply to datafields, not to controlfields (they do not have any subfields).
					if ((!isTranslateValue && !isTranslateValueContains && !isTranslateValueRegex) && (hasConcatenatedSubfields && !hasConnectedSubfields)) {
						if (type.equals("datafield")) {

							Datafield datafieldToUse = copiedDatafield;
							boolean apply = true;
							if (hasApplyToFields) {
								apply = false;
								List<String> applyToFields = relevantPropertiesObject.getApplyToFields().get("concatenatedSubfields");
								List<Datafield> applyToFieldsAsDatafields = getApplyToFieldsAsDatafields(applyToFields);

								for (Datafield applyToFieldAsDatafield : applyToFieldsAsDatafields) {
									if (copiedDatafield.match(applyToFieldAsDatafield)) { // Get the subfields that are concerned for the rule

										// If we have an "applyToFields" option we need to move all subfields in the bracket of concatenatedSubfields[z:h:x:\\, ] to
										// "passiveSubfields". In this example this would be subfields "z", "h" and "x". This is because there could be still an "active"
										// subfield "z", "h" or "x" that would not be used for concatenating and instead would be treated as a "main" subfield.
										// This has to be done BEFORE getConcatenatedSubfields!

										// Copy again the datafield to which to rule should be applied as we may need the other datafield
										// with the subfields we will move for other operations.
										Datafield applyToDatafield = Datafield.copy(copiedDatafield);
										

										// Get all "subfieldcode" from the bracket of concatenatedSubfields[subfieldcode:subfieldcode:subfieldcode:separator]
										List<String> subfieldsInBracketImmutable = Arrays.asList(relevantPropertiesObject.getConcatenatedSubfields().get(1).split("\\s*:\\s*"));
										List<String> subfieldsInBracketMutable = new ArrayList<String>();
										subfieldsInBracketMutable.addAll(subfieldsInBracketImmutable); // Create CHANGEABLE/MUTABLE List
										int lastListElement = (subfieldsInBracketMutable.size()-1);
										subfieldsInBracketMutable.remove(lastListElement); // Remove separator value from MUTABLE List (this would not be possible for immutable Lists)

										// Get the subfields in the brackets (e. g. "z", "h" or "x" in concatenatedSubfields[z:h:x:\\, ]) from the applyToDatafields as ArrayList<Subfield>
										ArrayList<Subfield> subfieldsToMove = applyToDatafield.getSubfieldsByCodes(subfieldsInBracketMutable);

										// Move the subfields to passive subfields
										applyToDatafield.moveToPassiveSubfields(subfieldsToMove);

										datafieldToUse = applyToDatafield;
										apply = true;
									}
								}
							}

							ArrayList<String> concatenatedValues = getConcatenatedSubfields(datafieldToUse, relevantPropertiesObject);
							String concatenatedSubfieldsSeparator = relevantPropertiesObject.getConcatenatedSubfieldsSeparator();
							for (Subfield subfield : datafieldToUse.getSubfields()) {
								String valueToAdd = null;
								String rawFieldvalue = subfield.getContent();
								if (apply) {
									if (concatenatedValues != null) {
										String concatenatedValue = StringUtils.join(concatenatedValues, concatenatedSubfieldsSeparator); // Join concatenated value(s) with the given separator character
										valueToAdd = rawFieldvalue + concatenatedSubfieldsSeparator + concatenatedValue; // Add the standard field value in front of the concatenated value(s), separated by the given separator character
									} else {
										valueToAdd = rawFieldvalue; // If there are not values for concatenation, add the original subfield content.
									}
								} else {
									valueToAdd = rawFieldvalue; // If the rule should not be applied to this subfield, add the original subfield content.
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
				}
			}
		}

		return solrFields;
	}


	/**
	 * Get the relevant PropertiesObject objects for the given raw field (Controlfield or Datafield). All PropertiesObject objects that do
	 * not match to a raw field (Controlfield or Datafield) will would produce an overhead and would waste a lot of ressources and time.
	 * 
	 * @param type						String: Only "controlfield" or "datafield" are possible
	 * @param rawField					Controlfield or Datafield: A Controlfield object or a Datafield object
	 * @param allPropertiesObjects		List<PropertiesObject>: A list of all PropertiesObject objects
	 * @return							List<PropertiesObject>: A list of all relevant PropertiesObject objects
	 */
	private List<PropertiesObject> getRelevantPropertiesObjects(String type, Object rawField, List<PropertiesObject> allPropertiesObjects) {

		List<PropertiesObject> relevantPropertiesObjects = new ArrayList<PropertiesObject>();

		// Remove unnecessary Controlfields from PropertiesObjects
		if (type.equals("controlfield")) {
			Controlfield rawControlfield = ((Controlfield) rawField);
			for (PropertiesObject propertiesObject : allPropertiesObjects) {
				if (rawControlfield.isContainedInPropertiesObject(propertiesObject)) {
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


	/**
	 * Getting the translated value that was translated with the help of a translation file.
	 * 
	 * @param solrFieldname								String: Name of Solr field 
	 * @param rawFieldname								Raw fieldname, e. g. 100$ab$c
	 * @param rawFieldvalue								Raw fieldvalue from XML record
	 * @param translateProperties						Map<String, String>: Contents of a translation.properties file
	 * @param fromCount									String: Index of first character to match or "all"
	 * @param toCount									String: Index of last character to match or "all"
	 * @param translateDefaultValue						String
	 * @param isTranslateValue							boolean
	 * @param isTranslateValueContains					boolean
	 * @param isTranslateValueRegex						boolean
	 * @param hasRegex									boolean
	 * @param regexValue								String
	 * @param hasRegexStrict							boolean
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


	/**
	 * Get a regexed value. If the regex does not match, return the original value (= rawFieldvalue)
	 * 
	 * @param regexPattern		String: The regex pattern
	 * @param rawFieldvalue		String: The value that should be regexed
	 * @return					String: The regexed value or the rawFieldvalue if the regex does not match
	 */
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


	/**
	 * Get a regexed value. If the regex does not match, return null.
	 * @param regexPattern		String: The regex pattern
	 * @param rawFieldvalue		String: The value that should be regexed
	 * @return					String: The regexed value or null if the regex does not match
	 */
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
	 * Get all connected subfields for a datafield.
	 * 
	 * @param datafield					Datafield: The Datafield object for which we want to get the connected subfields.
	 * @param relevantPropertiesObject	PropertiesObject: The PropertiesObject object that contains the rules for the connected subfields.
	 * @return							ArrayList<String>: A list of the connected subfields in the right order.
	 */
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


	/**
	 * Get all concatenated subfields for a datafield.
	 * 
	 * @param datafield					Datafield: The Datafield object for which we want to get the concatenated subfields.
	 * @param relevantPropertiesObject	PropertiesObject: The PropertiesObject object that contains the rules for the concatenated subfields.
	 * @return							ArrayList<String>: A list of the concatenated subfields in the right order.
	 */
	private ArrayList<String> getConcatenatedSubfields(Datafield datafield, PropertiesObject relevantPropertiesObject) {
		ArrayList<String> returnValue = new ArrayList<String>();
		LinkedHashMap<Integer, String> concatenatedSubfields = relevantPropertiesObject.getConcatenatedSubfields();
		boolean isTranslateConcatenatedSubfields = relevantPropertiesObject.isTranslateConcatenatedSubfields();

		for (Entry<Integer, String> concatenatedSubfield : concatenatedSubfields.entrySet()) {
			List<String> immutableList = Arrays.asList(concatenatedSubfield.getValue().split("\\s*:\\s*"));
			List<String> concatenatedSubfieldsCodes = new ArrayList<String>();
			concatenatedSubfieldsCodes.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
			int lastListElement = (concatenatedSubfieldsCodes.size()-1); // Get index of last List element
			concatenatedSubfieldsCodes.remove(lastListElement); // Remove the separator value so that only the subfield codes will remain
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



	/**
	 * Check if a field should be skipped because one or more given subfields do exist or not exist.
	 * 
	 * @param datafield						Datafield: The Datafield object for which we want to check if we skip it or not.
	 * @param relevantPropertiesObject		PropertiesObject: The PropertiesObject object that contains the rules for skiping fields.
	 * @param isExists						boolean: True if we need to check for the existance of a subfield, false otherwise (see also isNotExists)
	 * @param isNotExists					boolean: True if we need to check for the non-existance of a subfield, false otherwise (see also isExists)
	 * @return								boolean: True if the field should be skipped (will not be added to the index), false otherwise (will be added to the index)
	 */
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
			} else if (isNotExists && passiveSubfieldCodes.containsAll(subfieldCodesToCheck)) {
				skipField = true;
			}
		} else if (operator.equals("OR")) {
			if (isExists && Collections.disjoint(passiveSubfieldCodes, subfieldCodesToCheck)) {
				skipField = true;
			} else if (isNotExists && !Collections.disjoint(passiveSubfieldCodes, subfieldCodesToCheck)) {
				skipField = true;
			}
		}

		return skipField;
	}

	
	/**
	 * Get a list of Datafield object from a List of fieldnames.
	 * 
	 * @param applyToFields		List<String>: A list of fieldnames
	 * @return					List<Datafield>: A list of datafield objects
	 */
	private List<Datafield> getApplyToFieldsAsDatafields(List<String> applyToFields) {
		List<Datafield> applyToFieldsAsDatafields = new ArrayList<Datafield>();
		for (String applyToField : applyToFields) {			
			Datafield datafield = new Datafield();
			Subfield subfield = new Subfield();
			ArrayList<Subfield> subfields = new ArrayList<Subfield>();
			String tag = applyToField.substring(0, 3);
			String ind1 = applyToField.substring(4, 5);
			String ind2 = applyToField.substring(5, 6);
			String subfieldCode = applyToField.substring(7, 8);
			subfield.setCode(subfieldCode);
			subfields.add(subfield);
			datafield.setTag(tag);
			datafield.setInd1(ind1);
			datafield.setInd2(ind2);
			datafield.setSubfields(subfields);
			applyToFieldsAsDatafields.add(datafield);
		}

		return applyToFieldsAsDatafields;
	}


	public List<RawRecord> getRawRecords() {
		return rawRecords;
	}

	public void setRawRecords(List<RawRecord> rawRecords) {
		this.rawRecords = rawRecords;
	}

	public List<PropertiesObject> getPropertiesObjects() {
		return allPropertiesObjects;
	}

	public void setPropertiesObjects(List<PropertiesObject> propertiesObjects) {
		this.allPropertiesObjects = propertiesObjects;
	}

	public List<SolrRecord> getSolrRecords() {
		return this.matching(); // The matching() method sets the result that should be returnd to MarcContentHandler for indexing.
	}

}