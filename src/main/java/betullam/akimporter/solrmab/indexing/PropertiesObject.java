/**
 * PropertiesObject class.
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PropertiesObject {

	private String solrFieldname;
	private Map<String, List<String>> propertiesFields;
	private Leader leader;
	private List<Datafield> datafields;
	private List<Controlfield> controlfields;
	private boolean multiValued;
	private boolean customText;
	private boolean getAllFields;
	private List<String> allFieldsExceptions;
	private boolean getFullRecordAsXML;
	private boolean enrichSet;
	private boolean enrichAdd;
	private boolean translateValue;
	private boolean translateValueContains;
	private boolean translateValueRegex;
	private Map<String, String> translateProperties;
	private boolean hasDefaultValue;
	private String defaultValue;
	private boolean hasConnectedSubfields;
	private LinkedHashMap<Integer, String> connectedSubfields;
	private boolean translateConnectedSubfields;
	private Map<String, String> translateConnectedSubfieldsProperties;
	private boolean hasConcatenatedSubfields;
	private LinkedHashMap<Integer, String> concatenatedSubfields;
	private String concatenatedSubfieldsSeparator;
	private boolean translateConcatenatedSubfields;
	private Map<String, String> translateConcatenatedSubfieldsProperties;
	private boolean hasRegex;
	private String regexValue;
	private boolean hasRegexStrict;
	private String regexStrictValue;
	private boolean hasRegExReplace;
	private Map<Integer, String> regexReplaceValues;
	private boolean allowDuplicates;
	private boolean hasSubfieldExists;
	private LinkedHashMap<Integer, String> subfieldExists;
	private boolean hasSubfieldValueExists;
	private LinkedHashMap<Integer, String> subfieldValueExists;
	private boolean hasSubfieldNotExists;
	private LinkedHashMap<Integer, String> subfieldNotExists;
	LinkedHashMap<String, List<String>> applyToFields;


	public PropertiesObject() {}

	public PropertiesObject(
			String solrFieldname,
			Map<String, List<String>> propertiesFields,
			Leader leader,
			List<Datafield> datafields,
			List<Controlfield> controlfields,
			boolean multiValued,
			boolean customText,
			boolean getAllFields,
			List<String> allFieldsExceptions,
			boolean getFullRecordAsXML,
			boolean enrichSet,
			boolean enrichAdd,
			boolean translateValue,
			boolean translateValueContains,
			boolean translateValueRegex,
			Map<String, String> translateProperties,
			boolean hasDefaultValue,
			String defaultValue,
			boolean hasConnectedSubfields,
			LinkedHashMap<Integer, String> connectedSubfields,
			boolean translateConnectedSubfields,
			Map<String, String> translateConnectedSubfieldsProperties,
			boolean hasConcatenatedSubfields,
			LinkedHashMap<Integer, String> concatenatedSubfields,
			String concatenatedSubfieldsSeparator,
			boolean translateConcatenatedSubfields,
			Map<String, String> translateConcatenatedSubfieldsProperties,
			boolean hasRegex,
			String regexValue,
			boolean hasRegexStrict,
			String regexStrictValue,
			boolean hasRegExReplace,
			Map<Integer, String> regexReplaceValues,
			boolean allowDuplicates,
			boolean hasSubfieldExists,
			LinkedHashMap<Integer, String> subfieldExists,
			boolean hasSubfieldValueExists,
			LinkedHashMap<Integer, String> subfieldValueExists,
			boolean hasSubfieldNotExists,
			LinkedHashMap<Integer, String> subfieldNotExists,
			LinkedHashMap<String, List<String>> applyToFields
			) {

		this.setSolrFieldname(solrFieldname);
		this.setPropertiesFields(propertiesFields);
		this.setLeader(leader);
		this.setDatafields(datafields);
		this.setControlfields(controlfields);
		this.setMultiValued(multiValued);
		this.setCustomText(customText);
		this.setGetAllFields(getAllFields);
		this.setAllFieldsExceptions(allFieldsExceptions);
		this.setGetFullRecordAsXML(getFullRecordAsXML);
		this.setEnrichSet(enrichSet);
		this.setEnrichAdd(enrichAdd);
		this.setTranslateValue(translateValue);
		this.setTranslateValueContains(translateValueContains);
		this.setTranslateValueRegex(translateValueRegex);
		this.setTranslateProperties(translateProperties);
		this.setHasDefaultValue(hasDefaultValue);
		this.setDefaultValue(defaultValue);
		this.setHasConnectedSubfields(hasConnectedSubfields);
		this.setConnectedSubfields(connectedSubfields);		
		this.setTranslateConnectedSubfields(translateConnectedSubfields);
		this.setTranslateConnectedSubfieldsProperties(translateConnectedSubfieldsProperties);				
		this.setHasConcatenatedSubfields(hasConcatenatedSubfields);
		this.setConcatenatedSubfields(concatenatedSubfields);
		this.setConcatenatedSubfieldsSeparator(concatenatedSubfieldsSeparator);
		this.setTranslateConcatenatedSubfields(translateConcatenatedSubfields);
		this.setTranslateConcatenatedSubfieldsProperties(translateConcatenatedSubfieldsProperties);
		this.setHasRegex(hasRegex);
		this.setRegexValue(regexValue);
		this.setHasRegexStrict(hasRegexStrict);
		this.setRegexStrictValue(regexStrictValue);
		this.setHasRegExReplace(hasRegExReplace);
		this.setRegexReplaceValues(regexReplaceValues);
		this.setAllowDuplicates(allowDuplicates);
		this.setHasSubfieldExists(hasSubfieldExists);
		this.setSubfieldExists(subfieldExists);
		this.setHasSubfieldValueExists(hasSubfieldValueExists);
		this.setSubfieldValueExists(subfieldValueExists);
		this.setHasSubfieldNotExists(hasSubfieldNotExists);	
		this.setSubfieldNotExists(subfieldNotExists);
		this.setApplyToFields(applyToFields);
	}


	public String getSolrFieldname() {
		return this.solrFieldname;
	}

	public void setSolrFieldname(String solrFieldname) {
		this.solrFieldname = solrFieldname;
	}

	public Map<String, List<String>> getPropertiesFields() {
		return this.propertiesFields;
	}

	public void setPropertiesFields(Map<String, List<String>> mabFieldnames) {
		this.propertiesFields = mabFieldnames;
	}

	public Leader getLeader() {
		return leader;
	}

	public void setLeader(Leader leader) {
		this.leader = leader;
	}

	public List<Datafield> getDatafields() {
		return datafields;
	}

	public void setDatafields(List<Datafield> datafields) {
		this.datafields = datafields;
	}

	public List<Controlfield> getControlfields() {
		return controlfields;
	}

	public void setControlfields(List<Controlfield> controlfields) {
		this.controlfields = controlfields;
	}

	public boolean isMultiValued() {
		return multiValued;
	}

	public void setMultiValued(boolean multiValued) {
		this.multiValued = multiValued;
	}

	public boolean isCustomText() {
		return customText;
	}

	public void setCustomText(boolean customText) {
		this.customText = customText;
	}

	public boolean isGetAllFields() {
		return getAllFields;
	}

	public void setGetAllFields(boolean getAllFields) {
		this.getAllFields = getAllFields;
	}

	public List<String> getAllFieldsExceptions() {
		return allFieldsExceptions;
	}

	public void setAllFieldsExceptions(List<String> allFieldsExceptions) {
		this.allFieldsExceptions = allFieldsExceptions;
	}

	public boolean isGetFullRecordAsXML() {
		return getFullRecordAsXML;
	}

	public void setGetFullRecordAsXML(boolean getFullRecordAsXML) {
		this.getFullRecordAsXML = getFullRecordAsXML;
	}
	
	public boolean isEnrichSet() {
		return enrichSet;
	}

	public void setEnrichSet(boolean enrichSet) {
		this.enrichSet = enrichSet;
	}

	public boolean isEnrichAdd() {
		return enrichAdd;
	}

	public void setEnrichAdd(boolean enrichAdd) {
		this.enrichAdd = enrichAdd;
	}

	public boolean isTranslateValue() {
		return translateValue;
	}

	public void setTranslateValue(boolean translateValue) {
		this.translateValue = translateValue;
	}

	public boolean isTranslateValueContains() {
		return translateValueContains;
	}

	public void setTranslateValueContains(boolean translateValueContains) {
		this.translateValueContains = translateValueContains;
	}

	public boolean isTranslateValueRegex() {
		return translateValueRegex;
	}

	public void setTranslateValueRegex(boolean translateValueRegex) {
		this.translateValueRegex = translateValueRegex;
	}

	public Map<String, String> getTranslateProperties() {
		return translateProperties;
	}

	public void setTranslateProperties(Map<String, String> translateProperties) {
		this.translateProperties = translateProperties;
	}

	public boolean hasDefaultValue() {
		return hasDefaultValue;
	}

	public void setHasDefaultValue(boolean hasDefaultValue) {
		this.hasDefaultValue = hasDefaultValue;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public LinkedHashMap<Integer, String> getConnectedSubfields() {
		return connectedSubfields;
	}

	public void setConnectedSubfields(LinkedHashMap<Integer, String> connectedSubfields) {
		this.connectedSubfields = connectedSubfields;
	}

	public boolean isTranslateConnectedSubfields() {
		return translateConnectedSubfields;
	}

	public void setTranslateConnectedSubfields(boolean translateConnectedSubfields) {
		this.translateConnectedSubfields = translateConnectedSubfields;
	}

	public Map<String, String> getTranslateConnectedSubfieldsProperties() {
		return translateConnectedSubfieldsProperties;
	}

	public void setTranslateConnectedSubfieldsProperties(Map<String, String> translateConnectedSubfieldsProperties) {
		this.translateConnectedSubfieldsProperties = translateConnectedSubfieldsProperties;
	}

	public boolean hasConnectedSubfields() {
		return hasConnectedSubfields;
	}

	public void setHasConnectedSubfields(boolean hasConnectedSubfields) {
		this.hasConnectedSubfields = hasConnectedSubfields;
	}

	public boolean hasConcatenatedSubfields() {
		return hasConcatenatedSubfields;
	}

	public void setHasConcatenatedSubfields(boolean hasConcatenatedSubfields) {
		this.hasConcatenatedSubfields = hasConcatenatedSubfields;
	}

	public LinkedHashMap<Integer, String> getConcatenatedSubfields() {
		return concatenatedSubfields;
	}

	public void setConcatenatedSubfields(LinkedHashMap<Integer, String> concatenatedSubfields) {
		this.concatenatedSubfields = concatenatedSubfields;
	}

	public String getConcatenatedSubfieldsSeparator() {
		return concatenatedSubfieldsSeparator;
	}

	public void setConcatenatedSubfieldsSeparator(String concatenatedSubfieldsSeparator) {
		this.concatenatedSubfieldsSeparator = concatenatedSubfieldsSeparator;
	}

	public boolean isTranslateConcatenatedSubfields() {
		return translateConcatenatedSubfields;
	}

	public void setTranslateConcatenatedSubfields(boolean translateConcatenatedSubfields) {
		this.translateConcatenatedSubfields = translateConcatenatedSubfields;
	}

	public Map<String, String> getTranslateConcatenatedSubfieldsProperties() {
		return translateConcatenatedSubfieldsProperties;
	}

	public void setTranslateConcatenatedSubfieldsProperties(Map<String, String> translateConcatenatedSubfieldsProperties) {
		this.translateConcatenatedSubfieldsProperties = translateConcatenatedSubfieldsProperties;
	}

	public boolean hasRegex() {
		return hasRegex;
	}

	public void setHasRegex(boolean hasRegex) {
		this.hasRegex = hasRegex;
	}

	public String getRegexValue() {
		return regexValue;
	}

	public void setRegexValue(String regexValue) {
		this.regexValue = regexValue;
	}

	public boolean hasRegexStrict() {
		return hasRegexStrict;
	}

	public void setHasRegexStrict(boolean hasRegexStrict) {
		this.hasRegexStrict = hasRegexStrict;
	}

	public String getRegexStrictValue() {
		return regexStrictValue;
	}

	public void setRegexStrictValue(String regexStrictValue) {
		this.regexStrictValue = regexStrictValue;
	}

	public boolean hasRegExReplace() {
		return hasRegExReplace;
	}

	public void setHasRegExReplace(boolean hasRegExReplace) {
		this.hasRegExReplace = hasRegExReplace;
	}

	public Map<Integer, String> getRegexReplaceValues() {
		return regexReplaceValues;
	}

	public void setRegexReplaceValues(Map<Integer, String> regexReplaceValues) {
		this.regexReplaceValues = regexReplaceValues;
	}

	public boolean isAllowDuplicates() {
		return allowDuplicates;
	}

	public void setAllowDuplicates(boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
	}

	public boolean hasSubfieldExists() {
		return hasSubfieldExists;
	}

	public void setHasSubfieldExists(boolean hasSubfieldExists) {
		this.hasSubfieldExists = hasSubfieldExists;
	}

	public LinkedHashMap<Integer, String> getSubfieldExists() {
		return subfieldExists;
	}

	public void setSubfieldExists(LinkedHashMap<Integer, String> subfieldExists) {
		this.subfieldExists = subfieldExists;
	}

	public boolean hasSubfieldValueExists() {
		return hasSubfieldValueExists;
	}

	public void setHasSubfieldValueExists(boolean hasSubfieldValueExists) {
		this.hasSubfieldValueExists = hasSubfieldValueExists;
	}

	public LinkedHashMap<Integer, String> getSubfieldValueExists() {
		return subfieldValueExists;
	}

	public void setSubfieldValueExists(LinkedHashMap<Integer, String> subfieldValueExists) {
		this.subfieldValueExists = subfieldValueExists;
	}

	public boolean hasSubfieldNotExists() {
		return hasSubfieldNotExists;
	}

	public void setHasSubfieldNotExists(boolean hasSubfieldNotExists) {
		this.hasSubfieldNotExists = hasSubfieldNotExists;
	}

	public LinkedHashMap<Integer, String> getSubfieldNotExists() {
		return subfieldNotExists;
	}

	public void setSubfieldNotExists(LinkedHashMap<Integer, String> subfieldNotExists) {
		this.subfieldNotExists = subfieldNotExists;
	}

	public LinkedHashMap<String, List<String>> getApplyToFields() {
		return applyToFields;
	}

	public void setApplyToFields(LinkedHashMap<String, List<String>> applyToFields) {
		this.applyToFields = applyToFields;
	}


	/**
	 * Check if a given subfield is used in the current PropertiesObject. In other words: This method checks if the subfield
	 * is targeted by this rule (= PropertiesObject). If not, we can skip it in the MatchingOperations which saves us some
	 * ressources and time.
	 * 
	 * @param rawDatafield	Datafield: The datafield with the subfield we need to check.
	 * @param rawSubfield	Subfield: The subfield we need to check.
	 * @return				boolean: True if the Subfield is used in the PropertiesObject (do not skip), false other wise (can be skipped) 
	 */
	public boolean containsSubfieldOfDatafield(Datafield rawDatafield, Subfield rawSubfield) {
		boolean returnValue = false;

		String rawTag = rawDatafield.getTag();
		String rawInd1 = rawDatafield.getInd1();
		String rawInd2 = rawDatafield.getInd2();
		String rawSubfieldcode = rawSubfield.getCode();

		for (Datafield propertiesDatafield : this.getDatafields()) {
			if (propertiesDatafield.getTag().equals(rawTag)) {
				if (propertiesDatafield.getInd1().equals("*") || propertiesDatafield.getInd1().equals(rawInd1)) {
					if (propertiesDatafield.getInd2().equals("*") || propertiesDatafield.getInd2().equals(rawInd2)) {
						for (Subfield propertiesSubfield : propertiesDatafield.getSubfields()) {
							if (propertiesSubfield.getCode().equals("*") || propertiesSubfield.getCode().equals(rawSubfieldcode)) {
								returnValue = true;
							}
						}
					}
				}
			}
		}

		return returnValue;
	}


	@Override
	public String toString() {
		return "PropertiesObject [solrFieldname=" + solrFieldname + ", propertiesFields=" + propertiesFields
				+ ", leader=" + leader + ", datafields=" + datafields + ", controlfields=" + controlfields + ", multiValued=" + multiValued
				+ ", customText=" + customText + ", getAllFields=" + getAllFields + ", allFieldsExceptions="
				+ allFieldsExceptions + ", getFullRecordAsXML=" + getFullRecordAsXML + ", enrichSet=" + enrichSet + ", enrichAdd=" + enrichAdd
				+ ", translateValue=" + translateValue + ", translateValueContains=" + translateValueContains + ", translateValueRegex="
				+ translateValueRegex + ", translateProperties=" + translateProperties + ", hasDefaultValue="
				+ hasDefaultValue + ", defaultValue=" + defaultValue + ", hasConnectedSubfields="
				+ hasConnectedSubfields + ", connectedSubfields=" + connectedSubfields
				+ ", translateConnectedSubfields=" + translateConnectedSubfields
				+ ", translateConnectedSubfieldsProperties=" + translateConnectedSubfieldsProperties
				+ ", hasConcatenatedSubfields=" + hasConcatenatedSubfields + ", concatenatedSubfields="
				+ concatenatedSubfields + ", concatenatedSubfieldsSeparator=" + concatenatedSubfieldsSeparator
				+ ", translateConcatenatedSubfields=" + translateConcatenatedSubfields
				+ ", translateConcatenatedSubfieldsProperties=" + translateConcatenatedSubfieldsProperties
				+ ", hasRegex=" + hasRegex + ", regexValue=" + regexValue + ", hasRegexStrict=" + hasRegexStrict
				+ ", regexStrictValue=" + regexStrictValue + ", hasRegExReplace=" + hasRegExReplace
				+ ", regexReplaceValues=" + regexReplaceValues + ", allowDuplicates=" + allowDuplicates
				+ ", hasSubfieldExists=" + hasSubfieldExists + ", subfieldExists=" + subfieldExists
				+ ", hasSubfieldValueExists=" + hasSubfieldValueExists + ", subfieldValueExists=" + subfieldValueExists
				+ ", hasSubfieldNotExists=" + hasSubfieldNotExists + ", subfieldNotExists=" + subfieldNotExists
				+ ", applyToFields=" + applyToFields + "]";
	}
}