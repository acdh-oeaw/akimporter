/**
 * MatchingObject class.
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

import java.util.HashMap;
import java.util.List;

public class MatchingObject {

	
	private String solrFieldname; // Solr fieldname (e. g. "autor", "title", "id", etc.)
	private HashMap<String, List<String>> mabFieldnames;
	private boolean multiValued;
	private boolean customText;
	private boolean translateValue;
	private boolean translateValueContains;
	private HashMap<String, String> translateProperties;
	private boolean hasDefaultValue;
	private String defaultValue;
	private String connectedSubfield;
	private boolean hasRegex;
	private String regexValue;
	private boolean hasRegexStrict;
	private String regexStrictValue;
	private boolean allowDuplicates;
	
	
	public MatchingObject() {}
	
	public MatchingObject(String solrFieldname, HashMap<String, List<String>> mabFieldnames, boolean multiValued, boolean customText, boolean translateValue, boolean translateValueContains, HashMap<String, String> translateProperties, boolean hasDefaultValue, String defaultValue, String connectedSubfield, boolean hasRegex, String regexValue, boolean hasRegexStrict, String regexStrictValue, boolean allowDuplicates) {
		this.setSolrFieldname(solrFieldname);
		this.setMabFieldnames(mabFieldnames);
		this.setMultiValued(multiValued);
		this.setCustomText(customText);
		this.setTranslateValue(translateValue);
		this.setTranslateValueContains(translateValueContains);
		this.setTranslateProperties(translateProperties);
		this.setHasDefaultValue(hasDefaultValue);
		this.setDefaultValue(defaultValue);
		this.setConnectedSubfield(connectedSubfield);
		this.setHasRegex(hasRegex);
		this.setRegexValue(regexValue);
		this.setHasRegexStrict(hasRegexStrict);
		this.setRegexStrictValue(regexStrictValue);
		this.setAllowDuplicates(allowDuplicates);
	}

	public String getSolrFieldname() {
		return this.solrFieldname;
	}

	public void setSolrFieldname(String solrFieldname) {
		this.solrFieldname = solrFieldname;
	}

	public HashMap<String, List<String>> getMabFieldnames() {
		return this.mabFieldnames;
	}

	public void setMabFieldnames(HashMap<String, List<String>> mabFieldnames) {
		this.mabFieldnames = mabFieldnames;
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

	public HashMap<String, String> getTranslateProperties() {
		return translateProperties;
	}

	public void setTranslateProperties(HashMap<String, String> translateProperties) {
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
	
	public String getConnectedSubfield() {
		return connectedSubfield;
	}

	public void setConnectedSubfield(String connectedSubfield) {
		this.connectedSubfield = connectedSubfield;
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

	public boolean isAllowDuplicates() {
		return allowDuplicates;
	}

	public void setAllowDuplicates(boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
	}

	
	@Override
	public String toString() {
		return "MatchingObject [solrFieldname=" + solrFieldname + ", mabFieldnames=" + mabFieldnames + ", multiValued="
				+ multiValued + ", customText=" + customText + ", translateValue=" + translateValue
				+ ", translateValueContains=" + translateValueContains + ", translateProperties=" + translateProperties
				+ ", hasDefaultValue=" + hasDefaultValue + ", defaultValue=" + defaultValue + ", connectedSubfields="
				+ connectedSubfield + ", hasRegex=" + hasRegex + ", regexValue=" + regexValue + ", hasRegexStrict="
				+ hasRegexStrict + ", regexStrictValue=" + regexStrictValue + ", allowDuplicates=" + allowDuplicates
				+ "]";
	}
	
}