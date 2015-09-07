package betullam.akimporter.solrmab.indexing;

import java.util.HashMap;
import java.util.List;

public class MatchingObject {

	
	private String solrFieldname; // Solr fieldname (e. g. "autor", "title", "id", etc.)
	//private List<String> mabFieldnames; // MAB-Fieldnames (e. g. "100$a$p", "all100") which corresponds to the solrFieldname (e. g. "autor")
	private HashMap<String, List<String>> mabFieldnames;
	private boolean multiValued;
	private boolean customText;
	private boolean translateValue;
	private HashMap<String, String> translateProperties;
	
	public MatchingObject() {}
	
	public MatchingObject(String solrFieldname, HashMap<String, List<String>> mabFieldnames, boolean multiValued, boolean customText, boolean translateValue, HashMap<String, String> translateProperties) {
		this.setSolrFieldname(solrFieldname);
		this.setMabFieldnames(mabFieldnames);
		this.setMultiValued(multiValued);
		this.setCustomText(customText);
		this.setTranslateValue(translateValue);
		this.setTranslateProperties(translateProperties);
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

	public HashMap<String, String> getTranslateProperties() {
		return translateProperties;
	}

	public void setTranslateProperties(HashMap<String, String> translateProperties) {
		this.translateProperties = translateProperties;
	}

	@Override
	public String toString() {
		return "MatchingObject [solrFieldname=" + solrFieldname
				+ ", mabFieldnames=" + mabFieldnames + ", multiValued="
				+ multiValued + ", customText=" + customText
				+ ", translateValue=" + translateValue
				+ ", translateProperties=" + translateProperties + "]";
	}
	

}

 