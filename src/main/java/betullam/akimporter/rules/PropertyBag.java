package main.java.betullam.akimporter.rules;

import java.util.List;

public class PropertyBag {

	private String solrField;
	private List<String> dataFields;
	private List<String> dataRules;
	
	public String getSolrField() {
		return solrField;
	}
	public void setSolrField(String solrField) {
		this.solrField = solrField;
	}
	public List<String> getDataFields() {
		return dataFields;
	}
	public void setDataFields(List<String> dataFields) {
		this.dataFields = dataFields;
	}
	public List<String> getDataRules() {
		return dataRules;
	}
	public void setDataRules(List<String> dataRules) {
		this.dataRules = dataRules;
	}
	
	@Override
	public String toString() {
		return "PropertyBag [\n\tsolrField=" + solrField + "\n\tdataFields=" + dataFields + "\n\tdataRules=" + dataRules + "\n]";
	}
	
	
}