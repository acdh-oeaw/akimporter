package main.java.betullam.akimporter.solrmab.indexing;

import java.util.List;
import java.util.Set;

public class ExistsField {

	private Set<String> existsMasterFields;
	private List<String> existsSubfields;
	private String existsOperator;
	private boolean isSubfieldExists;
	private boolean isSubfieldNotExists;
	private List<String> solrFieldnames;
	
	
	public ExistsField(Set<String> existsMasterFields, List<String> existsSubfields, String existsOperator, boolean isSubfieldExists, boolean isSubfieldNotExists, List<String> solrFieldnames) {
		this.existsMasterFields = existsMasterFields;
		this.existsSubfields = existsSubfields;
		this.existsOperator = existsOperator;
		this.isSubfieldExists = isSubfieldExists;
		this.isSubfieldNotExists = isSubfieldNotExists;
		this.setSolrFieldnames(solrFieldnames);
	}

	public Set<String> getExistsMasterFields() {
		return existsMasterFields;
	}
	
	public void setExistsMasterFields(Set<String> existsMasterFields) {
		this.existsMasterFields = existsMasterFields;
	}
	
	public List<String> getExistsSubfields() {
		return existsSubfields;
	}
	
	public void setExistsSubfields(List<String> existsSubfields) {
		this.existsSubfields = existsSubfields;
	}
	
	public String getExistsOperator() {
		return existsOperator;
	}
	
	public void setExistsOperator(String existsOperator) {
		this.existsOperator = existsOperator;
	}
	
	public boolean isSubfieldExists() {
		return isSubfieldExists;
	}

	public void setIsSubfieldExists(boolean isSubfieldExists) {
		this.isSubfieldExists = isSubfieldExists;
	}

	public boolean isSubfieldNotExists() {
		return isSubfieldNotExists;
	}

	public void setIsSubfieldNotExists(boolean isSubfieldNotExists) {
		this.isSubfieldNotExists = isSubfieldNotExists;
	}

	public List<String> getSolrFieldnames() {
		return solrFieldnames;
	}

	public void setSolrFieldnames(List<String> solrFieldnames) {
		this.solrFieldnames = solrFieldnames;
	}

	
	@Override
	public String toString() {
		return "ExistsField [existsMasterFields=" + existsMasterFields + ", existsSubfields=" + existsSubfields
				+ ", existsOperator=" + existsOperator + ", isSubfieldExists=" + isSubfieldExists
				+ ", isSubfieldNotExists=" + isSubfieldNotExists + ", solrFieldnames=" + solrFieldnames + "]";
	}
	
}
