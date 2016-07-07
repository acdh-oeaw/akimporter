package main.java.betullam.akimporter.solrmab.indexing;

import java.util.List;
import java.util.Set;

public class ExistsField {

	private Set<String> existsMasterFields;
	private List<String> existsSubfields;
	private String existsOperator;
	
	
	
	public ExistsField(Set<String> existsMasterFields, List<String> existsSubfields, String existsOperator) {
		this.existsMasterFields = existsMasterFields;
		this.existsSubfields = existsSubfields;
		this.existsOperator = existsOperator;
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

	@Override
	public String toString() {
		return "ExistsField [existsMasterFields=" + existsMasterFields + ", existsSubfields=" + existsSubfields
				+ ", existsOperator=" + existsOperator + "]";
	}
	
}
