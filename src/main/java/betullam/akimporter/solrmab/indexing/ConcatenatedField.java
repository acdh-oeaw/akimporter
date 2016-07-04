package main.java.betullam.akimporter.solrmab.indexing;

import java.util.List;
import java.util.Set;

public class ConcatenatedField {

	private Set<String> concatenatedMasterFields;
	private List<String> concatenatedSubfields;
	private String concatenatedFieldsSeparator;
	
	public ConcatenatedField(Set<String> concatenatedMasterFields, List<String> concatenatedSubfields, String concatenatedFieldsSeparator) {
		this.concatenatedMasterFields = concatenatedMasterFields;
		this.concatenatedSubfields = concatenatedSubfields;
		this.concatenatedFieldsSeparator = concatenatedFieldsSeparator;
	}

	
	public Set<String> getConcatenatedMasterFields() {
		return concatenatedMasterFields;
	}

	public void setConcatenatedMasterFields(Set<String> concatenatedMasterFields) {
		this.concatenatedMasterFields = concatenatedMasterFields;
	}

	public List<String> getConcatenatedSubfields() {
		return concatenatedSubfields;
	}

	public void setConcatenatedSubfields(List<String> concatenatedSubfields) {
		this.concatenatedSubfields = concatenatedSubfields;
	}

	public String getConcatenatedFieldsSeparator() {
		return concatenatedFieldsSeparator;
	}

	public void setConcatenatedFieldsSeparator(String concatenatedFieldsSeparator) {
		this.concatenatedFieldsSeparator = concatenatedFieldsSeparator;
	}


	@Override
	public String toString() {
		return "ConcatenatedField [masterFields=" + concatenatedMasterFields + ", concatenatedSubfields=" + concatenatedSubfields
				+ ", separator=" + concatenatedFieldsSeparator + "]";
	}
	

}
