package main.java.betullam.akimporter.solrmab.indexing;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConcatenatedField {

	private Set<String> concatenatedMasterFields;
	private List<String> concatenatedSubfields;
	private String concatenatedFieldsSeparator;
	private boolean translateConcatenatedSubfields;
	private Map<String, String> translateConcatenatedSubfieldsProperties;
	
	public ConcatenatedField(Set<String> concatenatedMasterFields, List<String> concatenatedSubfields, String concatenatedFieldsSeparator, boolean translateConcatenatedSubfields, Map<String, String> translateConcatenatedSubfieldsProperties) {
		this.concatenatedMasterFields = concatenatedMasterFields;
		this.concatenatedSubfields = concatenatedSubfields;
		this.concatenatedFieldsSeparator = concatenatedFieldsSeparator;
		this.translateConcatenatedSubfields = translateConcatenatedSubfields;
		this.translateConcatenatedSubfieldsProperties = translateConcatenatedSubfieldsProperties;
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

	@Override
	public String toString() {
		return "ConcatenatedField [concatenatedMasterFields=" + concatenatedMasterFields + ", concatenatedSubfields="
				+ concatenatedSubfields + ", concatenatedFieldsSeparator=" + concatenatedFieldsSeparator
				+ ", translateConcatenatedSubfields=" + translateConcatenatedSubfields
				+ ", translateConcatenatedSubfieldsProperties=" + translateConcatenatedSubfieldsProperties + "]";
	}
}