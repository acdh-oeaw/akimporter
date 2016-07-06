/**
 * Mabfield class
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

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Mabfield implements Comparable<Mabfield> {

	private String fieldname;
	private String fieldvalue;
	private List<String> connectedValues;
	private boolean isTranslateConnectedSubfields = false;
	private Map<String, String> translateSubfieldsProperties;
	private boolean allowDuplicates = false;
	private List<String> concatenatedValues;
	private String concatenatedSeparator;
	private boolean isTranslateConcatenatedSubfields = false;
	private Map<String, String> translateConcatenatedSubfieldsProperties;

	public Mabfield() {}

	public Mabfield(String fieldname, String fieldvalue) {
		this.setFieldname(fieldname);
		this.setFieldvalue(fieldvalue);
	}


	public String getFieldname() {
		return this.fieldname;
	}

	public void setFieldname(String fieldname) {
		this.fieldname = fieldname;
	}

	public String getFieldvalue() {
		return this.fieldvalue;
	}

	public void setFieldvalue(String fieldvalue) {
		this.fieldvalue = fieldvalue;
	}
	
	public List<String> getConnectedValues() {
		return connectedValues;
	}

	public void setConnectedValues(List<String> connectedValues) {
		this.connectedValues = connectedValues;
	}

	public boolean isTranslateConnectedSubfields() {
		return isTranslateConnectedSubfields;
	}

	public void setTranslateConnectedSubfields(boolean isTranslateConnectedSubfields) {
		this.isTranslateConnectedSubfields = isTranslateConnectedSubfields;
	}

	public Map<String, String> getTranslateSubfieldsProperties() {
		return translateSubfieldsProperties;
	}

	public void setTranslateSubfieldsProperties(Map<String, String> translateSubfieldsProperties) {
		this.translateSubfieldsProperties = translateSubfieldsProperties;
	}

	public boolean isAllowDuplicates() {
		return allowDuplicates;
	}

	public void setAllowDuplicates(boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
	}

	public List<String> getConcatenatedValues() {
		return concatenatedValues;
	}

	public void setConcatenatedValues(List<String> concatenatedValues) {
		this.concatenatedValues = concatenatedValues;
	}
	
	public String getConcatenatedSeparator() {
		return concatenatedSeparator;
	}

	public void setConcatenatedSeparator(String concatenatedSeparator) {
		this.concatenatedSeparator = concatenatedSeparator;
	}

	public boolean isTranslateConcatenatedSubfields() {
		return isTranslateConcatenatedSubfields;
	}

	public void setTranslateConcatenatedSubfields(boolean isTranslateConcatenatedSubfields) {
		this.isTranslateConcatenatedSubfields = isTranslateConcatenatedSubfields;
	}

	public Map<String, String> getTranslateConcatenatedSubfieldsProperties() {
		return translateConcatenatedSubfieldsProperties;
	}

	public void setTranslateConcatenatedSubfieldsProperties(Map<String, String> translateConcatenatedSubfieldsProperties) {
		this.translateConcatenatedSubfieldsProperties = translateConcatenatedSubfieldsProperties;
	}


	@Override
	public String toString() {
		return "Mabfield [fieldname=" + fieldname + ", fieldvalue=" + fieldvalue + ", connectedValues="
				+ connectedValues + ", isTranslateConnectedSubfields=" + isTranslateConnectedSubfields
				+ ", translateSubfieldsProperties=" + translateSubfieldsProperties + ", allowDuplicates="
				+ allowDuplicates + ", concatenatedValues=" + concatenatedValues + ", concatenatedSeparator="
				+ concatenatedSeparator + ", isTranslateConcatenatedSubfields=" + isTranslateConcatenatedSubfields
				+ ", translateConcatenatedSubfieldsProperties=" + translateConcatenatedSubfieldsProperties + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fieldname == null) ? 0 : fieldname.hashCode());
		result = prime * result + ((fieldvalue == null) ? 0 : fieldvalue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Mabfield)) {
			return false;
		}
		Mabfield other = (Mabfield) obj;
		if (fieldname == null) {
			if (other.fieldname != null) {
				return false;
			}
		} else if (!fieldname.equals(other.fieldname)) {
			return false;
		}
		if (fieldvalue == null) {
			if (other.fieldvalue != null) {
				return false;
			}
		} else if (!fieldvalue.equals(other.fieldvalue)) {
			return false;
		}
		return true;
	}

	@Override
	public int compareTo(Mabfield mabField) {
		if (!(mabField instanceof Mabfield)) {
			throw new ClassCastException("Mabfield object expected in compareTo() method of Mabfield class.");
		}
		return fieldname.compareTo(mabField.getFieldname());
	}


	public class FieldNameComparator implements Comparator<Mabfield> {
		public int compare(Mabfield mabfield1, Mabfield mabfield2) {
			String fieldName1 = mabfield1.getFieldname().toLowerCase();
			String fieldName2 = mabfield2.getFieldname().toLowerCase();
			return fieldName1.compareTo(fieldName2);
		}
	}






}
