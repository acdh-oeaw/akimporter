/**
 * Connectedfield class.
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

import java.util.List;
import java.util.Map;

public class Connectedfield {

	private Map<String, String> connectedMasterFields; // First String = Master subfield code, Second String = Master datafield code 
	private List<String> connectedSubfields; // Subfields that should contain a value
	private String connectedDefaultValue; // Default value if none of the given subfields in connectedSubfields exists
	
	
	public Connectedfield(Map<String, String> connectedMasterFields, List<String> connectedSubfields, String connectedDefaultValue) {
		this.connectedMasterFields = connectedMasterFields;
		this.connectedSubfields = connectedSubfields;
		this.connectedDefaultValue = connectedDefaultValue;
	}

	
	public Map<String, String> getConnectedMasterFields() {
		return connectedMasterFields;
	}

	public void setConnectedMasterFields(Map<String, String> connectedMasterFields) {
		this.connectedMasterFields = connectedMasterFields;
	}

	public List<String> getConnectedSubfields() {
		return connectedSubfields;
	}

	public void setConnectedSubfields(List<String> connectedSubfields) {
		this.connectedSubfields = connectedSubfields;
	}

	public String getConnectedDefaultValue() {
		return connectedDefaultValue;
	}

	public void setConnectedDefaultValue(String connectedDefaultValue) {
		this.connectedDefaultValue = connectedDefaultValue;
	}

	@Override
	public String toString() {
		return "Connectedfield [connectedMasterFields=" + connectedMasterFields + ", connectedSubfields="
				+ connectedSubfields + ", connectedDefaultValue=" + connectedDefaultValue + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((connectedDefaultValue == null) ? 0 : connectedDefaultValue.hashCode());
		result = prime * result + ((connectedMasterFields == null) ? 0 : connectedMasterFields.hashCode());
		result = prime * result + ((connectedSubfields == null) ? 0 : connectedSubfields.hashCode());
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
		if (!(obj instanceof Connectedfield)) {
			return false;
		}
		Connectedfield other = (Connectedfield) obj;
		if (connectedDefaultValue == null) {
			if (other.connectedDefaultValue != null) {
				return false;
			}
		} else if (!connectedDefaultValue.equals(other.connectedDefaultValue)) {
			return false;
		}
		if (connectedMasterFields == null) {
			if (other.connectedMasterFields != null) {
				return false;
			}
		} else if (!connectedMasterFields.equals(other.connectedMasterFields)) {
			return false;
		}
		if (connectedSubfields == null) {
			if (other.connectedSubfields != null) {
				return false;
			}
		} else if (!connectedSubfields.equals(other.connectedSubfields)) {
			return false;
		}
		return true;
	}
}