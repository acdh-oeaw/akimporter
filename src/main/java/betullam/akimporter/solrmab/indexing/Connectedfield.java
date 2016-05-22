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

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Connectedfield {

	private Set<String> connectedMasterFields; // Set of Strings like 100p, 655u, etc. 
	private List<String> connectedSubfields; // Subfields that should contain a value
	private String connectedDefaultValue; // Default value if none of the given subfields in connectedSubfields exists
	private boolean translateConnectedSubfields;
	private HashMap<String, String> translateSubfieldsProperties;
	
	public Connectedfield(Set<String> connectedMasterFields, List<String> connectedSubfields, String connectedDefaultValue, boolean translateConnectedSubfields, HashMap<String, String> translateSubfieldsProperties) {
		this.connectedMasterFields = connectedMasterFields;
		this.connectedSubfields = connectedSubfields;
		this.connectedDefaultValue = connectedDefaultValue;
		this.translateConnectedSubfields = translateConnectedSubfields;
		this.translateSubfieldsProperties = translateSubfieldsProperties;
	}

	public Set<String> getConnectedMasterFields() {
		return connectedMasterFields;
	}

	public void setConnectedMasterFields(Set<String> connectedMasterFields) {
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

	public boolean isTranslateConnectedSubfields() {
		return translateConnectedSubfields;
	}

	public void setTranslateConnectedSubfields(boolean translateConnectedSubfields) {
		this.translateConnectedSubfields = translateConnectedSubfields;
	}

	public HashMap<String, String> getTranslateSubfieldsProperties() {
		return translateSubfieldsProperties;
	}

	public void setTranslateSubfieldsProperties(HashMap<String, String> translateSubfieldsProperties) {
		this.translateSubfieldsProperties = translateSubfieldsProperties;
	}

	

	@Override
	public String toString() {
		return "Connectedfield [connectedMasterFields=" + connectedMasterFields + ", connectedSubfields="
				+ connectedSubfields + ", connectedDefaultValue=" + connectedDefaultValue
				+ ", translateConnectedSubfields=" + translateConnectedSubfields + ", translateSubfieldsProperties="
				+ translateSubfieldsProperties + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((connectedDefaultValue == null) ? 0 : connectedDefaultValue.hashCode());
		result = prime * result + ((connectedMasterFields == null) ? 0 : connectedMasterFields.hashCode());
		result = prime * result + ((connectedSubfields == null) ? 0 : connectedSubfields.hashCode());
		result = prime * result + (translateConnectedSubfields ? 1231 : 1237);
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
		if (translateConnectedSubfields != other.translateConnectedSubfields) {
			return false;
		}
		return true;
	}
}