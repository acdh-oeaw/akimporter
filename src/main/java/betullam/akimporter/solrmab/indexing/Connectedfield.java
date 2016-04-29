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

import java.util.ArrayList;
import java.util.List;

public class Connectedfield {

	private String datafieldName;
	private String masterSubfield;
	private List<String> connectedSubfields = new ArrayList<String>();
	private String defaultValueIfMissing;
	
	
	public Connectedfield(String datafieldName, String masterSubfield, List<String> connectedSubfields, String defaultValueIfMissing) {
		this.datafieldName = datafieldName;
		this.masterSubfield = masterSubfield;
		this.connectedSubfields = connectedSubfields;
		this.defaultValueIfMissing = defaultValueIfMissing;
	}

	
	public String getDatafieldName() {
		return datafieldName;
	}

	public void setDatafieldName(String datafieldName) {
		this.datafieldName = datafieldName;
	}

	public String getMasterSubfield() {
		return masterSubfield;
	}

	public void setMasterSubfield(String masterSubfield) {
		this.masterSubfield = masterSubfield;
	}

	public List<String> getConnectedSubfields() {
		return connectedSubfields;
	}

	public void setConnectedSubfields(List<String> dependentSubfields) {
		this.connectedSubfields = dependentSubfields;
	}

	public String getDefaultValueIfMissing() {
		return defaultValueIfMissing;
	}

	public void setDefaultValueIfMissing(String defaultValueIfMissing) {
		this.defaultValueIfMissing = defaultValueIfMissing;
	}

	@Override
	public String toString() {
		return "Connectedfield [datafieldName=" + datafieldName + ", masterSubfield=" + masterSubfield
				+ ", connectedSubfields=" + connectedSubfields + ", defaultValueIfMissing=" + defaultValueIfMissing
				+ "]";
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((connectedSubfields == null) ? 0 : connectedSubfields.hashCode());
		result = prime * result + ((datafieldName == null) ? 0 : datafieldName.hashCode());
		result = prime * result + ((defaultValueIfMissing == null) ? 0 : defaultValueIfMissing.hashCode());
		result = prime * result + ((masterSubfield == null) ? 0 : masterSubfield.hashCode());
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
		if (connectedSubfields == null) {
			if (other.connectedSubfields != null) {
				return false;
			}
		} else if (!connectedSubfields.equals(other.connectedSubfields)) {
			return false;
		}
		if (datafieldName == null) {
			if (other.datafieldName != null) {
				return false;
			}
		} else if (!datafieldName.equals(other.datafieldName)) {
			return false;
		}
		if (defaultValueIfMissing == null) {
			if (other.defaultValueIfMissing != null) {
				return false;
			}
		} else if (!defaultValueIfMissing.equals(other.defaultValueIfMissing)) {
			return false;
		}
		if (masterSubfield == null) {
			if (other.masterSubfield != null) {
				return false;
			}
		} else if (!masterSubfield.equals(other.masterSubfield)) {
			return false;
		}
		return true;
	}


	
	
	
}