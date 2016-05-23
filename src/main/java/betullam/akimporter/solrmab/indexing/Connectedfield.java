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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Connectedfield {

	private Set<String> connectedMasterFields; // Set of Strings like 100p, 655u, etc. 
	LinkedHashMap<Integer, Map<String, List<String>>> connectedSubfields; // Subfields that should contain a value. Integer is the order, Map<String, List<String>> is Map<DefaultValue, List<SubfieldCode1, SubfieldCode2, ...>>

	private boolean translateConnectedSubfields;
	private Map<String, String> translateSubfieldsProperties;
	
	public Connectedfield(Set<String> connectedMasterFields, LinkedHashMap<Integer, Map<String, List<String>>> connectedSubfields, boolean translateConnectedSubfields, Map<String, String> translateSubfieldsProperties) {
		this.connectedMasterFields = connectedMasterFields;
		this.connectedSubfields = connectedSubfields;
		this.translateConnectedSubfields = translateConnectedSubfields;
		this.translateSubfieldsProperties = translateSubfieldsProperties;
	}

	public Set<String> getConnectedMasterFields() {
		return connectedMasterFields;
	}

	public void setConnectedMasterFields(Set<String> connectedMasterFields) {
		this.connectedMasterFields = connectedMasterFields;
	}

	public LinkedHashMap<Integer, Map<String, List<String>>> getConnectedSubfields() {
		return connectedSubfields;
	}

	public void setConnectedSubfields(LinkedHashMap<Integer, Map<String, List<String>>> connectedSubfields) {
		this.connectedSubfields = connectedSubfields;
	}

	public boolean isTranslateConnectedSubfields() {
		return translateConnectedSubfields;
	}

	public void setTranslateConnectedSubfields(boolean translateConnectedSubfields) {
		this.translateConnectedSubfields = translateConnectedSubfields;
	}

	public Map<String, String> getTranslateSubfieldsProperties() {
		return translateSubfieldsProperties;
	}

	public void setTranslateSubfieldsProperties(Map<String, String> translateSubfieldsProperties) {
		this.translateSubfieldsProperties = translateSubfieldsProperties;
	}

	@Override
	public String toString() {
		return "Connectedfield [connectedMasterFields=" + connectedMasterFields + ", connectedSubfields="
				+ connectedSubfields + ", translateConnectedSubfields=" + translateConnectedSubfields
				+ ", translateSubfieldsProperties=" + translateSubfieldsProperties + "]";
	}

	

	
}