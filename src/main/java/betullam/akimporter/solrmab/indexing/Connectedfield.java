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
				+ ", dependentSubfields=" + connectedSubfields + ", defaultValueIfMissing=" + defaultValueIfMissing
				+ "]";
	}
	
	
	

}
