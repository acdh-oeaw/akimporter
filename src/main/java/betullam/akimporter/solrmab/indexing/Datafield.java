/**
 * Datafield class
 *
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
 * 
 * TODO: Check if this class is still necessary!
 * 		 Maybe we could yous Mabfield class instead.
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

public class Datafield extends Controlfield {

	//private String tag;
	private String ind1;
	private String ind2;
	//private List<Mabfield> mabfields;
	private ArrayList<Subfield> subfields;

	public Datafield() {}


	/*
	public String getTag() {
		return this.tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}
	*/

	public String getInd1() {
		return this.ind1;
	}

	public void setInd1(String ind1) {
		this.ind1 = ind1;
	}

	public String getInd2() {
		return this.ind2;
	}

	public void setInd2(String ind2) {
		this.ind2 = ind2;
	}

	public ArrayList<Subfield> getSubfields() {
		return subfields;
	}

	public void setSubfields(ArrayList<Subfield> subfields) {
		this.subfields = subfields;
	}

	/*
	public List<Mabfield> getSubfields() {
		return this.mabfields;
	}

	public void setSubfields(List<Mabfield> mabfields) {
		this.mabfields = mabfields;
	}
	 */

	@Override
	public String toString() {
		return "Datafield [tag=" + tag + ", ind1=" + ind1 + ", ind2=" + ind2 + ", subfields=" + subfields + "]";
	}


	// RAW:
	// Datafield [tag=PER, ind1=*, ind2=*, subfields=[Subfield [code=p, content=Roques, Jean Léon], Subfield [code=d, content=1839-1923], Subfield [code=9, content=(DE-588)134683714]]]
	// -----------------------
	// Datafield [tag=902, ind1=-, ind2=1, subfields=[Subfield [code=z, content=Geschichte 1865], Subfield [code=x, content=xxxxx]]]

	// MAB.PROPERTIES:
	// Datafield [tag=PER, ind1=*, ind2=*, subfields=[Subfield [code=p, content=Roques, Jean Léon]]]
	// Datafield [tag=PER, ind1=*, ind2=*, subfields=[Subfield [code=d, content=1839-1923]]]
	// Datafield [tag=PER, ind1=*, ind2=*, subfields=[Subfield [code=9, content=(DE-588)134683714]]]
	// -----------------------
	// Datafield [tag=902, ind1=-, ind2=1, subfields=[Subfield [code=z, content=Geschichte 1865], Subfield [code=x, content=xxxxx]]]
	/**
	 * Compare method for matching raw datafields from MarcXML with datafields given in mab.properties
	 * @param		rawDatafield: The raw datafield that was parsed from the MarcXML file
	 * @return		true if the raw datafield matches with one or multiple fields in mab.properties, false otherwise
	 */
	/*
	public boolean match(Datafield rawDatafield) {

		// Return true if it is the same instance
		if (this == rawDatafield) {
			return true;
		}

		// Return false if Datafield object is null
		if (rawDatafield == null) {
			return false;
		}

		boolean returnValue = false;

		if (rawDatafield.getTag() == tag) {
			if (ind1 == "*" || rawDatafield.getInd1() == ind1) {
				if (ind2 == "*" || rawDatafield.getInd2() == ind2) {
					// Check subfields here
				}
			}
		}

		return returnValue;
	}
	 */

	/**
	 * Compare method for matching datafields given in mab.properties with raw datafields from MarcXML 
	 * @param		propertiesDatafield: The datafield that was parsed from the mab.properties file
	 * @return		true if a field in mab.properties matches with raw datafields, false otherwise
	 */
	public boolean match(Datafield propertiesDatafield) {
		
		// Return true if it is the same instance
		if (this == propertiesDatafield) {
			return true;
		}
		
		// Return false if Datafield object is null
		if (propertiesDatafield == null) {
			return false;
		}
		
		boolean returnValue = false;

		
		//System.out.println("Raw Tag : " + tag);
		//System.out.println("Prop Tag: " + propertiesDatafield.getTag());
		
		if (propertiesDatafield.getTag().equals(tag)) {
			
			if (propertiesDatafield.getInd1().equals("*") || propertiesDatafield.getInd1().equals(ind1)) {
				if (propertiesDatafield.getInd2().equals("*") || propertiesDatafield.getInd2().equals(ind2)) {
					for (Subfield rawSubfield : subfields) {
						for (Subfield propertiesSubfield : propertiesDatafield.getSubfields()) {
							if (propertiesSubfield.getCode().equals("*") || propertiesSubfield.getCode().equals(rawSubfield.getCode())) {
								returnValue = true;
							}
						}
					}
				}
			}
		}

		return returnValue;
	}
}