/**
 * Datafield class. Represents a datafield of a MarcXML record.
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

public class Datafield {

	private String tag;
	private String ind1;
	private String ind2;
	private ArrayList<Subfield> subfields = new ArrayList<Subfield>();
	private ArrayList<Subfield> passiveSubfields = new ArrayList<Subfield>();

	
	public Datafield() {}

	public String getTag() {
		return this.tag;
	}

	
	public void setTag(String tag) {
		this.tag = tag;
	}

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

	public ArrayList<Subfield> getPassiveSubfields() {
		return passiveSubfields;
	}

	public void setPassiveSubfields(ArrayList<Subfield> passiveSubfields) {
		this.passiveSubfields = passiveSubfields;
	}
	
	public void addPassiveSubfields(ArrayList<Subfield> passiveSubfields) {
		this.passiveSubfields.addAll(passiveSubfields);
	}


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

	/**
	 * Check if a raw datafield is contained in a properties object (represents a line in mab.properties)
	 * @param 	propertiesObject	PropertiesObject: A properties object representing a line in mab.properties
	 * @return						boolean: true if the properties object contains the datafield this method is applied on, false otherwise
	 */
	public boolean isContainedInPropertiesObject(PropertiesObject propertiesObject) {

		// Return false if propertiesObject is null
		if (propertiesObject == null) {
			return false;
		}
		
		// Returns false if propertiesObject is no instance of PropertiesObject
		if (!(propertiesObject instanceof PropertiesObject)) {
			return false;
		}

		boolean returnValue = false;

		for (Datafield propertiesDatafield : propertiesObject.getDatafields()) {
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
		}

		return returnValue;
	}
	
	
	/**
	 * Move subfields to a new ArrayList<Subfield> called "passiveSubfields" and remove them from the default ArrayList<Subfield> called "subfields".
	 * @param subfields		ArrayList<Subfield>: An ArrayList of Subfield objects.
	 */
	public void moveToPassiveSubfields(ArrayList<Subfield> subfields) {
		this.addPassiveSubfields(subfields);
		this.subfields.removeAll(subfields);
	}
	

	/**
	 * Get subfields of a Datafield by subfield codes.
	 * 
	 * @param subfieldCodes		List<String>: The subfield codes of the subfields that should be returned.
	 * @return					ArrayList<Subfield>: A list of subfields with the given subfield codes.
	 */
	public ArrayList<Subfield> getSubfieldsByCodes(List<String> subfieldCodes) {
		ArrayList<Subfield> returnValue = new ArrayList<Subfield>();
		for (Subfield subfield : subfields) {
			if (subfieldCodes.contains(subfield.getCode())) {
				returnValue.add(subfield);
			}
		}
		return returnValue;
	}

	
	/**
	 * Copy a datafield object for changing values without impacting the original datafield.
	 * @param originalDatafield		Datafield: A Datafield object which should be copied
	 * @return				Datafield: A copy of a Datafield object. We can make changes to this copy without affecting the original Datafield object. 
	 */
	public static Datafield copy(Datafield originalDatafield) {
		Datafield newDatafield = new Datafield();
		newDatafield.tag = originalDatafield.tag;
		newDatafield.ind1 = originalDatafield.ind1;
		newDatafield.ind2 = originalDatafield.ind2;
		
		ArrayList<Subfield> newSubfields = new ArrayList<Subfield>();
		for (Subfield originalSubfield : originalDatafield.getSubfields()) {
			Subfield newSubfield = new Subfield();
			newSubfield.setCode(originalSubfield.getCode());
			newSubfield.setContent(originalSubfield.getContent());
			newSubfields.add(newSubfield);
		}
		newDatafield.subfields = newSubfields;
		
		ArrayList<Subfield> newPassiveSubfields = new ArrayList<Subfield>();
		for (Subfield originalPssiveSubfield : originalDatafield.getPassiveSubfields()) {
			Subfield newPassiveSubfield = new Subfield();
			newPassiveSubfield.setCode(originalPssiveSubfield.getCode());
			newPassiveSubfield.setContent(originalPssiveSubfield.getContent());
			newPassiveSubfields.add(newPassiveSubfield);
		}
		newDatafield.passiveSubfields = newPassiveSubfields;
		
		return newDatafield;
	}
	
	
	/**
	 * Bring the subfields of a datafield in an order according to an ArrayList<String>
	 * that contains the subfield codes in the right order.
	 * @param datafield				Datafield: The datafield of which the subfields should be ordered
	 * @param sortOrderStrings		ArrayList<String>: A list of subfield codes in the right order
	 * @return						Datafield: The datafield with subfields in the right order
	 */
	public static Datafield sort(Datafield datafield, ArrayList<String> sortOrderStrings) {
		ArrayList<Subfield> sortedSubfields = new ArrayList<Subfield>();
		
		// Put the subfields in the right order into a new ArrayList<Subfield> 
		for(String sortOrderString : sortOrderStrings) {
			for(Subfield sf : datafield.getSubfields()) {
				if (sf.getCode().equals(sortOrderString)) {
					sortedSubfields.add(sf);
				}
			}
		}
		
		// Remove the ordered subfields from the datafield because they could be in the wrong order
		datafield.getSubfields().removeAll(sortedSubfields);
		
		// Add subfields again at index position 0 and in the right order
		datafield.getSubfields().addAll(0, sortedSubfields);
		
		return datafield;
	}

	
	@Override
	public String toString() {
		return "Datafield [tag=" + tag + ", ind1=" + ind1 + ", ind2=" + ind2 + ", subfields=" + subfields
				+ ", passiveSubfields=" + passiveSubfields + "]";
	}
}