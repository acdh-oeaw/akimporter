/**
 * Controlfield class. Represents a controlfield of a MarcXML record.
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

public class Controlfield {
	
	protected String tag;
	private String content;
	
	
	public Controlfield() {};
	
	public Controlfield(String tag, String content) {
		this.tag = tag;
		this.content = content;
	}
	
	
	public String getTag() {
		return tag;
	}
	
	public void setTag(String tag) {
		this.tag = tag;
	}
	
	public String getContent() {
		return content;
	}
	
	public void setContent(String content) {
		this.content = content;
	}

	
	/**
	 * Check if a raw controlfield is contained in a properties object (represents a line in mab.properties)
	 * @param 	propertiesObject	PropertiesObject: A properties object representing a line in mab.properties
	 * @return						boolean: true if the properties object contains the controlfield this method is applied on, false otherwise
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

		for (Controlfield propertiesControlfield : propertiesObject.getControlfields()) {
			if (this.equals(propertiesControlfield)) {
				returnValue = true;
			}
		}

		return returnValue;
	}
	
	@Override
	public String toString() {
		return "Controlfield [tag=" + tag + ", content=" + content + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
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
		if (!(obj instanceof Controlfield)) {
			return false;
		}
		Controlfield other = (Controlfield) obj;
		if (tag == null) {
			if (other.tag != null) {
				return false;
			}
		} else if (!tag.equals(other.tag)) {
			return false;
		}
		return true;
	}
}