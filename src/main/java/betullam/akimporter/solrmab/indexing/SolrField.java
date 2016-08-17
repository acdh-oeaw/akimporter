/**
 * SolrField class
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

public class SolrField {

	private String fieldname;
	private ArrayList<String> fieldvalues;
	private boolean isMultivalued;
	private boolean allowDuplicates;

	
	public SolrField() {}

	/**
	 * SolrField constructor
	 * 
	 * @param fieldname			String
	 * @param fieldvalues		ArrayList<String>
	 * @param isMultivalued		boolean
	 * @param allowDuplicates	boolean
	 */
	public SolrField(String fieldname, ArrayList<String> fieldvalues, boolean isMultivalued, boolean allowDuplicates) {
		this.setFieldname(fieldname);
		this.setFieldvalues(fieldvalues);
		this.setMultivalued(isMultivalued);
		this.setAllowDuplicates(allowDuplicates);
	}

	public String getFieldname() {
		return this.fieldname;
	}

	public void setFieldname(String fieldname) {
		this.fieldname = fieldname;
	}

	public ArrayList<String> getFieldvalues() {
		return fieldvalues;
	}

	public void setFieldvalues(ArrayList<String> fieldvalues) {
		this.fieldvalues = fieldvalues;
	}

	public boolean isMultivalued() {
		return isMultivalued;
	}

	public void setMultivalued(boolean isMultivalued) {
		this.isMultivalued = isMultivalued;
	}

	public boolean allowDuplicates() {
		return allowDuplicates;
	}

	public void setAllowDuplicates(boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
	}

	@Override
	public String toString() {
		return "SolrField [fieldname=" + fieldname + ", fieldvalues=" + fieldvalues + ", isMultivalued=" + isMultivalued
				+ ", allowDuplicates=" + allowDuplicates + "]";
	}
}