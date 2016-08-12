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

import java.util.List;

public class SolrField {

	private String fieldname;
	private String fieldvalue;
	private List<String> connectedValues;
	private List<String> concatenatedValues;
	private String concatenatedSeparator;

	
	public SolrField() {}

	public SolrField(String fieldname, String fieldvalue) {
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

	
	@Override
	public String toString() {
		return "SolrField [fieldname=" + fieldname + ", fieldvalue=" + fieldvalue + ", connectedValues="
				+ connectedValues + ", concatenatedValues=" + concatenatedValues + ", concatenatedSeparator="
				+ concatenatedSeparator + "]";
	}
}