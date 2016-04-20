/**
 * Mabfield class
 *
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
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

import java.util.Comparator;

public class Mabfield implements Comparable<Mabfield> {

	private String fieldname;
	private String fieldvalue;

	public Mabfield() {}

	public Mabfield(String fieldname, String fieldvalue) {
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

	@Override
	public String toString() {
		return "Mabfield [fieldname=" + fieldname + ", fieldvalue=" + fieldvalue + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fieldname == null) ? 0 : fieldname.hashCode());
		result = prime * result + ((fieldvalue == null) ? 0 : fieldvalue.hashCode());
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
		if (!(obj instanceof Mabfield)) {
			return false;
		}
		Mabfield other = (Mabfield) obj;
		if (fieldname == null) {
			if (other.fieldname != null) {
				return false;
			}
		} else if (!fieldname.equals(other.fieldname)) {
			return false;
		}
		if (fieldvalue == null) {
			if (other.fieldvalue != null) {
				return false;
			}
		} else if (!fieldvalue.equals(other.fieldvalue)) {
			return false;
		}
		return true;
	}

	@Override
	public int compareTo(Mabfield mabField) {
		if (!(mabField instanceof Mabfield)) {
			throw new ClassCastException("Mabfield object expected in compareTo() method of Mabfield class.");
		}
		return fieldname.compareTo(mabField.getFieldname());
	}


	public class FieldNameComparator implements Comparator<Mabfield> {
		public int compare(Mabfield mabfield1, Mabfield mabfield2) {
			String fieldName1 = mabfield1.getFieldname().toLowerCase();
			String fieldName2 = mabfield2.getFieldname().toLowerCase();
			return fieldName1.compareTo(fieldName2);
		}
	}






}
