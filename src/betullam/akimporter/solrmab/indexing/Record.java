/**
 * Record class.
 * Represents a MAB record including some additional information.
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
package betullam.akimporter.solrmab.indexing;

import java.util.List;

public class Record {

	private List<Mabfield> mabfields;
	private String recordID;
	private String recordSYS;
	private String indexTimestamp;
	

	public Record() {}
	

	public List<Mabfield> getMabfields() {
		return mabfields;
	}

	public void setMabfields(List<Mabfield> mabfields) {
		this.mabfields = mabfields;
	}

	public String getRecordID() {
		return recordID;
	}

	public void setRecordID(String recordID) {
		this.recordID = recordID;
	}

	public String getRecordSYS() {
		return recordSYS;
	}

	public void setRecordSYS(String recordSYS) {
		this.recordSYS = recordSYS;
	}

	public String getIndexTimestamp() {
		return indexTimestamp;
	}

	public void setIndexTimestamp(String indexTimestamp) {
		this.indexTimestamp = indexTimestamp;
	}

	@Override
	public String toString() {
		return "Record [mabfields=" + mabfields + ", recordID=" + recordID + ", recordSYS=" + recordSYS + ", indexTimestamp=" + indexTimestamp + "]";
	}

}