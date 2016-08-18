/**
 * SolrRecord class.
 * Represents a Solr record including some additional information.
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

public class SolrRecord {

	private String recordID;
	private String recordSYS;
	private String indexTimestamp;
	private ArrayList<SolrField> solrFields;
	private String fullRecord;
	
	public SolrRecord(String recordID, String recordSYS, String indexTimestamp, ArrayList<SolrField> solrFields, String fullRecord) {
		this.recordID = recordID;
		this.recordSYS = recordSYS;
		this.indexTimestamp = indexTimestamp;
		this.solrFields = solrFields;
		this.fullRecord = fullRecord;
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

	public ArrayList<SolrField> getSolrFields() {
		return solrFields;
	}

	public void setSolrFields(ArrayList<SolrField> solrFields) {
		this.solrFields = solrFields;
	}
	
	public String getFullRecord() {
		return fullRecord;
	}

	public void setFullRecord(String fullRecord) {
		this.fullRecord = fullRecord;
	}

	@Override
	public String toString() {
		return "SolrRecord [recordID=" + recordID + ", recordSYS=" + recordSYS + ", indexTimestamp=" + indexTimestamp
				+ ", solrFields=" + solrFields + ", fullRecord=" + fullRecord + "]";
	}
}