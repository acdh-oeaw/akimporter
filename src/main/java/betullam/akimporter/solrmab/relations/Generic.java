/**
 * Relating a record to another record.
 * This is a more generic code that links records to each other the same way, but for different
 * relation types (e. g. other edition, attachments). In fact, these are linked to each other
 * exactly the same way, but the link type must be distinguishable so that we use separat Solr
 * fields.
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

package main.java.betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class Generic {

	private HttpSolrServer solrServerBiblio;
	private RelationHelper relationHelper;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private int NO_OF_ROWS = 500;
	private boolean print = false;
	private long noOfDocs = 0;
	private int counter = 0;
	private String relationType;
	
	public Generic(HttpSolrServer solrServerBiblio, String relationType, String timeStamp, boolean print) {
		this.solrServerBiblio = solrServerBiblio;
		this.relationType = relationType;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServerBiblio, null, timeStamp);
	}


	/**
	 * Adding link to related records based on the information of the current indexed record. 
	 */
	public void addGenericLink() {

		// Get all currently indexed records that are containing data for generic related records
		SolrDocumentList queryResults = relationHelper.getCurrentlyIndexedRecordsWithGenericRelations(relationType, true, null);

		// Get the number of documents that were found
		noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Clear query results. We don't need them anymore.
			queryResults.clear();
			queryResults = null;

			// Calculate the number of Solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the SYS No of the related record to the current record
				lastDocId = setGenericRelation(isFirstPage, lastDocId);

				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setGenericRelation(isFirstPage, lastDocId);

				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}


			// Commit the changes
			try {
				this.solrServerBiblio.commit();
			} catch (SolrServerException e) {
				System.err.println("Error while generic relating");
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Error while generic relating");
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				queryResults = null;
			}			
		}
	}


	/**
	 * Actually setting the ID-No. and other data of the related records to the current record by using atomic updates.
	 *  
	 * @param isFirstPage	boolean: true if first page of Solr result
	 * @param lastDocId		String: Doc Id of the last processed Solr document
	 * @return				String: The new "last doc id" for the next iteration
	 */
	private String setGenericRelation(boolean isFirstPage, String lastDocId) {
		// Variable for return value
		String returnValue = null;

		// Get currently indexed records that are containing data for related records (use paging in Solr query for better performance)
		SolrDocumentList currentIndexedRecords = relationHelper.getCurrentlyIndexedRecordsWithGenericRelations(relationType, isFirstPage, lastDocId);
		String newLastDocId = currentIndexedRecords.get(currentIndexedRecords.size()-1).getFieldValue("id").toString();
		String docId = null;
		
		// Variables for generic indexing
		String relationField = null;
		String placeholderNoSys = null;
		String newSolrFieldname = null;
		String consoleDisplayText = null;
		if (relationType.equals("otherEdition")) {
			relationField = "otherEdition_str_mv";
			placeholderNoSys = "NoOtherEditionSys";
			newSolrFieldname = "otherEditionDisplay_str_mv";
			consoleDisplayText = "other editions";
		}
		if (relationType.equals("attachment")) {
			relationField = "attachment_str_mv";
			placeholderNoSys = "NoAttachmentSys";
			newSolrFieldname = "attachmentDisplay_str_mv";
			consoleDisplayText = "attachments";
		}
		if (relationType.equals("attachementTo")) {
			relationField = "attachmentTo_str_mv";
			placeholderNoSys = "NoAttachmentToSys";
			newSolrFieldname = "attachmentToDisplay_str_mv";
			consoleDisplayText = "attachments to";
		}
		if (relationType.equals("predecessor")) {
			relationField = "predecessor_str_mv";
			placeholderNoSys = "NoPredecessorId";
			newSolrFieldname = "predecessorDisplay_str_mv";
			consoleDisplayText = "predecessors";
		}
		if (relationType.equals("successor")) {
			relationField = "successor_str_mv";
			placeholderNoSys = "NoSuccessorId";
			newSolrFieldname = "successorDisplay_str_mv";
			consoleDisplayText = "successors";
		}
		if (relationType.equals("otherRelation")) {
			relationField = "otherRelation_txt_mv";
			placeholderNoSys = "NoOtherRelationId";
			newSolrFieldname = "otherRelationDisplay_txt_mv";
			consoleDisplayText = "other relations";
		}

		for (SolrDocument currentIndexedRecord : currentIndexedRecords) {
			docId = (currentIndexedRecord.getFieldValue("id") != null) ? currentIndexedRecord.getFieldValue("id").toString() : null;
			Collection<Object> currentRelatedRecords = (currentIndexedRecord != null && currentIndexedRecord.getFieldValues(relationField) != null && !currentIndexedRecord.getFieldValues(relationField).isEmpty()) ? currentIndexedRecord.getFieldValues(relationField) : null;
			
			if (currentRelatedRecords != null) {
				int loopCounter = 0;
				ArrayList<String> newRelatedInfo = new ArrayList<String>();
				
				for (Object currentRelatedRecord : currentRelatedRecords) {
					loopCounter = loopCounter + 1;
					
					String relatedRecordSys = null;
					if (loopCounter % 4 == 0) {
						// TODO: Do this in a more elegant way: It should not always have to be the 4th field!
						// Every 4th value of Solr field should be an ID of a related record. See also mab.properties.
						// The indexing-rules for the mentioned fields should be like this (pay attention to 3rd square bracket of "connectedSubfields"):
						// otherEdition_str_mv	: 527$**$a, connectedSubfields[p:NoOtherEditionType][n:NoOtherEditionComment][9:NoOtherEditionId], allowDuplicates, multiValued
						// attachment_str_mv	: 529$**$a, connectedSubfields[p:NoAttachementType][n:NoAttachementComment][9:NoAttachementId], allowDuplicates, multiValued
						// attachmentTo_str_mv	: 530$**$a, connectedSubfields[p:NoAttachementToType][n:NoAttachementToComment][9:NoAttachementToId], allowDuplicates, multiValued

						String relatedRecordId = (currentRelatedRecord != null) ? currentRelatedRecord.toString() : null;
						
						if (relatedRecordId != null) {
							relatedRecordSys = getRelatedRecordSys(relatedRecordId);
							relatedRecordSys = (relatedRecordSys != null) ? relatedRecordSys : placeholderNoSys;
						}
					}
					
					String relationInfo = (currentRelatedRecord != null) ? currentRelatedRecord.toString() : "NoRelationInfo";
					newRelatedInfo.add(relationInfo);
					newRelatedInfo.add(relatedRecordSys);
				}
				
				// Prepare current record for atomic updates:
				SolrInputDocument atomicUpdateDoc = null;
				atomicUpdateDoc = new SolrInputDocument();
				atomicUpdateDoc.setField("id", docId);

				// Add related record info to a new Solr field for displaying (see variable "newSolrFieldname").
				// It contains (besides other data) the SYS-No (if available, if not: a placeholder text) to the
				// record of the related record.
				Map<String, ArrayList<String>> mapRelation = new HashMap<String, ArrayList<String>>();
				mapRelation.put("set", newRelatedInfo);
				atomicUpdateDoc.setField(newSolrFieldname, mapRelation);
				
				docsForAtomicUpdates.add(atomicUpdateDoc);
			}

			counter = counter + 1;
			AkImporterHelper.print(this.print, "\nLinking \"" + consoleDisplayText + "\". Processing record no " + counter  + " of " + noOfDocs);

			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}

		return returnValue;


	}
	
	
	/**
	 * Getting SYS No. (ID) of the related record by its AC-No. or ZDB-ID
	 * 
	 * @param id		E. g. AC-No. or ZDB-ID
	 * @return			String containing ID of the related record
	 */
	private String getRelatedRecordSys(String id) {
		String relatedRecordSys = null;
		SolrQuery queryRelatedRecord = new SolrQuery(); // New Solr query
		queryRelatedRecord.setQuery("id:\""+id+"\" || acNo_txt:\""+id+"\" || ids_txt_mv:\""+id+"\""); // Define a query
		queryRelatedRecord.setFields("id"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultDocList = this.solrServerBiblio.query(queryRelatedRecord).getResults();
			if (resultDocList != null && resultDocList.getNumFound() > 0) {
				// Get ID of related record. We only take the first one, because we only link to one other related record per line.
				relatedRecordSys = resultDocList.get(0).getFieldValue("id").toString();
			}
		} catch (SolrServerException e) {
			System.err.println("Error while generic relating");
			e.printStackTrace();
		}

		return relatedRecordSys;
	}
}