/**
 * Relating a record to another record that represents an "other edition" of the current record.
 * E. g.: Printed edition of a journal relates to the electronic edition of the same journal.
 *        Spanish edition of a Series relates to the english edition of the same series.
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

import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class OtherEdition {

	private HttpSolrServer solrServerBiblio;
	private RelationHelper relationHelper;
	private SolrMabHelper smHelper;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private int NO_OF_ROWS = 500;
	private boolean print = false;
	private long noOfDocs = 0;
	private int counter = 0;
	
	public OtherEdition(HttpSolrServer solrServerBiblio, String timeStamp, boolean print) {
		this.solrServerBiblio = solrServerBiblio;
		this.print = print;
		this.smHelper = new SolrMabHelper();
		this.relationHelper = new RelationHelper(solrServerBiblio, null, timeStamp);
	}


	/**
	 * Adding link to "other edition" based on the information of the record of the current edition. 
	 */
	public void addOtherEditions() {

		// Get all currently indexed records that are containing data for other editions
		SolrDocumentList queryResults = relationHelper.getCurrentlyIndexedRecordsWithOtherEdition(true, null);

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

				// Set the SYS No of the other edition to the record of the current edition
				lastDocId = setOtherEdition(isFirstPage, lastDocId);

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
				setOtherEdition(isFirstPage, lastDocId);

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
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				queryResults = null;
			}			
		}
	}


	/**
	 * Actually setting the ID-No. and other data of the "other edition" to the current record by using atomic updates.
	 *  
	 * @param isFirstPage	boolean: true if first page of Solr result
	 * @param lastDocId		String: Doc Id of the last processed Solr document
	 * @return				String: The new "last doc id" for the next iteration
	 */
	private String setOtherEdition(boolean isFirstPage, String lastDocId) {
		// Variable for return value
		String returnValue = null;

		// Get currently indexed records that are containing data for other editions (use paging in Solr query for better performance)
		SolrDocumentList currentIndexedRecords = relationHelper.getCurrentlyIndexedRecordsWithOtherEdition(isFirstPage, lastDocId);
		String newLastDocId = currentIndexedRecords.get(currentIndexedRecords.size()-1).getFieldValue("id").toString();
		String docId = null;

		for (SolrDocument currentIndexedRecord : currentIndexedRecords) {
			docId = (currentIndexedRecord.getFieldValue("id") != null) ? currentIndexedRecord.getFieldValue("id").toString() : null;
			Collection<Object> currentEditions = (currentIndexedRecord != null && currentIndexedRecord.getFieldValues("otherEdition_str_mv") != null && !currentIndexedRecord.getFieldValues("otherEdition_str_mv").isEmpty()) ? currentIndexedRecord.getFieldValues("otherEdition_str_mv") : null;
			
			if (currentEditions != null) {
				int loopCounter = 0;
				ArrayList<String> newOtherEditionsInfo = new ArrayList<String>();
				
				for (Object currentEdition : currentEditions) {
					loopCounter = loopCounter + 1;
					
					String otherEditionSys = null;
					if (loopCounter % 4 == 0) {
						// Evere 4th value of Solr field "otherEdition_str_mv" should be an ID of another edition. See also mab.properties.
						// The indexing-rules for the mentioned field should be like this (pay attention to 3rd square bracket of "connectedSubfields"):
						// otherEdition_str_mv: 527$**$a, connectedSubfields[p:NoOtherEditionType][n:NoOtherEditionComment][9:NoOtherEditionId], allowDuplicates, multiValued
						String otherEditionId = (currentEdition != null) ? currentEdition.toString() : null;
						
						if (otherEditionId != null) {
							otherEditionSys = getOtherEditionSys(otherEditionId);
							otherEditionSys = (otherEditionSys != null) ? otherEditionSys : "NoOtherEditionSys";
						}
					}
					
					String editionInfo = (currentEdition != null) ? currentEdition.toString() : "NoOtherEditionInfo";
					newOtherEditionsInfo.add(editionInfo);
					newOtherEditionsInfo.add(otherEditionSys);
				}
				
				// Prepare current record for atomic updates:
				SolrInputDocument atomicUpdateDoc = null;
				atomicUpdateDoc = new SolrInputDocument();
				atomicUpdateDoc.setField("id", docId);

				// Add other edition info to a new Solr field for displaying (otherEditionDisplay_str_mv).
				// It contains the SYS-No (if available) to the record of the other edition.
				Map<String, ArrayList<String>> mapOtherEdition = new HashMap<String, ArrayList<String>>();
				mapOtherEdition.put("set", newOtherEditionsInfo);
				atomicUpdateDoc.setField("otherEditionDisplay_str_mv", mapOtherEdition);
				
				docsForAtomicUpdates.add(atomicUpdateDoc);
			}

			counter = counter + 1;
			this.smHelper.print(this.print, "Linking \"other editions\". Processing record no " + counter  + " of " + noOfDocs + "                                      \r");


			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}

		return returnValue;


	}
	/**
	 * Getting SYS No. (ID) of the "other edition" record by its AC-No. or ZDB-ID
	 * 
	 * @param id		AC-No. or ZDB-ID of record
	 * @return			String containing ID of the record of the "other edition"
	 */
	private String getOtherEditionSys(String id) {
		String otherEditionSys = null;
		SolrQuery queryOtherEdition = new SolrQuery(); // New Solr query
		queryOtherEdition.setQuery("acNo_txt:\""+id+"\" || zdbId_txt:\""+id+"\""); // Define a query
		queryOtherEdition.setFields("id"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultDocList = this.solrServerBiblio.query(queryOtherEdition).getResults();
			if (resultDocList != null && resultDocList.getNumFound() > 0) {
				// Get ID of "other edition". We only take the first one, because we only can link to one other edition.
				otherEditionSys = resultDocList.get(0).getFieldValue("id").toString();
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return otherEditionSys;
	}
}