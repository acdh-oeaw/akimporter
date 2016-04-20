/**
 * Unlinking childs to parents and vice versa.
 * Is necessary before we relate all childs to their parents (see Relate class)
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
package main.java.betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class UnlinkChildsFromParents {

	// General variables
	HttpSolrServer solrServer;
	Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	RelationHelper relationHelper;
	private SolrMabHelper smHelper = new SolrMabHelper();
	int NO_OF_ROWS = 500;
	List<String> parentAcs = new ArrayList<String>();
	boolean print = false;

	
	/**
	 * Constructor for a class which unlinks child records from their parent records.
	 * @param solrServer	The Solr server where the records are stored.
	 * @param timeStamp		Timestamp of moment the import process started.
	 * @param print			True if status messages should be printed to console.
	 */
	public UnlinkChildsFromParents(HttpSolrServer solrServer, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServer, null, timeStamp);
	}



	/**
	 * Handling the unlinking of child records from their parent records.
	 */
	public void unlinkChildsFromParents() {

		SolrDocumentList queryResults = relationHelper.getCurrentlyIndexedChildRecords(true, null);

		// Show how many documents were found
		long noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Clear query. We don't need it anymore.
			queryResults.clear();
			queryResults = null;

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the AC Nos and record types from the parents to a class variable of type "Map<String, String>" to get a
				// list without duplicated values to avoid an overhead
				lastDocId = setParentAcsFromWhichToUnlink(isFirstPage, lastDocId);
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;
				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setParentAcsFromWhichToUnlink(isFirstPage, lastDocId);
			}
			
			// Set the documents for atomic updates to a class variable of type "Collection<SolrInputDocument>" and add that to Solr
			setParentAtomicUpdateDocs();

			try {
				this.solrServer.commit(); // Commit the changes
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				parentAcs.clear();
				parentAcs = null;
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				queryResults = null;
			}
		}
	}



	/**
	 * Add all parent AC numbers to a class variable to have no duplicated values because this would cause an overhead.
	 * 
	 * @param isFirstPage	True if first page of Solr results
	 * @param lastDocId		Doc Id of the previous last processed Solr document
	 * @return				Doc Id of the new last processed Solr document
	 */
	public String setParentAcsFromWhichToUnlink(boolean isFirstPage, String lastDocId) {

		// Variable for return value:
		String returnValue = null;

		SolrDocumentList childRecords = relationHelper.getCurrentlyIndexedChildRecords(isFirstPage, lastDocId);

		if (childRecords.size() > 0) {
			String newLastDocId = childRecords.get(childRecords.size()-1).getFieldValue("id").toString();

			String docId = null;

			for (SolrDocument childRecord : childRecords) {
				Set<String> arrParentAcsSingleChild = relationHelper.getDedupParentAcsFromSingleChild(childRecord);
				//String recordType = relationHelper.getChildRecordType(childRecord);
				if (arrParentAcsSingleChild != null && arrParentAcsSingleChild.size() > 0) {
					for (String parentAc : arrParentAcsSingleChild) {
						parentAcs.add(parentAc);
					}
				}
				docId = (childRecord.getFieldValue("id") != null) ? childRecord.getFieldValue("id").toString() : null;

				// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
				if (docId.equals(newLastDocId)) {
					returnValue = docId;
				}
			}
		}

		return returnValue;
	}


	/**
	 * Set documents for atomic Solr update an index them.
	 */
	private void setParentAtomicUpdateDocs() {

		int counter = 0;
		int noOfParents = parentAcs.size();
		
		if (noOfParents > 0) {

			for (String parentAc : parentAcs) {
				
				SolrDocument parentRecord = relationHelper.getParentRecord(parentAc);
				
				if (parentRecord != null) {

					String parentSys = (parentRecord.getFieldValue("id") != null) ? parentRecord.getFieldValue("id").toString() : "0";
					
					// Prepare parent record for atomic updates:
					SolrInputDocument deleteChild = null;
					deleteChild = new SolrInputDocument();
					deleteChild.setField("id", parentSys);
					
					// Unlink all child records:
					Map<String, List<String>> mapRemChildType = new HashMap<String, List<String>>();
					mapRemChildType.put("set", null);
					deleteChild.setField("childType_str_mv", mapRemChildType);
					
					Map<String, String> mapRemSYS = new HashMap<String, String>();
					mapRemSYS.put("set", null);
					deleteChild.setField("childSYS_str_mv", mapRemSYS);
					
					Map<String, String> mapRemAC = new HashMap<String, String>();
					mapRemAC.put("set", null);
					deleteChild.setField("childAC_str_mv", mapRemAC);

					Map<String, String> mapRemTitle = new HashMap<String, String>();
					mapRemTitle.put("set", null);
					deleteChild.setField("childTitle_str_mv", mapRemTitle);

					Map<String, String> mapRemVolumeNo = new HashMap<String, String>();
					mapRemVolumeNo.put("set", null);
					deleteChild.setField("childVolumeNo_str_mv", mapRemVolumeNo);

					Map<String, String> mapRemVolumeNoSort = new HashMap<String, String>();
					mapRemVolumeNoSort.put("set", null);
					deleteChild.setField("childVolumeNoSort_str_mv", mapRemVolumeNoSort);

					Map<String, String> mapRemEdition = new HashMap<String, String>();
					mapRemEdition.put("set", null);
					deleteChild.setField("childEdition_str_mv", mapRemEdition);

					Map<String, String> mapRemPublishDate = new HashMap<String, String>();
					mapRemPublishDate.put("set", null);
					deleteChild.setField("childPublishDate_str_mv", mapRemPublishDate);
					
					docsForAtomicUpdates.add(deleteChild);
				}
				
				counter = counter + 1;
				
				// Add documents from the class variable which was set before to Solr
				if (counter % NO_OF_ROWS == 0) { // Every n-th record, add documents to solr
					relationHelper.indexDocuments(docsForAtomicUpdates, solrServer);
					docsForAtomicUpdates.clear();
					docsForAtomicUpdates = null;
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				} else if (counter >= noOfParents) { // The remainding documents
					relationHelper.indexDocuments(docsForAtomicUpdates, solrServer);
					docsForAtomicUpdates.clear();
					docsForAtomicUpdates = null;
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				}

				this.smHelper.print(this.print, "Unlinking childs from parent " + parentAc + ". Processing record no " + counter  + " of " + noOfParents + "                   \r");
			}

		}

	}


}
