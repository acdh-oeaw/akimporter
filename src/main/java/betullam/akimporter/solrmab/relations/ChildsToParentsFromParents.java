/**
 * Linking child records to parent records according to
 * the information found in the respective parent record.
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import main.java.betullam.akimporter.solrmab.SolrMabHelper;

/**
 * Class to link child records to parent records based on the information of the parent record.
 * 
 * A parent record may be updated. Then, the existing parent record in solr will be overwritten because it will be indexed from scratch.
 * So the info for its child records will be deleted too. If a child record of that parent is indexed too with the update, then everything
 * is OK because this one and all other childs will be added to the parent. If no child record is indexed along with the update, the parent
 * will remain without children. Thats why we need to look if the record has children, and if not, we need to see if there are children at
 * all. If yes, we need to link them to the parent again. If not, then there are no children and we can leave the parent record as it is.
 */
public class ChildsToParentsFromParents {

	// General variables
	private HttpSolrServer solrServer;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private RelationHelper relationHelper;
	private SolrMabHelper smHelper = new SolrMabHelper();
	private int NO_OF_ROWS = 500;
	private Set<String> parentSYSs = new HashSet<String>();
	private boolean print = false;

	/**
	 * Constructor for indexing infos from child records to parent records
	 * 
	 * @param solrServer	Solr server we want to index to
	 * @param timeStamp		Timestamp of moment the import process started
	 * @param print			True if status messages should be printed to console
	 */
	public ChildsToParentsFromParents(HttpSolrServer solrServer, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServer, null, timeStamp);
	}


	/**
	 * This handles the linking of child records to parent records based on the information of the parent record.
	 */
	public void addChildsToParentsFromParents() {

		SolrDocumentList recordsWithNoChilds = relationHelper.getCurrentlyIndexedRecordsWithNoChilds(true, null);

		// Get the number of documents that were found
		long noOfDocs = recordsWithNoChilds.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (recordsWithNoChilds != null && noOfDocs > 0) {

			// Clear query results. We don't need them anymore.
			recordsWithNoChilds.clear();
			recordsWithNoChilds = null;

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			
			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the AC or SYS Nos from the parents to a class variable of type "Set<String>" to get a list without duplicated
				// values to avoid an overhead
				lastDocId = setParentAtomicUpdateDocs(isFirstPage, lastDocId);
				
				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates, solrServer);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			
			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setParentAtomicUpdateDocs(isFirstPage, lastDocId);
				
				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates, solrServer);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			
			// Commit the changes
			try {
				this.solrServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				parentSYSs.clear();
				parentSYSs = null;
				recordsWithNoChilds = null;
			}			
		}
	}


	/**
	 * Adding child records to all parents having unlinked child records
	 * 
	 * @param isFirstPage	true if first page of Solr result
	 * @param lastDocId		Doc Id of the last processed Solr document
	 * @return
	 */
	public String setParentAtomicUpdateDocs(boolean isFirstPage, String lastDocId) {
		
		// Variable for return value:
		String returnValue = null;

		// Records without childs (these could be parents with childs that are not linked yet)
		//SolrDocumentList recordsWithNoChilds = relationHelper.getCurrentlyIndexedRecordsWithNoChilds(true, null);
		SolrDocumentList recordsWithNoChilds = relationHelper.getCurrentlyIndexedRecordsWithNoChilds(isFirstPage, lastDocId);

		int counter = 0;
		long noOfParents = recordsWithNoChilds.getNumFound();

		// Deep paging stuff (better performance)
		String newLastDocId = recordsWithNoChilds.get(recordsWithNoChilds.size()-1).getFieldValue("id").toString();
		String docId = null;

		for (SolrDocument recordWithNoChild : recordsWithNoChilds) {

			String parentSYS = (recordWithNoChild.getFieldValue("id") != null) ? recordWithNoChild.getFieldValue("id").toString() : null;

			if (parentSYS != null) {
				
				// Get all non deleted childs of this records
				SolrDocumentList nonDeletedChilds = getNonDeletedChildsByParentSYS(parentSYS);
				long noOfNonDeletedChilds = nonDeletedChilds.getNumFound();
				
				
				// If childs are existing, link them to it's parent
				if (nonDeletedChilds != null && noOfNonDeletedChilds > 0) {

					// Some Lists to add infos from multiple child records
					List<String> childTypes = new ArrayList<String>();
					List<String> childSYSs = new ArrayList<String>();
					List<String> childACs = new ArrayList<String>();
					List<String> childTitles = new ArrayList<String>();
					List<String> childVolumeNos = new ArrayList<String>();
					List<String> childVolumeNosSort = new ArrayList<String>();
					List<String> childEditions = new ArrayList<String>();
					List<String> childPublishDates = new ArrayList<String>();

					// Add childs to this parent
					for (SolrDocument nonDeletedChild : nonDeletedChilds) {

						// Get type of child record (multivolume, serialvolume, etc.)
						String childType = relationHelper.getChildRecordType(nonDeletedChild);

						// Get child infos
						String childSys = (nonDeletedChild.getFieldValue("sysNo_txt") != null) ? nonDeletedChild.getFieldValue("sysNo_txt").toString() : "0";
						String childAc = (nonDeletedChild.getFieldValue("acNo_txt") != null) ? nonDeletedChild.getFieldValue("acNo_txt").toString() : "0";
						String childTitle = (nonDeletedChild.getFieldValue("title") != null) ? nonDeletedChild.getFieldValue("title").toString() : "0";
						String childVolumeNo = "0";
						String childVolumeNoSort = "0";
						if (childType.equals("multivolume")) {
							childVolumeNo = (nonDeletedChild.getFieldValue("multiVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNo_str").toString() : "0";
							childVolumeNoSort = (nonDeletedChild.getFieldValue("multiVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNoSort_str").toString() : "0";
						} else if (childType.equals("serialvolume")) {
							childVolumeNo = (nonDeletedChild.getFieldValue("serialVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNo_str").toString() : "0";
							childVolumeNoSort = (nonDeletedChild.getFieldValue("serialVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNoSort_str").toString() : "0";
						} else if (childType.equals("article")) {
							childVolumeNo = "0";
							childVolumeNoSort = "0";
						}
						String childEdition = (nonDeletedChild.getFieldValue("edition") != null) ? nonDeletedChild.getFieldValue("edition").toString() : "0";
						String childPublishDate = (nonDeletedChild.getFieldValue("publishDate") != null) ? nonDeletedChild.getFieldValue("publishDate").toString() : "0";

						// Add child infos to Lists
						childTypes.add(childType);
						childSYSs.add(childSys);
						childACs.add(childAc);
						childTitles.add(childTitle);
						childVolumeNos.add(childVolumeNo);
						childVolumeNosSort.add(childVolumeNoSort);
						childEditions.add(childEdition);
						childPublishDates.add(childPublishDate);						
					}

					// Prepare parent record for atomic updates:
					SolrInputDocument linkedChild = null;
					linkedChild = new SolrInputDocument();
					linkedChild.setField("id", parentSYS);

					// Set values for atomic update of parent record:
					Map<String, List<String>> mapChildType = new HashMap<String, List<String>>();
					mapChildType.put("set", childTypes);
					linkedChild.setField("childType_str_mv", mapChildType);

					Map<String, List<String>> mapChildSYS = new HashMap<String, List<String>>();
					mapChildSYS.put("set", childSYSs);
					linkedChild.setField("childSYS_str_mv", mapChildSYS);

					Map<String, List<String>> mapChildAC = new HashMap<String, List<String>>();
					mapChildAC.put("set", childACs);
					linkedChild.setField("childAC_str_mv", mapChildAC);

					Map<String, List<String>> mapChildTitle = new HashMap<String, List<String>>();
					mapChildTitle.put("set", childTitles);
					linkedChild.setField("childTitle_str_mv", mapChildTitle);

					Map<String, List<String>> mapChildVolumeNo = new HashMap<String, List<String>>();
					mapChildVolumeNo.put("set", childVolumeNos);
					linkedChild.setField("childVolumeNo_str_mv", mapChildVolumeNo);

					Map<String, List<String>> mapChildVolumeNoSort = new HashMap<String, List<String>>();
					mapChildVolumeNoSort.put("set", childVolumeNosSort);
					linkedChild.setField("childVolumeNoSort_str_mv", mapChildVolumeNoSort);

					Map<String, List<String>> mapChildEdition = new HashMap<String, List<String>>();
					mapChildEdition.put("set", childEditions);
					linkedChild.setField("childEdition_str_mv", mapChildEdition);

					Map<String, List<String>> mapChildPublishDate = new HashMap<String, List<String>>();
					mapChildPublishDate.put("set", childPublishDates);
					linkedChild.setField("childPublishDate_str_mv", mapChildPublishDate);

					docsForAtomicUpdates.add(linkedChild);
				}
			}

			counter = counter + 1;
			this.smHelper.print(this.print, "Linking childs to parent from unlinked parents. Processing record no " + counter  + " of " + noOfParents + "                 \r");
			
			docId = (recordWithNoChild.getFieldValue("id") != null) ? recordWithNoChild.getFieldValue("id").toString() : null;

			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}
		
		return returnValue;
	}

	
	/**
	 * Getting all child records that are not deleted, based on the Aleph SYS no. of the parent record.
	 * 
	 * @param	parentSYS	The Aleph SYS no. of the parent record
	 * @return				A SolrDocumentList object
	 */
	private SolrDocumentList getNonDeletedChildsByParentSYS(String parentSYS) {

		SolrDocumentList nonDeletedChildRecords = null;

		// New Solr query
		SolrQuery querynonDeletedChilds = new SolrQuery();

		// Add sorting
		querynonDeletedChilds.addSort(new SortClause("id", SolrQuery.ORDER.asc));

		// Set rows to max integer value so that we really get all child records.
		// This should not be the bottleneck concerning performance because no parent record will have
		// such a high number of child volumes
		querynonDeletedChilds.setRows(Integer.MAX_VALUE);

		// Define a query for getting all documents. We get the deleted records with a filter query because of performance (see below)
		querynonDeletedChilds.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that are child volumes
		// (because we need to get their parent records to be able to unlink these childs from there).
		querynonDeletedChilds.setFilterQueries("parentSYS_str_mv:"+parentSYS, "-deleted_str:Y");

		// Set fields that should be given back from the query
		querynonDeletedChilds.setFields(
				"sysNo_txt",
				"acNo_txt",
				"title",
				"multiVolumeNo_str",
				"multiVolumeNoSort_str",
				"serialVolumeNo_str",
				"serialVolumeNoSort_str",
				"edition",
				"publishDate",
				"parentSYS_str_mv",
				"parentMultiAC_str",
				"parentSeriesAC_str_mv",
				"articleParentAC_str"
				);
		try {
			nonDeletedChildRecords = solrServer.query(querynonDeletedChilds).getResults();			
		} catch (SolrServerException e) {
			nonDeletedChildRecords = null;
			e.printStackTrace();
		}

		return nonDeletedChildRecords;
	}

}
