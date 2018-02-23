/**
 * Linking child records to parent records according to
 * the information found in the respective child record.
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

import main.java.betullam.akimporter.main.AkImporterHelper;

public class ChildsToParentsFromChilds {

	// General variables
	private HttpSolrServer solrServer;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private RelationHelper relationHelper;
	private int NO_OF_ROWS = 500;
	private int CHILD_INDEX_RATE = 250;
	private Set<String> parentSYSs = new HashSet<String>();
	private boolean print = false;


	/**
	 * Constructor for setting some variables.
	 * 
	 * @param solrServer	String indicating the URL incl. core name of the Solr bibliographic index (e. g. http://localhost:8080/solr/biblio)
	 * @param timeStamp		A unix time stamp as a String or null
	 * @param print			boolean indicating whether to print status messages or not
	 */
	public ChildsToParentsFromChilds(HttpSolrServer solrServer, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServer, null, timeStamp);
	}


	/**
	 * Adding child records to parent records based on the information of the child record. 
	 */
	public void addChildsToParentsFromChilds() {

		SolrDocumentList queryResults = relationHelper.getCurrentlyIndexedChildRecords(true, null);

		// Get the number of documents that were found
		long noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Clear query results. We don't need them anymore.
			queryResults.clear();
			queryResults = null;

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);


			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the AC or SYS Nos from the parents to a class variable of type "Set<String>" to get a list without duplicated
				// values to avoid an overhead
				lastDocId = setParentSYSsForLinking(isFirstPage, lastDocId);
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setParentSYSsForLinking(isFirstPage, lastDocId);
			}

			setParentAtomicUpdateDocs();

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
				queryResults = null;
			}			
		}
	}

	
	/**
	 * Adding all parent SYS numbers to a class variable to have no duplicated values because this would cause an overhead.
	 * 
	 * @param	isFirstPage		True if first page of Solr results	
	 * @param	lastDocId		Doc ID of the last processed Solr document
	 * @return
	 */
	public String setParentSYSsForLinking(boolean isFirstPage, String lastDocId) {
		// Variable for return value:
		String returnValue = null;

		SolrDocumentList childRecords = relationHelper.getCurrentlyIndexedChildRecords(isFirstPage, lastDocId);
		String newLastDocId = childRecords.get(childRecords.size()-1).getFieldValue("id").toString();

		String docId = null;

		for (SolrDocument childRecord : childRecords) {
			Set<String> parentSYSsFromChild = relationHelper.getDedupParentSYSsFromSingleChild(childRecord);

			if (parentSYSsFromChild != null && parentSYSsFromChild.size() > 0) {
				for (String parentSYS : parentSYSsFromChild) {
					parentSYSs.add(parentSYS);
				}
			}
			docId = (childRecord.getFieldValue("id") != null) ? childRecord.getFieldValue("id").toString() : null;

			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}

		return returnValue;
	}


	/**
	 * Set documents for atomic Solr update and index them.
	 */
	private void setParentAtomicUpdateDocs() {

		int counter = 0;
		int noOfParents = parentSYSs.size();

		if (noOfParents > 0) {

			for (String parentSYS : parentSYSs) {

				// Some Lists to add infos from multiple child records
				List<String> childTypes = new ArrayList<String>();
				List<String> childSYSs = new ArrayList<String>();
				List<String> childACs = new ArrayList<String>();
				List<String> childTitles = new ArrayList<String>();
				List<String> childVolumeNos = new ArrayList<String>();
				List<String> childVolumeNosSort = new ArrayList<String>();
				List<String> childEditions = new ArrayList<String>();
				List<String> childPublishDates = new ArrayList<String>();
				List<String> childIssueNos = new ArrayList<String>();
				List<String> childPagesFrom = new ArrayList<String>();
				List<String> childPagesTo = new ArrayList<String>();
				List<String> childLevels = new ArrayList<String>();
				List<String> childSortLogIds = new ArrayList<String>();
				List<String> childUrls = new ArrayList<String>();

				// Query all childs from a parent except the deleted ones
				SolrDocumentList nonDeletedChilds = getNonDeletedChildsByParentSYS(parentSYS);

				// Add all non deleted childs to it's parents:
				for (SolrDocument nonDeletedChild : nonDeletedChilds) {

					// Get type of child record (multivolume, serialvolume, etc.)
					String childType = relationHelper.getChildRecordType(nonDeletedChild);

					// Get parentSYSs from child record
					String[] arrParentSYSsOfChild = (childType != null && nonDeletedChild.getFieldValues("parentSYS_str_mv") != null) ? nonDeletedChild.getFieldValues("parentSYS_str_mv").toArray(new String[0]) : null;

					if (arrParentSYSsOfChild != null && arrParentSYSsOfChild.length > 0) {
						
						if (this.isChildOfParent(arrParentSYSsOfChild, parentSYS)) {

							// Get child infos
							String childSys = (nonDeletedChild.getFieldValue("sysNo_txt") != null) ? nonDeletedChild.getFieldValue("sysNo_txt").toString() : "0";
							String childAc = (nonDeletedChild.getFieldValue("acNo_txt") != null) ? nonDeletedChild.getFieldValue("acNo_txt").toString() : "0";
							String childTitle = (nonDeletedChild.getFieldValue("title_part_txt") != null) ? nonDeletedChild.getFieldValue("title_part_txt").toString() : null;
							System.out.println(childTitle);
							if (childTitle == null) {
								childTitle = (nonDeletedChild.getFieldValue("title") != null) ? nonDeletedChild.getFieldValue("title").toString() : "0";
							}
							
							String childVolumeNo = "0";
							String childVolumeNoSort = "0";
							if (childType.equals("multivolume")) {
								childVolumeNo = (nonDeletedChild.getFieldValue("multiVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNo_str").toString() : "0";
								childVolumeNoSort = (nonDeletedChild.getFieldValue("multiVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNoSort_str").toString() : "0";
							} else if (childType.equals("serialvolume")) {
								childVolumeNo = (nonDeletedChild.getFieldValue("serialVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNo_str").toString() : "0";
								childVolumeNoSort = (nonDeletedChild.getFieldValue("serialVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNoSort_str").toString() : "0";
							} else if (childType.equals("article")) {
								childVolumeNo = (nonDeletedChild.getFieldValue("articleParentVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("articleParentVolumeNo_str").toString() : "0";
								childVolumeNoSort = (nonDeletedChild.getFieldValue("articleParentVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("articleParentVolumeNo_str").toString() : "0";
							}
							String childEdition = (nonDeletedChild.getFieldValue("edition") != null) ? nonDeletedChild.getFieldValue("edition").toString() : "0";
							String childPublishDate = (nonDeletedChild.getFieldValues("publishDate") != null && !nonDeletedChild.getFieldValues("publishDate").isEmpty()) ? nonDeletedChild.getFieldValues("publishDate").iterator().next().toString() : "0";
							String childIssueNo = (nonDeletedChild.getFieldValue("articleParentIssue_str") != null) ? nonDeletedChild.getFieldValue("articleParentIssue_str").toString() : "0";
							String childPageFrom = (nonDeletedChild.getFieldValue("pageFrom_str") != null) ? nonDeletedChild.getFieldValue("pageFrom_str").toString() : "0";
							String childPageTo = (nonDeletedChild.getFieldValue("pageTo_str") != null) ? nonDeletedChild.getFieldValue("pageTo_str").toString() : "0";
							String childLevel = (nonDeletedChild.getFieldValue("level_str") != null) ? nonDeletedChild.getFieldValue("level_str").toString() : "0";
							String childSortLogId = (nonDeletedChild.getFieldValue("sortNoLogId_str") != null) ? nonDeletedChild.getFieldValue("sortNoLogId_str").toString() : "0";
							String childUrl = (nonDeletedChild.getFieldValues("url") != null && !nonDeletedChild.getFieldValues("url").isEmpty()) ? nonDeletedChild.getFieldValues("url").iterator().next().toString() : "0";

							// Add child infos to Lists
							childTypes.add(childType);
							childSYSs.add(childSys);
							childACs.add(childAc);
							childTitles.add(childTitle);
							childVolumeNos.add(childVolumeNo);
							childVolumeNosSort.add(childVolumeNoSort);
							childEditions.add(childEdition);
							childPublishDates.add(childPublishDate);
							childIssueNos.add(childIssueNo);
							childPagesFrom.add(childPageFrom);
							childPagesTo.add(childPageTo);
							childLevels.add(childLevel);
							childSortLogIds.add(childSortLogId);
							childUrls.add(childUrl);
						}
					}
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
				
				Map<String, List<String>> mapChildIssueNo = new HashMap<String, List<String>>();
				mapChildIssueNo.put("set", childIssueNos);
				linkedChild.setField("childIssueNo_str_mv", mapChildIssueNo);
				
				Map<String, List<String>> mapChildPageFrom = new HashMap<String, List<String>>();
				mapChildPageFrom.put("set", childPagesFrom);
				linkedChild.setField("childPageFrom_str_mv", mapChildPageFrom);
				
				Map<String, List<String>> mapChildPageTo = new HashMap<String, List<String>>();
				mapChildPageTo.put("set", childPagesTo);
				linkedChild.setField("childPageTo_str_mv", mapChildPageTo);
								
				Map<String, List<String>> mapChildLevel = new HashMap<String, List<String>>();
				mapChildLevel.put("set", childLevels);
				linkedChild.setField("childLevel_str_mv", mapChildLevel);
				
				Map<String, List<String>> mapChildLogId = new HashMap<String, List<String>>();
				mapChildLogId.put("set", childSortLogIds);
				linkedChild.setField("childLogId_str_mv", mapChildLogId);
				
				Map<String, List<String>> mapChildUrl = new HashMap<String, List<String>>();
				mapChildUrl.put("set", childUrls);
				linkedChild.setField("childUrl_str_mv", mapChildUrl);

				docsForAtomicUpdates.add(linkedChild);

				counter = counter + 1;

				// Add documents from the class variable which was set before to Solr
				if (counter % CHILD_INDEX_RATE == 0) { // Every n-th record, add documents to solr
					relationHelper.indexDocuments(docsForAtomicUpdates, solrServer);
					docsForAtomicUpdates.clear();
					docsForAtomicUpdates = null;
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				} else if (counter >= noOfParents) { // The remainding documents (if division with NO_OF_ROWS 
					relationHelper.indexDocuments(docsForAtomicUpdates, solrServer);
					docsForAtomicUpdates.clear();
					docsForAtomicUpdates = null;
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				}


				AkImporterHelper.print(this.print, "Linking childs to parent from unlinked childs. Processing record no " + counter  + " of " + noOfParents + "                \r");
			}
		}
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

		// Set rows to a very high number so that we really get all child records
		// This should not be the bottleneck concerning performance because no parent record will have
		// such a high number of child volumes
		querynonDeletedChilds.setRows(500000);

		// Define a query for getting all documents. We get the deleted records with a filter query because of performance (see below)
		querynonDeletedChilds.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that are child volumes
		// (because we need to get their parent records to be able to unlink these childs from there).
		querynonDeletedChilds.setFilterQueries("parentSYS_str_mv:"+parentSYS, "-deleted_str:Y", "-customField_txt_mv:ausgeschieden");

		// Set fields that should be given back from the query
		querynonDeletedChilds.setFields(
				"sysNo_txt",
				"acNo_txt",
				"title",
				"title_part_txt",
				"multiVolumeNo_str",
				"multiVolumeNoSort_str",
				"serialVolumeNo_str",
				"serialVolumeNoSort_str",
				"articleParentVolumeNo_str",
				"edition",
				"publishDate",
				"parentSYS_str_mv",
				"parentMultiAC_str",
				"parentSeriesAC_str_mv",
				"articleParentAC_str",
				"articleParentIssue_str",
				"pageFrom_str",
				"pageTo_str",
				"level_str",
				"sortNoLogId_str",
				"url"
				);


		try {
			nonDeletedChildRecords = solrServer.query(querynonDeletedChilds).getResults();			
		} catch (SolrServerException e) {
			nonDeletedChildRecords = null;
			e.printStackTrace();
		}

		return nonDeletedChildRecords;
	}

	
	/**
	 * Check if a specific child record belongs to a specific parent record.
	 * 
	 * @param parentSYSsOfChild		An array of Aleph SYS nos.
	 * @param parentSYS				An Aleph SYS no. to check against.
	 * @return						True if the child record belongs to the parent record.
	 */
	private boolean isChildOfParent(String[] parentSYSsOfChild, String parentSYS) {
		for(String parentSYSOfChild : parentSYSsOfChild){
			if(parentSYSOfChild.equals(parentSYS)) {
				return true;
			}
		}
		return false;
	}


}
