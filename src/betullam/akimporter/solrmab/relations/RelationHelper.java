/**
 * Helper class for linking childs to parents and vice versa.
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
package betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class RelationHelper {

	// General variables
	private HttpSolrServer solrServerBiblio;
	private HttpSolrServer solrServerAuth;
	private String timeStamp = null;
	private int NO_OF_ROWS = 500;

	/**
	 * Constructor for helper class for relating parent records and child records
	 * 
	 * @param solrServerBiblio	Solr Server we want to index to
	 * @param timeStamp		Timestamp of moment the import process started
	 */
	public RelationHelper(HttpSolrServer solrServerBiblio, HttpSolrServer solrServerAuth, String timeStamp) {
		this.solrServerBiblio = solrServerBiblio;
		this.solrServerAuth = solrServerAuth;
		this.timeStamp = timeStamp;
	}

	/**
	 * Getting all child records of the current index process.
	 *  
	 * @param isFirstPage	True if first page of Solr results
	 * @param lastDocId		Doc Id of the last processed Solr document
	 * @return				SolrDocumentList object
	 */
	public SolrDocumentList getCurrentlyIndexedChildRecords(boolean isFirstPage, String lastDocId) {

		// Set up variable
		SolrDocumentList queryResult = null;

		// New Solr query
		SolrQuery query = new SolrQuery();

		// Set no of rows
		query.setRows(NO_OF_ROWS);

		// Add sorting (more efficient for deep paging)
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that are child volumes
		// (because we need to get their parent records to be able to unlink these childs from there).
		if (this.timeStamp != null) {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("parentMultiAC_str:* || parentSeriesAC_str_mv:* || articleParentAC_str:*", "indexTimestamp_str:"+this.timeStamp, "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("parentMultiAC_str:* || parentSeriesAC_str_mv:* || articleParentAC_str:*", "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("parentMultiAC_str:* || parentSeriesAC_str_mv:* || articleParentAC_str:*", "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("parentMultiAC_str:* || parentSeriesAC_str_mv:* || articleParentAC_str:*", "id:[" + lastDocId + " TO *]");
			}

		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_txt", "parentSYS_str_mv", "parentMultiAC_str", "parentSeriesAC_str_mv", "articleParentAC_str");


		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;
	}


	/**
	 * Getting all currently indexed records that don't have child records.
	 * 
	 * @param isFirstPage	True if first page of Solr results
	 * @param lastDocId		Doc Id of the last processed Solr document
	 * @return				SolrDocumentList object
	 */
	public SolrDocumentList getCurrentlyIndexedRecordsWithNoChilds(boolean isFirstPage, String lastDocId) {

		// Set up variable
		SolrDocumentList queryResult = null;

		// New Solr query
		SolrQuery query = new SolrQuery();

		// Set no of rows
		query.setRows(NO_OF_ROWS);

		// Add sorting (more efficient for deep paging)
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that do not have child volumes
		if (this.timeStamp != null) {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("indexTimestamp_str:"+this.timeStamp, "-childSYS_str_mv:*", "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("indexTimestamp_str:"+this.timeStamp, "-childSYS_str_mv:*", "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("-childSYS_str_mv:*", "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("-childSYS_str_mv:*", "id:[" + lastDocId + " TO *]");
			}
		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_txt", "parentSYS_str_mv", "parentMultiAC_str", "parentSeriesAC_str_mv", "articleParentAC_str");


		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return queryResult;
	}




	/**
	 * Getting all records that contains authority data (a GND-No).
	 * 
	 * @param isFirstPage	True if first page of Solr results
	 * @param lastDocId		Doc Id of the last processed Solr document
	 * @return				SolrDocumentList object
	 */
	public SolrDocumentList getRecordsWithGnd(boolean isFirstPage, String lastDocId) {
		// Set up variable
		SolrDocumentList queryResult = null;

		// New Solr query
		SolrQuery query = new SolrQuery();

		// Set no of rows
		query.setRows(NO_OF_ROWS);
		
		// Add sorting (more efficient for deep paging)
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Filter all records that were indexed with the current import process
		if (this.timeStamp != null) {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str:*", "indexTimestamp_str:"+this.timeStamp, "id:*");	
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str:*", "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str:*", "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str:*", "id:[" + lastDocId + " TO *]");
			}
		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_txt", "author_GndNo_str", "author2_GndNo_str", "author_additional_GndNo_str_mv", "corporateAuthorGndNo_str", "corporateAuthor2GndNo_str_mv", "subjectGndNo_str");

		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return queryResult;
	}
	
	
	/**
	 * Getting an authority record (GND) by it's ID.
	 * 
	 * @param gndId		String representing an ID of an authority record.
	 * @return			SolrDocument containing the authority record.
	 */
	public SolrDocument getGndRecordById(String gndId) {
		SolrDocument gndRecord = null;
		
		SolrQuery queryGndRecord = new SolrQuery(); // New Solr query
		queryGndRecord.setQuery("id:\""+gndId+"\""); // Define a query
		queryGndRecord.setFields("id"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultList = this.solrServerAuth.query(queryGndRecord).getResults();
			gndRecord = (resultList.getNumFound() > 0 && resultList != null) ? resultList.get(0) : null;// Get GND record (there should only be one)
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		
		return gndRecord;
	}




	/**
	 * Getting type of child record (e. g.: multivolume, article) 
	 * 
	 * @param record	SolrDocument object
	 * @return			String describing the type of the child record
	 */
	public String getChildRecordType(SolrDocument record) {
		String childRecordType = null;

		if (record.getFieldValue("parentMultiAC_str") != null) {
			childRecordType = "multivolume";
		} else if (record.getFieldValue("parentSeriesAC_str_mv") != null) {
			childRecordType = "serialvolume";
		} else if (record.getFieldValue("articleParentAC_str") != null) {
			childRecordType = "article";
		} else {
			System.err.println("\nRecord: " + record.getFieldValue("id"));
			System.err.println("Returnd fields in solr query must include the fields \"parentMultiAC_str\",  \"parentSeriesAC_str_mv\" and \"articleParentAC_str\".");
			return null;
		}

		return childRecordType;
	}


	/**
	 * Deduplication of a list of AC numbers of parent records.
	 * Explanation:	If one parent record has multiple child records, each child record has the same AC no. of the parent record (e. g. in MAB field 010).
	 * 				If we get all 010 fields of these child records, we will have duplicate values of the 010 fields. Here, we will deduplicate them.
	 *  
	 * @param childDocumentList		A SolrDocumentList containing the child records as Solr documents
	 * @return						A deduplicated Set<String> of AC nos.
	 */
	public Set<String> getDedupParentAcsFromMultipleChilds(SolrDocumentList childDocumentList) {
		Set<String> parentAcs = null;
		if (childDocumentList != null && !childDocumentList.isEmpty()) {
			parentAcs = new HashSet<String>();
			for (SolrDocument child : childDocumentList) {
				Set<String> lstParentAcs = getDedupParentAcsFromSingleChild(child);
				if (lstParentAcs.size() > 0) {
					for (String parentAc : lstParentAcs) {
						parentAcs.add(parentAc);
					}
				}
			}
		}
		return parentAcs;
	}

	/**
	 * Helper method for getDedupParentAcsFromMultipleChilds.
	 * 
	 * @param childRecord	A SolrDocument representing a child record
	 * @return				A deduplicated Set<String> of AC nos.
	 */
	public Set<String> getDedupParentAcsFromSingleChild(SolrDocument childRecord) {
		Set<String> parentAcs = new HashSet<String>();


		if (childRecord != null) {
			String childRecordType = getChildRecordType(childRecord);
			if (childRecordType != null) {

				String[] parentMultiACs = (childRecord.getFieldValues("parentMultiAC_str") != null) ? childRecord.getFieldValues("parentMultiAC_str").toArray(new String[0]) : null;
				String[] parentSeriesACs = (childRecord.getFieldValues("parentSeriesAC_str_mv") != null) ? childRecord.getFieldValues("parentSeriesAC_str_mv").toArray(new String[0]) : null;
				String[] parentArticleACs = (childRecord.getFieldValues("articleParentAC_str") != null) ? childRecord.getFieldValues("articleParentAC_str").toArray(new String[0]) : null;

				if (parentMultiACs != null) {
					for (String parentMultiAc : parentMultiACs) {
						parentAcs.add(parentMultiAc);
					}
				}

				if (parentSeriesACs != null) {
					for (String parentSeriesAc : parentSeriesACs) {
						parentAcs.add(parentSeriesAc);
					}
				}

				if (parentArticleACs != null) {
					for (String parentArticleAc : parentArticleACs) {
						parentAcs.add(parentArticleAc);
					}
				}
			}
		}

		return parentAcs;
	}


	/**
	 * Deduplication of parent SYS nos. of a child record.
	 *  
	 * @param childRecord	A Solr document representing a child record.
	 * @return				A deduplicated Set<String> of SYS nos.
	 */
	public Set<String> getDedupParentSYSsFromSingleChild(SolrDocument childRecord) {
		Set<String> parentSYSs = new HashSet<String>();
		if (childRecord != null) {
			String childRecordType = getChildRecordType(childRecord);
			if (childRecordType != null) {
				String[] parentSYSsOfChild = (childRecord.getFieldValues("parentSYS_str_mv") != null) ? childRecord.getFieldValues("parentSYS_str_mv").toArray(new String[0]) : null;
				if (parentSYSsOfChild != null) {
					for (String parentSYSOfChild : parentSYSsOfChild) {
						parentSYSs.add(parentSYSOfChild);
					}
				}
			}
		}
		return parentSYSs;
	}


	/**
	 * Getting a parent record by its AC no.
	 * 
	 * @param parentAc	AC no. of record
	 * @return			SolrDocument representing the parent record
	 */
	public SolrDocument getParentRecord(String parentAc) {
		SolrDocument parentRecord = null;
		SolrQuery queryParent = new SolrQuery(); // New Solr query
		queryParent.setQuery("acNo_txt:\""+parentAc+"\""); // Define a query
		queryParent.setFields("id", "title"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultList = this.solrServerBiblio.query(queryParent).getResults();
			if (resultList.getNumFound() > 0 && resultList != null) {
				parentRecord = this.solrServerBiblio.query(queryParent).getResults().get(0);// Get parent document (there should only be one)
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return parentRecord;
	}

	/**
	 * Getting multiple parent records by their AC nos.
	 * @param parentAcs		A Set<String> containing AC nos. of the parent records
	 * @return				A list of SolrDocuments representing the parent records
	 */
	public List<SolrDocument> getParentRecords(Set<String> parentAcs) {
		List<SolrDocument> parentRecords = null;

		if (parentAcs != null) {
			parentRecords = new ArrayList<SolrDocument>();
			for (String parentAc : parentAcs) {
				SolrDocument sdParentRecord = this.getParentRecord(parentAc);
				if (sdParentRecord != null) {
					parentRecords.add(sdParentRecord);
				}
			}
		}

		return parentRecords;
	}

	/**
	 * Helper method for indexing documents to a Solr server.
	 * 
	 * @param docsForAtomicUpdates	A collection of SolrImputDocument objects.
	 */
	public void indexDocuments(Collection<SolrInputDocument> docsForAtomicUpdates, HttpSolrServer solrServer) {		
		if (!docsForAtomicUpdates.isEmpty()) {
			try {
				solrServer.add(docsForAtomicUpdates); // Add the collection of documents to Solr
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
			}
		}
	}


}
