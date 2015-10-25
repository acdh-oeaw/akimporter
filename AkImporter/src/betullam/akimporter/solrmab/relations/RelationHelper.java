/**
 * Helper class for linking childs to parents and vice versa.
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
	private HttpSolrServer solrServer;
	private String timeStamp = null;
	private int NO_OF_ROWS = 500;

	public RelationHelper(HttpSolrServer solrServer, String timeStamp) {
		this.solrServer = solrServer;
		this.timeStamp = timeStamp;
	}


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
			queryResult = this.solrServer.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;
	}


	
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
				query.setFilterQueries("id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("id:[" + lastDocId + " TO *]");
			}
		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_txt", "parentSYS_str_mv", "parentMultiAC_str", "parentSeriesAC_str_mv", "articleParentAC_str");


		try {
			// Execute query and get results
			queryResult = this.solrServer.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return queryResult;
	}



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



	public SolrDocument getParentRecord(String parentAc) {
		SolrDocument parentRecord = null;
		SolrQuery queryParent = new SolrQuery(); // New Solr query
		queryParent.setQuery("acNo_txt:"+parentAc); // Define a query
		queryParent.setFields("id", "title"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultList = this.solrServer.query(queryParent).getResults();
			if (resultList.getNumFound() > 0 && resultList != null) {
				parentRecord = this.solrServer.query(queryParent).getResults().get(0);// Get parent document (there should only be one)
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return parentRecord;
	}

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

	public void indexDocuments(Collection<SolrInputDocument> docsForAtomicUpdates) {		
		if (!docsForAtomicUpdates.isEmpty()) {
			try {
				this.solrServer.add(docsForAtomicUpdates); // Add the collection of documents to Solr
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
