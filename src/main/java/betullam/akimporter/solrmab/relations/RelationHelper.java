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
package main.java.betullam.akimporter.solrmab.relations;

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
	 * Getting all records of the current index process that have data about "other editions".
	 * 
	 * @param relationType	String: Type of relation to another record: "otherEdition", "attachment" or "attachementTo".
	 * @param isFirstPage	boolean: "true" if first page of Solr results
	 * @param lastDocId		String: Doc Id of the last processed Solr document
	 * @return				SolrDocumentList: List of Solr documents that have data about "other editions"
	 */
	public SolrDocumentList getCurrentlyIndexedRecordsWithGenericRelations(String relationType, boolean isFirstPage, String lastDocId) {

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

		// Define a filter query based on the type of relation
		String filterQuery = null;
		List<String> returnFieldsList = new ArrayList<String>();
		returnFieldsList.add("id");
		
		if (relationType.equals("otherEdition")) {
			filterQuery = "otherEdition_str_mv:*";
			returnFieldsList.add("otherEdition_str_mv");
		}
		if (relationType.equals("attachment")) {
			filterQuery = "attachment_str_mv:*";
			returnFieldsList.add("attachment_str_mv");
		}
		if (relationType.equals("attachementTo")) {
			filterQuery = "attachmentTo_str_mv:*";
			returnFieldsList.add("attachmentTo_str_mv");
		}
		if (relationType.equals("predecessor")) {
			filterQuery = "predecessor_str_mv:*";
			returnFieldsList.add("predecessor_str_mv");
		}
		if (relationType.equals("successor")) {
			filterQuery = "successor_str_mv:*";
			returnFieldsList.add("successor_str_mv");
		}
		if (relationType.equals("otherRelation")) {
			filterQuery = "otherRelation_txt_mv:*";
			returnFieldsList.add("otherRelation_txt_mv");
		}
			
			
		// Filter all records that were indexed with the current import process and that contain generic relations to other records
		if (this.timeStamp != null) {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(filterQuery, "indexTimestamp_str:"+this.timeStamp, "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(filterQuery, "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(filterQuery, "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(filterQuery, "id:[" + lastDocId + " TO *]");
			}

		}

		// Set fields that should be given back from the query
		query.setFields(returnFieldsList.toArray(new String[0]));

		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;
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
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str_mv:*", "indexTimestamp_str:"+this.timeStamp, "id:*");	
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str_mv:*", "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str_mv:*", "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("author_GndNo_str:* || author2_GndNo_str:* || author_additional_GndNo_str_mv:* || corporateAuthorGndNo_str:* || corporateAuthor2GndNo_str_mv:* || subjectGndNo_str_mv:*", "id:[" + lastDocId + " TO *]");
			}
		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_txt", "author_GndNo_str", "author2_GndNo_str", "author_additional_GndNo_str_mv", "corporateAuthorGndNo_str", "corporateAuthor2GndNo_str_mv", "subjectGndNo_str_mv");

		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return queryResult;
	}


	/**
	 * Getting all records that contains authority data (a GND-No) in the given Solr fields.
	 * 
	 * @param solrFields	List<String> of Solr fields that must be present in the document for that it is included to the query result.
	 * @param isFirstPage	True if first page of Solr results
	 * @param lastDocId		Doc Id of the last processed Solr document
	 * @return				SolrDocumentList object
	 */
	public SolrDocumentList getRecordsWithGndByFields(List<String> solrFields, boolean isFirstPage, String lastDocId) {
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


		// Create query string:
		String queryString = "";
		for (String solrField : solrFields) {
			queryString += solrField + ":* || "; // Join fields with the "OR" query operator
		}
		queryString = queryString.trim();
		queryString = queryString.replaceFirst("(\\|\\|)$", "").trim(); // Remove the last "OR" query operator


		// Filter all records that were indexed with the current import process
		if (this.timeStamp != null) {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(queryString, "indexTimestamp_str:"+this.timeStamp, "id:*");	
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(queryString, "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(queryString, "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(queryString, "id:[" + lastDocId + " TO *]");
			}
		}

		// Set fields that should be given back from the query
		List<String> solrFieldsToReturn = new ArrayList<>(solrFields); // Copy immutable List<String> to a mutable List<String>
		solrFieldsToReturn.add("id");
		String[] returnSolrFields = solrFieldsToReturn.toArray(new String[0]);
		query.setFields(returnSolrFields);

		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return queryResult;
	}



	/**
	 * Get bibliographich records that contains at least one of the given authority IDs in at least
	 * one of the given Solr fields.
	 * @param authIds		Set<String>: Authority IDs that the bibliographic record must contain
	 * @param solrFields	List<String>: Solr filds to search for the authority IDs
	 * @param isFirstPage	boolean: True if first page of Solr results
	 * @param lastDocId		String: Doc Id of the last processed Solr document
	 * @return				SolrDocumentList: Query result of Solr.
	 */
	public SolrDocumentList getRecordsByGndIdsAndFields(Set<String> authIds, List<String> solrFields, boolean isFirstPage, String lastDocId) {

		// Set variables
		SolrDocumentList biblioRecords = new SolrDocumentList();
		SolrQuery query = new SolrQuery();

		// Set no of rows
		query.setRows(NO_OF_ROWS);

		// Add sorting
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance (filter query users filter cache)
		query.setQuery("*:*");


		// Create query string:
		String queryString = "";
		for (String solrField : solrFields) {
			for (String authId : authIds) {
				queryString += solrField + ":\""+authId+"\" || "; // Join fields with the "OR" query operator
			}
		}
		queryString = queryString.trim();
		queryString = queryString.replaceFirst("(\\|\\|)$", "").trim(); // Remove the last "OR" query operator


		// Set filter query
		// We do not work with timeStamp here because we will need all bibliographich records that are using
		// the specified authority ID, no matter when they were indexed. 
		if (isFirstPage) { // No range filter on first page
			query.setFilterQueries(queryString, "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			query.setStart(1);
			query.setFilterQueries(queryString, "id:[" + lastDocId + " TO *]");
		}

		
		// Set fields that should be given back from the query
		List<String> lstSolrFieldsToReturn = new ArrayList<>(solrFields); // Copy immutable List<String> to a mutable List<String>
		lstSolrFieldsToReturn.add("id");
		String[] arrSolrFieldsToReturn = lstSolrFieldsToReturn.toArray(new String[0]);
		query.setFields(arrSolrFieldsToReturn);

		
		try {
			// Execute query and get results
			SolrDocumentList queryResult = this.solrServerBiblio.query(query).getResults();
			if (queryResult != null && !queryResult.isEmpty()) {
				biblioRecords.addAll(queryResult);
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return biblioRecords;
	}

	/**
	 * Get bibliographic records that contains at least one of the given authority IDs.
	 * @param authIds	Set<String>: Authority IDs that the bibliographic record must contain
	 * @return			SolrDocumentList: Query result of Solr.
	 */
	public SolrDocumentList getRecordsByGndIds(Set<String> authIds) {

		// Set variables
		SolrDocumentList biblioRecords = new SolrDocumentList();
		SolrQuery query = new SolrQuery();

		// Set no of rows
		query.setRows(NO_OF_ROWS);

		// Add sorting
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance (filter query users filter cache)
		query.setQuery("*:*");

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_txt", "author_GndNo_str", "author2_GndNo_str", "author_additional_GndNo_str_mv", "corporateAuthorGndNo_str", "corporateAuthor2GndNo_str_mv", "subjectGndNo_str_mv");

		for (String authId : authIds) {
			query.setFilterQueries("author_GndNo_str:\""+authId+"\" || author2_GndNo_str:\""+authId+"\" || author_additional_GndNo_str_mv:\""+authId+"\" || corporateAuthorGndNo_str:\""+authId+"\" || corporateAuthor2GndNo_str_mv:\""+authId+"\" || subjectGndNo_str_mv:\""+authId+"\"", "id:*");	
			try {
				// Execute query and get results
				SolrDocumentList queryResult = this.solrServerBiblio.query(query).getResults();
				if (queryResult != null && !queryResult.isEmpty()) {
					biblioRecords.addAll(queryResult);
				}
			} catch (SolrServerException e) {
				e.printStackTrace();
			}
		}

		return biblioRecords;
	}


	/**
	 * Getting a GND authority record by an ID that is used in it's MAB field 001
	 * 
	 * @param gndId		String representing an ID of a GND record
	 * @return			SolrDocument containing the query result with the GND record
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
	 * Getting GND authority records by an ID that is used in it's MAB fields 001 or 035
	 * 
	 * @param gndId		String representing an ID of a GND record
	 * @return			SolrDocumentList containing the query result with GND records
	 */
	public SolrDocumentList getGndRecordsByIdAnd035(String gndId) {
		SolrDocumentList gndRecords = null;

		SolrQuery queryGndRecord = new SolrQuery(); // New Solr query
		queryGndRecord.setQuery("id:\""+gndId+"\" || gndId035_str_mv:\""+gndId+"\""); // Define a query
		queryGndRecord.setFields("id"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultList = this.solrServerAuth.query(queryGndRecord).getResults();
			gndRecords = (resultList.getNumFound() > 0 && resultList != null) ? resultList : null; // Get GND records
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return gndRecords;
	}



	/**
	 * Getting all authority records of a given entity (e. g. Person) and with the flag of existance
	 * @param entity		Type of authority entity, e. g. Person
	 * @param isFirstPage	True if first page of Solr results
	 * @param lastDocId		Doc Id of the last processed Solr document
	 * @return				SolrDocumentList
	 */
	public SolrDocumentList getAuthorityRecordsByEntity(String entity, boolean isFirstPage, String lastDocId) {

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
				query.setFilterQueries("entity_str:\""+entity+"\"", "existsInBiblio_str:true", "indexTimestamp_str:"+this.timeStamp, "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("entity_str:\""+entity+"\"", "existsInBiblio_str:true", "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries("entity_str:\""+entity+"\"", "existsInBiblio_str:true", "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries("entity_str:\""+entity+"\"", "existsInBiblio_str:true", "id:[" + lastDocId + " TO *]");
			}
		}


		// Set fields that should be given back from the query
		query.setFields("id", "gndId035_str_mv", "heading", "heading_additions_txt_mv", "use_for", "use_for_additions_txt_mv", "other_additions_txt_mv");

		try {
			// Execute query and get results
			queryResult = this.solrServerAuth.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;
	}



	/**
	 * Get all possible IDs of the currently indexed authority records
	 * @return	Set<String> of all possible IDs
	 */
	public Set<String> getIdsAnd035OfCurrentlyIndexedAuthRecords() {

		// Set up variables
		Set<String> distinctAuthIds = new HashSet<String>();
		SolrDocumentList currentlyIndexedAuthRecords = null;
		SolrQuery query = new SolrQuery();

		// Set no of rows
		// The query should not give back a lot of records, so we use Integer.MAX_VALUE. But in the future we should use a better strategy.
		// TODO: Change to deep pageing query in case we get back too much documents (performance).
		query.setRows(Integer.MAX_VALUE);

		// Add sorting
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Filter all records that were indexed with the current import process (timeStamp)
		query.setFilterQueries("indexTimestamp_str:"+this.timeStamp, "id:*");

		// Set fields that should be given back from the query
		query.setFields("id", "gndId035_str_mv");

		try {
			// Execute query and get results
			currentlyIndexedAuthRecords = this.solrServerAuth.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		if (!currentlyIndexedAuthRecords.isEmpty() && currentlyIndexedAuthRecords != null) {
			for (SolrDocument authRecord : currentlyIndexedAuthRecords) {
				// Get all IDs of the authority document
				String authId = authRecord.getFieldValue("id").toString();
				Collection<Object> gndIds035 = (authRecord.getFieldValues("gndId035_str_mv") != null && !authRecord.getFieldValues("gndId035_str_mv").isEmpty()) ? authRecord.getFieldValues("gndId035_str_mv") : null;

				// Add authority IDs to a Set<String> to get a deduplicated list of authority IDs 
				distinctAuthIds.add(authId);
				if (gndIds035 != null) {
					for (Object gndId035 : gndIds035) {
						distinctAuthIds.add(gndId035.toString());
					}
				}
			}
		}

		return distinctAuthIds;
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
			System.err.println("\nReturned fields in solr query must include the fields \"parentMultiAC_str\",  \"parentSeriesAC_str_mv\" and \"articleParentAC_str\".");
			System.err.println("Problematic record: " + record.toString());
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
		queryParent.setFilterQueries("-deleted_str:Y"); // Do not use deleted records. We could get problems when doubled records exist with the same AC number and one is deleted, the other one not. This could lead to linking to the deleted record (other SYS-No, same AC-No)

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
	 * @param solrServer			A HttpSolrServer object of the server where the documents should be indexed
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
