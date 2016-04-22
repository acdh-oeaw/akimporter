/**
 * Integrates authority data into bibliographic data for better
 * search results.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class AuthorityIntegrate {

	private RelationHelper relationHelper;
	private SolrMabHelper smHelper = new SolrMabHelper();
	private HttpSolrServer solrServerBiblio;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private boolean print = false;
	private int NO_OF_ROWS = 500;
	private int INDEX_RATE = 500;
	private String timeStamp = null;

	/**
	 * Constructor for setting some variables.
	 * 
	 * @param solrServerBiblio		HttpSolrServer object of the Solr server and core which holds the bibliographic data
	 * @param solrServerAuthority	HttpSolrServer object of the Solr server and core which holds the authority data
	 * @param timeStamp				String: timestamp of integration time
	 * @param print					boolean that indicates if status messages should be print
	 */
	public AuthorityIntegrate(HttpSolrServer solrServerBiblio, HttpSolrServer solrServerAuthority, String timeStamp, boolean print) {
		this.solrServerBiblio = solrServerBiblio;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServerBiblio, solrServerAuthority, timeStamp);
	}


	/**
	 * Start integration process for a specific authority entity (e. g. Person, Congress, Corporation, etc.)
	 * 
	 * @param entity	String indicating the authority entity to integrate (e. g. Person, Congress, Corporation, etc.)
	 */
	public void integrateAuthorityRecords(String entity) {
		String[] entities = entity.split(",");

		for (String ent : entities) {

			// Getting fields in which to serach the GND ID numbers for an entity (e. g. for Person in author_GndNo_str and author2_GndNo_str)
			String strEntFields = null;
			Pattern pattern = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between square brackets and the brackets themselve (we will remove them later)
			Matcher matcher = pattern.matcher(ent);
			strEntFields = (matcher.find()) ? matcher.group().replaceFirst("\\[", "").replaceFirst("\\]$", "").trim() : null;
			List<String> currentEntitySolrFields =  Arrays.asList(strEntFields.split(":"));
			ent = ent.replaceAll("\\[.*?\\]", "");

			docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			String currentEntity = ent.trim();
			SolrDocumentList queryResults = this.relationHelper.getAuthorityRecords(currentEntity, true, null);

			// Get the number of documents that were found
			long noOfDocs = queryResults.getNumFound();

			// If there are some records, go on. If not, do nothing.
			if (queryResults != null && noOfDocs > 0) {
				// Clear query results. We don't need them anymore.
				queryResults.clear();
				queryResults = null;

				this.smHelper.print(this.print, "\nGetting " + noOfDocs + " relevant " + ent + " authority records ... \n");

				// Calculate the number of solr result pages we need to iterate over
				long wholePages = (noOfDocs/NO_OF_ROWS);
				long fractionPages = (noOfDocs%NO_OF_ROWS);

				// Variable for lastDocId
				String lastDocId = null;

				for (long l = 0; l < wholePages; l++) {
					boolean isFirstPage = (l == 0) ? true : false;

					SolrDocumentList authorityRecords = this.relationHelper.getAuthorityRecords(currentEntity, isFirstPage, lastDocId);

					// Integrate the data of the authority records to the bibliographic records				
					lastDocId = addAuthInfoToBiblio(authorityRecords, currentEntity, currentEntitySolrFields);
				}

				// Add documents on the last page:
				if (fractionPages != 0) {
					boolean isFirstPage = (wholePages <= 0) ? true : false;

					SolrDocumentList authorityRecords = this.relationHelper.getAuthorityRecords(currentEntity, isFirstPage, lastDocId);

					// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
					addAuthInfoToBiblio(authorityRecords, currentEntity, currentEntitySolrFields);
				}

				this.smHelper.print(this.print, "\nDone\n");

				// Commit the changes
				try {
					this.solrServerBiblio.commit();
				} catch (SolrServerException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					this.docsForAtomicUpdates.clear();
					this.docsForAtomicUpdates = null;
					queryResults = null;
				}

			}
		}
	}


	/**
	 * This function actually gets the bibliographic records to which the authority data should be integrated
	 * and sets the atomic update record for Solr.
	 * 
	 * @param authorityRecords	SolrDocumentList of authority records of a given entity
	 * @param entity			String indicating the authority entity to integrate (e. g. Person, Congress, Corporation, etc.)
	 * @return					String: doc ID of the last processed Solr document
	 */
	private String addAuthInfoToBiblio(SolrDocumentList authorityRecords, String entity, List<String> currentEntitySolrFields) {
		// Variable for return value:
		String returnValue = null;
		int noOfAuthRecords = authorityRecords.size();
		int counter = 0;
		long noOfFoundAuthRecords = authorityRecords.getNumFound();


		if (noOfAuthRecords > 0) {

			// Setting field names according to entity
			String fieldNameHeading = "authHeading"+entity+"_txt_mv";
			String fieldNameHeadingAdditions = "authHeadingAdditions"+entity+"_txt_mv";
			String fieldNameUseFor = "authUseFor"+entity+"_txt_mv";
			String fieldNameUseForAdditions = "authUseForAdditions"+entity+"_txt_mv";
			String fieldNameOtherAdditions = "authOtherAdditions"+entity+"_txt_mv";

			// Array with the fieldNames for setFields in Solr query
			String[] fieldsToReturnFromBiblio = {"id", fieldNameHeading, fieldNameHeadingAdditions, fieldNameUseFor, fieldNameUseForAdditions, fieldNameOtherAdditions};

			String newLastDocId = authorityRecords.get(authorityRecords.size()-1).getFieldValue("id").toString();
			for(SolrDocument authorityRecord : authorityRecords) {
				counter = counter + 1;

				String authId = authorityRecord.getFieldValue("id").toString();
				Collection<Object> gndId035 = authorityRecord.getFieldValues("gndId035_str_mv");
				String authHeading = authorityRecord.getFieldValue("heading").toString();
				Collection<Object> authHeadingAdditions = (authorityRecord.getFieldValues("heading_additions_txt_mv") != null) ? authorityRecord.getFieldValues("heading_additions_txt_mv") : new ArrayList<Object>();
				Collection<Object> authUseFor = (authorityRecord.getFieldValues("use_for") != null) ? authorityRecord.getFieldValues("use_for") : new ArrayList<Object>();
				Collection<Object> authUseForAdditions = (authorityRecord.getFieldValues("use_for_additions_txt_mv") != null) ? authorityRecord.getFieldValues("use_for_additions_txt_mv") : new ArrayList<Object>();
				Collection<Object> authOtherAdditions = (authorityRecord.getFieldValues("other_additions_txt_mv") != null) ? authorityRecord.getFieldValues("other_additions_txt_mv") : new ArrayList<Object>();


				SolrDocumentList bibRecordsForIntegration = getBiblioRecordsByAuthId(authId, gndId035, fieldsToReturnFromBiblio, currentEntitySolrFields);

				for(SolrDocument bibRecord : bibRecordsForIntegration) {
					// Get values from bibliographic record
					//Collection<Object> bibHeadings = (bibRecord.getFieldValues(fieldNameHeading) != null) ? bibRecord.getFieldValues(fieldNameHeading) : new ArrayList<Object>();
					//Collection<Object> bibHeadingAdditions = (bibRecord.getFieldValues(fieldNameHeadingAdditions) != null) ? bibRecord.getFieldValues(fieldNameHeadingAdditions) : new ArrayList<Object>();
					//Collection<Object> bibUseFor = (bibRecord.getFieldValues(fieldNameUseFor) != null) ? bibRecord.getFieldValues(fieldNameUseFor) : new ArrayList<Object>();
					//Collection<Object> bibUseForAdditions = (bibRecord.getFieldValues(fieldNameUseForAdditions) != null) ? bibRecord.getFieldValues(fieldNameUseForAdditions) : new ArrayList<Object>();
					//Collection<Object> bibOtherAdditions = (bibRecord.getFieldValues(fieldNameOtherAdditions) != null) ? bibRecord.getFieldValues(fieldNameOtherAdditions) : new ArrayList<Object>();

					// SolrInputDocument for atomic update:
					SolrInputDocument bibRecordAtomic = null;
					
					// Prepare bibliographic record for atomic update:
					String bibId = bibRecord.getFieldValue("id").toString();
					bibRecordAtomic = new SolrInputDocument();
					bibRecordAtomic.setField("id", bibId);

					// Heading
					Map<String, String> headingField = new HashMap<String, String>();
					headingField.put("add", authHeading);
					bibRecordAtomic.setField(fieldNameHeading, headingField);

					// Heading additions
					Map<String, Collection<Object>> headingAdditionsField = new HashMap<String, Collection<Object>>();
					headingAdditionsField.put("add", authHeadingAdditions);
					bibRecordAtomic.setField(fieldNameHeadingAdditions, headingAdditionsField);
					
					// Use-for
					Map<String, Collection<Object>> useForField = new HashMap<String, Collection<Object>>();
					useForField.put("add", authUseFor);
					bibRecordAtomic.setField(fieldNameUseFor, useForField);

					// Use-for additions
					Map<String, Collection<Object>> useForAdditionsField = new HashMap<String, Collection<Object>>();
					useForAdditionsField.put("add", authUseForAdditions);
					bibRecordAtomic.setField(fieldNameUseForAdditions, useForAdditionsField);

					// Other additions
					Map<String, Collection<Object>> otherAdditionsField = new HashMap<String, Collection<Object>>();
					otherAdditionsField.put("add", authOtherAdditions);
					bibRecordAtomic.setField(fieldNameOtherAdditions, otherAdditionsField);
					
					// Add record for atomic update only if there is something to index. This is to avoid overhead.
					if (!authHeading.isEmpty() || !authHeadingAdditions.isEmpty() || !authUseFor.isEmpty() || !authUseForAdditions.isEmpty() || !authOtherAdditions.isEmpty()) {
						this.docsForAtomicUpdates.add(bibRecordAtomic);
					}
					
					/*
					// Heading
					if (!bibHeadings.contains(authHeading)) {bibHeadings.add(authHeading);} // Add heading only if it does not already exist. This is to avoid duplicates.
					Map<String, Collection<Object>> headingField = new HashMap<String, Collection<Object>>();
					headingField.put("add", bibHeadings);
					bibRecordAtomic.setField(fieldNameHeading, headingField);

					// Heading additions
					authHeadingAdditions.removeAll(bibHeadingAdditions); // Remove field from authority values that already exists in biblio values (getting distinct values)
					bibHeadingAdditions.addAll(authHeadingAdditions); // Add distinct authority values to biblio values 
					Map<String, Collection<Object>> headingAdditionsField = new HashMap<String, Collection<Object>>();
					headingAdditionsField.put("add", bibHeadingAdditions);
					bibRecordAtomic.setField(fieldNameHeadingAdditions, headingAdditionsField);
					
					// Use-for
					authUseFor.removeAll(bibUseFor); // Remove field from authority values that already exists in biblio values (getting distinct values)
					bibUseFor.addAll(authUseFor); // Add distinct authority values to biblio values 
					Map<String, Collection<Object>> useForField = new HashMap<String, Collection<Object>>();
					useForField.put("add", bibUseFor);
					bibRecordAtomic.setField(fieldNameUseFor, useForField);

					// Use-for additions
					authUseForAdditions.removeAll(bibUseForAdditions); // Remove field from authority values that already exists in biblio values (getting distinct values)
					bibUseForAdditions.addAll(authUseForAdditions); // Add distinct authority values to biblio values 
					Map<String, Collection<Object>> useForAdditionsField = new HashMap<String, Collection<Object>>();
					useForAdditionsField.put("add", bibUseForAdditions);
					bibRecordAtomic.setField(fieldNameUseForAdditions, useForAdditionsField);

					// Other additions
					authOtherAdditions.removeAll(bibOtherAdditions); // Remove field from authority values that already exists in biblio values (getting distinct values)
					bibOtherAdditions.addAll(authOtherAdditions); // Add distinct authority values to biblio values 
					Map<String, Collection<Object>> otherAdditionsField = new HashMap<String, Collection<Object>>();
					otherAdditionsField.put("add", bibOtherAdditions);
					bibRecordAtomic.setField(fieldNameOtherAdditions, otherAdditionsField);
					
					// Add record for atomic update only if there is something to index. This is to avoid overhead.
					if (!bibHeadings.isEmpty() || !bibHeadingAdditions.isEmpty() || !bibUseFor.isEmpty() || !bibUseForAdditions.isEmpty() || !bibOtherAdditions.isEmpty()) {
						this.docsForAtomicUpdates.add(bibRecordAtomic);
					}
					*/
					
					this.smHelper.print(this.print, "Integrating authority data to bibliographic record " + bibId + ". Records to process: " + noOfFoundAuthRecords + "                                   \r");

					


					// Add documents from the class variable which was set before to Solr
					if (counter % INDEX_RATE == 0) { // Every n-th record, add documents to solr
						this.relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);
						this.docsForAtomicUpdates.clear();
						this.docsForAtomicUpdates = null;
						this.docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument

					} else if (counter >= noOfAuthRecords) { // The remaining documents (if division with NO_OF_ROWS)
						this.relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);
						this.docsForAtomicUpdates.clear();
						this.docsForAtomicUpdates = null;
						this.docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
					}
				}

				// If the last document of the solr result page is reached, build a new query so that we can iterate over the next result page:
				if (authId.equals(newLastDocId)) {
					returnValue = authId;
				}

			}
		}


		return returnValue;
	}



	/**
	 * Gets all bibliographic records containing a given authority ID.
	 * 
	 * @param authId	String indicating the authority ID to search for in the bibliographic records
	 * @return			SolrDocumentList with bibliographic records containing the given authority ID
	 */
	private SolrDocumentList getBiblioRecordsByAuthId(String authId, Collection<Object> gndIds035, String[] fieldsToReturn, List<String> currentEntitySolrFields) {
		// Set up variable
		SolrDocumentList queryResult = null;

		// New Solr query
		SolrQuery query = new SolrQuery();

		// Set no of rows
		// Setting to a high value. Ther should never be such a high number of results
		// that we could get into trouble. TODO: But as a security measure, an appropriate
		// deep paging function could be introduced at a later date.
		query.setRows(50000);

		// Add sorting
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Create query string:
		String queryString = "";
		for (String currentEntitySolrField : currentEntitySolrFields) {
			queryString += currentEntitySolrField + ":\""+authId+"\" || "; // Join fields with the "OR" query operator
		}

		for (String currentEntitySolrField : currentEntitySolrFields) {
			for (Object gndId035Obj : gndIds035) {
				if (gndId035Obj != null) {
					String gndId035 = gndId035Obj.toString();
					queryString += currentEntitySolrField + ":\""+gndId035+"\" || "; // Join fields with the "OR" query operator;
				}
			}
		}

		queryString = queryString.trim();
		queryString = queryString.replaceFirst("(\\|\\|)$", "").trim(); // Remove the last "OR" query operator

		if (this.timeStamp != null) {
			query.setFilterQueries(queryString.trim(), "indexTimestamp_str:"+this.timeStamp, "id:*");
		} else {
			query.setFilterQueries(queryString.trim(), "id:*");
		}
		
		// Set fields that should be given back from the query
		query.setFields(fieldsToReturn);

		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;


	}
}