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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class AuthorityMerge {


	private RelationHelper relationHelper;
	private SolrMabHelper smHelper = new SolrMabHelper();
	private HttpSolrServer solrServerBiblio;
	private HttpSolrServer solrServerAuth;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private boolean print = false;
	private int NO_OF_ROWS = 500;
	private int INDEX_RATE = 500;

	/**
	 * Constructor for setting some variables.
	 * 
	 * @param solrServerBiblio		HttpSolrServer object of the Solr server and core which holds the bibliographic data
	 * @param solrServerAuthority	HttpSolrServer object of the Solr server and core which holds the authority data
	 * @param timeStamp				String: timestamp of integration time
	 * @param print					boolean that indicates if status messages should be print
	 */
	public AuthorityMerge(HttpSolrServer solrServerBiblio, HttpSolrServer solrServerAuthority, String timeStamp, boolean print) {
		this.solrServerBiblio = solrServerBiblio;
		this.solrServerAuth = solrServerAuthority;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServerBiblio, solrServerAuthority, timeStamp);
	}


	/**
	 * Start integration process for a specific authority entity (e. g. Person, Congress, Corporation, etc.)
	 * 
	 * @param entity	String indicating the authority entity to integrate (e. g. Person, Congress, Corporation, etc.)
	 */
	public void mergeAuthorityToBiblio(String entity) {
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
			
			// Get bibliographic records that uses authority IDs
			SolrDocumentList queryResults = this.relationHelper.getRecordsWithGndByFields(currentEntitySolrFields, true, null);

			// Get the number of documents that were found
			long noOfDocs = queryResults.getNumFound();

			// If there are some records, go on. If not, do nothing.
			if (queryResults != null && noOfDocs > 0) {
				// Clear query results. We don't need them anymore.
				queryResults.clear();
				queryResults = null;

				this.smHelper.print(this.print, "\nGetting " + noOfDocs + " records with " + ent + " GND IDs ... \n");

				// Calculate the number of solr result pages we need to iterate over
				long wholePages = (noOfDocs/NO_OF_ROWS);
				long fractionPages = (noOfDocs%NO_OF_ROWS);

				// Variable for lastDocId
				String lastDocId = null;

				for (long l = 0; l < wholePages; l++) {
					boolean isFirstPage = (l == 0) ? true : false;

					// Get bibliographic records that uses authority IDs
					SolrDocumentList biblioRecords = this.relationHelper.getRecordsWithGndByFields(currentEntitySolrFields, isFirstPage, lastDocId);

					// Integrate the data of the authority records to the bibliographic records				
					lastDocId = addAuthInfoToBiblio(biblioRecords, currentEntity, currentEntitySolrFields);
				}

				// Add documents on the last page:
				if (fractionPages != 0) {
					boolean isFirstPage = (wholePages <= 0) ? true : false;

					// Get bibliographic records that uses authority IDs
					SolrDocumentList biblioRecords = this.relationHelper.getRecordsWithGndByFields(currentEntitySolrFields, isFirstPage, lastDocId);

					// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
					addAuthInfoToBiblio(biblioRecords, currentEntity, currentEntitySolrFields);
				}

				this.smHelper.print(this.print, "\nDone");

				try {
					// Commit the changes to Solr
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
	 * This function actually iterates over bibliographic records, gets the authority ID that is used in them and, based on these IDs,
	 * queries the authority data that should be integrated to them. Then it sets the atomic update record for Solr.
	 * 
	 * @param biblioRecords				SolrDocumentList of bibliographic records to which the authority data should be integrated
	 * @param entity					String indicating the authority entity to integrate (e. g. Person, Congress, Corporation, etc.)
	 * @param currentEntitySolrFields	List<String> of the Solr fields that should be queried
	 * @return							String: doc ID of the last processed Solr document
	 */
	private String addAuthInfoToBiblio(SolrDocumentList biblioRecords, String entity, List<String> currentEntitySolrFields) {
		// Variable for return value
		String returnValue = null;

		// Variables for counting
		int noOfBibRecords = biblioRecords.size();
		int counter = 0;
		long noOfFoundBibRecords = biblioRecords.getNumFound();


		if (noOfBibRecords > 0) {

			// Setting field names according to entity
			String fieldNameHeading = "authHeading"+entity+"_txt_mv";
			String fieldNameHeadingAdditions = "authHeadingAdditions"+entity+"_txt_mv";
			String fieldNameUseFor = "authUseFor"+entity+"_txt_mv";
			String fieldNameUseForAdditions = "authUseForAdditions"+entity+"_txt_mv";
			String fieldNameOtherAdditions = "authOtherAdditions"+entity+"_txt_mv";

			String newLastDocId = biblioRecords.get(biblioRecords.size()-1).getFieldValue("id").toString();

			for(SolrDocument biblioRecord : biblioRecords) {
				String recordId = biblioRecord.getFieldValue("id").toString();
				counter = counter + 1;
				
				// Get fieldnames of fields with GND IDs
				Collection<String> fieldNames = biblioRecord.getFieldNames();

				// Set all GND-IDs of the bibliographic record to a Set<String>
				Set<String> recordGndIds = new HashSet<String>();
				for (String fieldName : fieldNames) {
					if (!fieldName.equals("id")) {
						Collection<Object> gndIds = biblioRecord.getFieldValues(fieldName);
						for (Object gndId : gndIds) {
							if (gndId != null) {
								recordGndIds.add(gndId.toString());
							}
						}
					}
				}

				// Get all authority information for the given GND IDs that the bibliographic record contains
				SolrDocumentList authRecordsForIntegration = getGndRecords(entity, recordGndIds);

				if (authRecordsForIntegration != null && !authRecordsForIntegration.isEmpty()) {
					
					Set<String> headings = new HashSet<String>();
					Set<String> headingsAdditions = new HashSet<String>();
					Set<String> useFors = new HashSet<String>();
					Set<String> useForsAdditions = new HashSet<String>();
					Set<String> othersAdditions = new HashSet<String>();

					// Get information of each authority record and add it to a Set<String> to avoid duplicates
					for(SolrDocument authRecord : authRecordsForIntegration) {

						String authHeading = (authRecord.getFieldValue("heading") != null) ? authRecord.getFieldValue("heading").toString() : null;
						Collection<Object> authHeadingAdditions = (authRecord.getFieldValues("heading_additions_txt_mv") != null) ? authRecord.getFieldValues("heading_additions_txt_mv") : null;
						Collection<Object> authUseFors = (authRecord.getFieldValues("use_for") != null) ? authRecord.getFieldValues("use_for") : null;
						Collection<Object> authUseForAdditions = (authRecord.getFieldValues("use_for_additions_txt_mv") != null) ? authRecord.getFieldValues("use_for_additions_txt_mv") : null;
						Collection<Object> authOtherAdditions= (authRecord.getFieldValues("other_additions_txt_mv") != null) ? authRecord.getFieldValues("other_additions_txt_mv") : null;

						if (authHeading != null) {
							headings.add(authHeading);
						}
						if (authHeadingAdditions != null && !authHeadingAdditions.isEmpty()) {
							for (Object authHeadingAddition : authHeadingAdditions) {
								headingsAdditions.add(authHeadingAddition.toString());
							}
						}
						if (authUseFors != null && !authUseFors.isEmpty()) {
							for (Object authUseFor : authUseFors) {
								useFors.add(authUseFor.toString());
							}
						}
						if (authUseForAdditions != null && !authUseForAdditions.isEmpty()) {
							for (Object authUseForAddition : authUseForAdditions) {
								useForsAdditions.add(authUseForAddition.toString());
							}
						}
						if (authOtherAdditions != null && !authOtherAdditions.isEmpty()) {
							for (Object authOtherAddition : authOtherAdditions) {
								othersAdditions.add(authOtherAddition.toString());
							}
						}					
					}

					// SolrInputDocument for atomic update:
					SolrInputDocument bibRecordAtomic = null;

					// Prepare bibliographic record for atomic update:
					bibRecordAtomic = new SolrInputDocument();
					bibRecordAtomic.setField("id", recordId);

					// Headings
					if (!headings.isEmpty()) {
						Map<String, Set<String>> headingField = new HashMap<String, Set<String>>();
						headingField.put("set", headings);
						bibRecordAtomic.setField(fieldNameHeading, headingField);
					}

					// Heading additions
					if (!headingsAdditions.isEmpty()) {
						Map<String, Set<String>> headingAdditionsField = new HashMap<String, Set<String>>();
						headingAdditionsField.put("set", headingsAdditions);
						bibRecordAtomic.setField(fieldNameHeadingAdditions, headingAdditionsField);
					}

					// Use-for
					if (!useFors.isEmpty()) {
						Map<String, Set<String>> useForField = new HashMap<String, Set<String>>();
						useForField.put("set", useFors);
						bibRecordAtomic.setField(fieldNameUseFor, useForField);
					}

					// Use-for additions
					if (!useForsAdditions.isEmpty()) {
						Map<String, Set<String>> useForAdditionsField = new HashMap<String, Set<String>>();
						useForAdditionsField.put("set", useForsAdditions);
						bibRecordAtomic.setField(fieldNameUseForAdditions, useForAdditionsField);
					}

					// Other additions
					if (!othersAdditions.isEmpty()) {
						Map<String, Set<String>> otherAdditionsField = new HashMap<String, Set<String>>();
						otherAdditionsField.put("set", othersAdditions);
						bibRecordAtomic.setField(fieldNameOtherAdditions, otherAdditionsField);
					}

					// Add record for atomic update only if there is something to index. This is to avoid overhead.
					if (!headings.isEmpty() || !headingsAdditions.isEmpty() || !useFors.isEmpty() || !useForsAdditions.isEmpty() || !othersAdditions.isEmpty()) {
						this.docsForAtomicUpdates.add(bibRecordAtomic);
					}
				}
				
				this.smHelper.print(this.print, "Integrating authority data to bibliographic record " + recordId + ". Records to process: " + noOfFoundBibRecords + "                                   \r");

				// Add documents from the class variable which was set before to Solr
				if (counter % INDEX_RATE == 0) { // Every n-th record, add documents to solr
					this.relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);
					this.docsForAtomicUpdates.clear();
					this.docsForAtomicUpdates = null;
					this.docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument

				} else if (counter >= noOfBibRecords) { // The remaining documents (if division with NO_OF_ROWS)
					this.relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);
					this.docsForAtomicUpdates.clear();
					this.docsForAtomicUpdates = null;
					this.docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				}
				
				// If the last document of the solr result page is reached, build a new query so that we can iterate over the next result page:
				if (recordId.equals(newLastDocId)) {
					returnValue = recordId;
				}
			}
		}

		return returnValue;
	}


	/**
	 * Getting GND authority records by multiple IDs that are used in MAB fields 001 or 035
	 * 
	 * @param entity	String representing the entity of GND records to query
	 * @param gndIds	List<String> representing IDs of GND records
	 * @return			SolrDocumentList containing the query result with GND records
	 */
	private SolrDocumentList getGndRecords(String entity, Set<String> gndIds) {
		SolrDocumentList gndRecords = null;

		// Create query string
		String queryString = "";
		for (String gndId : gndIds) {
			queryString += "id:\""+gndId+"\" || gndId035_str_mv:\""+gndId+"\" || "; // Join fields with the "OR" query operator
		}
		queryString = queryString.trim();
		queryString = queryString.replaceFirst("(\\|\\|)$", "").trim(); // Remove the last "OR" query operator


		// New Solr query
		SolrQuery queryGndRecords = new SolrQuery();

		// Define a query (empty query for all - we will use a filter query below)
		queryGndRecords.setQuery("*:*");

		// Set a filter query
		queryGndRecords.setFilterQueries(queryString, "entity_str:\"" + entity + "\"", "existsInBiblio_str:true");

		// Set fields that should be given back from the query
		queryGndRecords.setFields("id", "heading", "heading_additions_txt_mv", "use_for", "use_for_additions_txt_mv", "other_additions_txt_mv");


		try {
			SolrDocumentList resultList = this.solrServerAuth.query(queryGndRecords).getResults();
			gndRecords = (resultList.getNumFound() > 0 && resultList != null) ? resultList : null; // Get GND records
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return gndRecords;
	}
}