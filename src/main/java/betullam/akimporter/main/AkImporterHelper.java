/**
 * Helper class for indexing data to Solr.
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
package main.java.betullam.akimporter.main;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class AkImporterHelper {

	private HttpSolrServer solrServer = null;
	private String timeStamp = null;
	private int NO_OF_ROWS = 500;
	private int INDEX_RATE = 500;
	private Collection<SolrInputDocument> docsForAtomicUpdates;

	public AkImporterHelper() {}

	public AkImporterHelper(HttpSolrServer solrServer) {
		this.solrServer = solrServer;
	}

	
	/**
	 * Getting the rules defined in a translation file.
	 * 
	 * @param filename					File name of the translation file.
	 * @param pathToTranslationFiles	Path to the directory where the translation files are stored.
	 * @return							A HashMap<String, String> representing the rules defined in a translation file.
	 */
	public HashMap<String, String> getTranslateProperties(String filename, String pathToTranslationFiles, boolean useDefaultProperties) {

		HashMap<String, String> translateProperties = new HashMap<String, String>();

		Properties properties = new Properties();
		String translationFile = pathToTranslationFiles + File.separator + filename;
		BufferedInputStream translationStream = null;

		try {
			// Get .properties file and load contents:
			if (useDefaultProperties) {
				translationStream = new BufferedInputStream(Main.class.getResourceAsStream("/main/resources/" + filename));
			} else {
				translationStream = new BufferedInputStream(new FileInputStream(translationFile));
			}
			properties.load(translationStream);
			translationStream.close();
		} catch (FileNotFoundException e) {
			System.err.println("Error: File not found! Please check if the file \"" + translationFile + "\" is in the same directory as mab.properties.\n");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Map.Entry<?, ?> property : properties.entrySet()) {
			String key = (String)property.getKey();
			String value = (String)property.getValue();
			translateProperties.put(key, value);
		}

		return translateProperties;
	}
	

	/**
	 * De-duplicate multivalued Solr fields with atomic updates.
	 * @param solrServer		HttpSolrServer: Solr server incl. core containing the fields to de-duplicate
	 * @param solrFieldNames	List<String>: Solr fields to de-duplicate
	 * @param print				boolean: true if status messages should be printed, false otherwise.
	 * @param optimize			boolean: true if Solr server should be optimized, false otherwise.
	 */
	public void dedupSolrMultivaluedField(HttpSolrServer solrServer, List<String> solrFieldNames, boolean print, boolean optimize) {
		docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
		SolrDocumentList queryResults = this.getSolrDocsByFieldnames(solrServer, solrFieldNames, true, null);

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

				SolrDocumentList solrDocs = this.getSolrDocsByFieldnames(solrServer, solrFieldNames, isFirstPage, lastDocId);

				// Integrate the data of the authority records to the bibliographic records				
				lastDocId = this.addDedupUpdateToSolr(solrServer, solrDocs, solrFieldNames, print);
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				SolrDocumentList solrDocs = this.getSolrDocsByFieldnames(solrServer, solrFieldNames, isFirstPage, lastDocId);

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				this.addDedupUpdateToSolr(solrServer, solrDocs, solrFieldNames, print);
			}

			this.print(print, "\nDone deduplication.");

			// Commit the changes
			try {
				solrServer.commit();
				if (optimize) {
					solrServer.optimize();
				}
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

	
	/**
	 * Adding de-duplicated values for multivalued fields to Solr
	 * @param solrServer		HttpSolrServer: Solr server incl. core containing the fields to de-duplicate
	 * @param documents			SolrDocumentList: List of documents containing the fields to de-duplicate
	 * @param solrFieldNames	List<String>: Solr fields to de-duplicate
	 * @param print				boolean: true if status messages should be printed, false otherwise.
	 * @return
	 */
	private String addDedupUpdateToSolr(HttpSolrServer solrServer, SolrDocumentList documents, List<String> solrFieldNames, boolean print) {

		// Variables:
		String returnValue = null;
		int counter = 0;

		try {

			if (documents != null) {

				int noOfDocs = documents.size();

				if (noOfDocs > 0) {

					String newLastDocId = documents.get(documents.size()-1).getFieldValue("id").toString();

					for (SolrDocument document : documents) {
						counter = counter + 1;
						String id = document.getFieldValue("id").toString();

						// Prepare Solr record for atomic update
						SolrInputDocument updateRecord = null;
						updateRecord = new SolrInputDocument();
						updateRecord.setField("id", id);

						for (String solrFieldName : solrFieldNames) {
							if (!solrFieldName.equals("id")) {
								
								Collection<Object> fieldValues = null;
								if (document.getFieldValues(solrFieldName) != null) {
									fieldValues = document.getFieldValues(solrFieldName);
								}

								if (fieldValues != null && fieldValues.size() > 0) {

									Set<String> dedupValues = new HashSet<String>();

									for (Object fieldValue : fieldValues) {
										dedupValues.add(fieldValue.toString());
									}

									// Set values for atomic update
									Map<String, Set<String>> updateField = new HashMap<String, Set<String>>();
									updateField.put("set", dedupValues);
									updateRecord.setField(solrFieldName, updateField);

									if (updateRecord != null) {
										this.docsForAtomicUpdates.add(updateRecord);
									}
								}
							}
						}

						this.print(print, "Deduplicating fields of record " + id +"                                                               \r");


						// Add documents from the class variable which was set before to Solr
						if (!docsForAtomicUpdates.isEmpty()) {
							if (counter % INDEX_RATE == 0) { // Every n-th record, add documents to solr
								solrServer.add(this.docsForAtomicUpdates);
								this.docsForAtomicUpdates.clear();
								this.docsForAtomicUpdates = null;
								this.docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
							} else if (counter >= noOfDocs) { // The remaining documents (if division with NO_OF_ROWS)
								solrServer.add(this.docsForAtomicUpdates);
								this.docsForAtomicUpdates.clear();
								this.docsForAtomicUpdates = null;
								this.docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
							}
						}

						// If the last document of the solr result page is reached, build a new query so that we can iterate over the next result page:
						if (id.equals(newLastDocId)) {
							returnValue = id;
						}
					}
				}
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return returnValue;
	}

	/**
	 * Returns Solr documents if the specified field name exists in the documents.
	 * 
	 * @param solrServer		HttpSolrServer: The Solr server to use
	 * @param solrFieldName		String: The Solr field name for the query
	 * @param isFirstPage		boolean: First page of result?
	 * @param lastDocId			String: Document ID of the last processed Solr document
	 * @return					SolrDocumentList: A result set of the query as SolrDocumentList
	 */
	private SolrDocumentList getSolrDocsByFieldnames(HttpSolrServer solrServer, List<String> solrFieldNames, boolean isFirstPage, String lastDocId) {

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
		for (String solrFieldName : solrFieldNames) {
			queryString += solrFieldName + ":* || "; // Join fields with the "OR" query operator
		}
		queryString = queryString.trim();
		queryString = queryString.replaceFirst("(\\|\\|)$", "").trim(); // Remove the last "OR" query operator

		
		// Filter all records that were indexed with the current import process if applicable to avoid overhead.
		if (this.timeStamp != null) {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(queryString.trim(), "indexTimestamp_str:"+this.timeStamp, "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(queryString.trim(), "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(queryString.trim(), "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(queryString.trim(), "id:[" + lastDocId + " TO *]");
			}
		}

		// Set fields that should be given back from the query
		List<String> solrFieldsToReturn = new ArrayList<>(solrFieldNames); // Copy immutable List<String> to a mutable List<String>
		solrFieldsToReturn.add("id");
		String[] arrFieldsToReturn = solrFieldsToReturn.toArray(new String[0]);
		query.setFields(arrFieldsToReturn);

		try {
			// Execute query and get results
			queryResult = solrServer.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return queryResult;
	}

	/**
	 * Get human readable execution time between two moments in time expressed in milliseconds.
	 * @param startTime		Moment of start in milliseconds
	 * @param endTime		Moment of end in milliseconds
	 * @return				String of human readable execution time.
	 */
	public String getExecutionTime(long startTime, long endTime) {
		String executionTime = null;

		long timeElapsedMilli =  endTime - startTime;
		int seconds = (int) (timeElapsedMilli / 1000) % 60 ;
		int minutes = (int) ((timeElapsedMilli / (1000*60)) % 60);
		int hours   = (int) ((timeElapsedMilli / (1000*60*60)) % 24);

		executionTime = hours + ":" + minutes + ":" + seconds;
		return executionTime;
	}

	/**
	 * Starts an optimize action for a Solr core.
	 */
	public void solrOptimize() {
		try {
			this.solrServer.optimize();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints a text message to the console if "print" is true.
	 * @param print		True if the message should be printed.
	 * @param text		The text to print to the console.
	 */
	public void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}
}
