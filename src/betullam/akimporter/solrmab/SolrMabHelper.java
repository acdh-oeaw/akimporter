/**
 * Helper class for indexing data to Solr.
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
package betullam.akimporter.solrmab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrMabHelper {

	private HttpSolrServer solrServer = null;
	private String timeStamp = null;
	private int NO_OF_ROWS = 500;
	private int INDEX_RATE = 500;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();

	public SolrMabHelper() {}

	public SolrMabHelper(HttpSolrServer solrServer) {
		this.solrServer = solrServer;
	}


	public void dedupSolrMultivaluedField(HttpSolrServer solrServer, String solrFieldName, boolean print, boolean optimize) {
		SolrDocumentList queryResults = this.getSolrDocsByFieldname(solrServer, solrFieldName, true, null);

		// Get the number of documents that were found
		long noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {
			// Clear query results. We don't need them anymore.
			queryResults.clear();
			queryResults = null;

			this.print(print, "Deduplicating field " + solrFieldName + " in " + noOfDocs + " documents ... ");

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				SolrDocumentList solrDocs = this.getSolrDocsByFieldname(solrServer, solrFieldName, isFirstPage, lastDocId);

				// Integrate the data of the authority records to the bibliographic records				
				lastDocId = addDedupUpdateToSolr(solrServer, solrDocs, solrFieldName);
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				SolrDocumentList solrDocs = this.getSolrDocsByFieldname(solrServer, solrFieldName, isFirstPage, lastDocId);

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				addDedupUpdateToSolr(solrServer, solrDocs, solrFieldName);
			}

			this.print(print, "Done deduplication.\n");

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

	
	private String addDedupUpdateToSolr(HttpSolrServer solrServer, SolrDocumentList documents, String solrFieldName) {
		
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
						Collection<Object> fieldValues = document.getFieldValues(solrFieldName);

						if (fieldValues.size() > 0) {

							Set<String> dedupValues = new HashSet<String>();

							for (Object fieldValue : fieldValues) {
								dedupValues.add(fieldValue.toString());
							}

							// Prepare Solr record for atomic update
							SolrInputDocument updateRecord = null;
							updateRecord = new SolrInputDocument();
							updateRecord.setField("id", id);

							// Set values for atomic update
							Map<String, Set<String>> updateField = new HashMap<String, Set<String>>();
							updateField.put("set", dedupValues);
							updateRecord.setField(solrFieldName, updateField);

							this.docsForAtomicUpdates.add(updateRecord);

							System.out.println("Deduplicateing field " + solrFieldName + " of record id " + id + ".");
						}

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
	private SolrDocumentList getSolrDocsByFieldname(HttpSolrServer solrServer, String solrFieldName, boolean isFirstPage, String lastDocId) {
		//SolrDocumentList documents = null;
		//Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();

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

		// Filter all records that were indexed with the current import process if applicable to avoid overhead.
		if (this.timeStamp != null) {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(solrFieldName+":*", "indexTimestamp_str:"+this.timeStamp, "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(solrFieldName+":*", "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
			}
		} else {
			if (isFirstPage) { // No range filter on first page
				query.setFilterQueries(solrFieldName+":*", "id:*");
			} else { // After the first query, we need to use ranges to get the appropriate results
				query.setStart(1);
				query.setFilterQueries(solrFieldName+":*", "id:[" + lastDocId + " TO *]");
			}
		}

		// Set fields that should be given back from the query
		query.setFields("id", solrFieldName);

		try {
			// Execute query and get results
			queryResult = solrServer.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return queryResult;

		/*
		try {
			SolrDocumentList resultList = solrServer.query(query).getResults();
			documents = (resultList.getNumFound() > 0 && resultList != null) ? resultList : null; // Get records

			if (documents != null) {
				for (SolrDocument document : documents) {
					Collection<Object> values = document.getFieldValues(solrFieldName);
					if (values.size() > 0) {
						String id = document.getFieldValue("id").toString();
						Set<String> dedupValues = new HashSet<String>();

						for (Object value : values) {
							dedupValues.add(value.toString());
						}

						// Prepare Solr record for atomic update:
						SolrInputDocument updateRecord = null;
						updateRecord = new SolrInputDocument();
						updateRecord.setField("id", id);

						// Set values for atomic update of parent record:
						Map<String, Set<String>> updateField = new HashMap<String, Set<String>>();
						updateField.put("set", dedupValues);
						updateRecord.setField(solrFieldName, updateField);

						docsForAtomicUpdates.add(updateRecord);

						System.out.println("Dedup field " + solrFieldName + " of record id " + id);
					}
				}
			}

			if (!docsForAtomicUpdates.isEmpty()) {
				try {
					solrServer.add(docsForAtomicUpdates); // Add the collection of documents to Solr
					solrServer.commit(); // Commit
					if (optimize) {
						solrServer.optimize();
					}
				} catch (SolrServerException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					docsForAtomicUpdates.clear();
					docsForAtomicUpdates = null;
				}
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		 */

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
