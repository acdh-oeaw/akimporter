package main.java.betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class OtherEdition {

	private HttpSolrServer solrServerBiblio;
	private RelationHelper relationHelper;
	private String timeStamp;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private int NO_OF_ROWS = 500;

	public OtherEdition(HttpSolrServer solrServerBiblio, String timeStamp) {
		this.solrServerBiblio = solrServerBiblio;
		this.timeStamp = timeStamp;
		this.relationHelper = new RelationHelper(solrServerBiblio, null, timeStamp);
	}


	/**
	 * 1. Get all records with the field "otherEditionId_str_mv" (RelationHelper -> getCurrentlyIndexedRecordsWithOtherEdition())
	 * 2. Iterate over these records and use "getOtherEditionRecord" to get the Solr-Record-ID (SYS-No.) of the record for the "other Edition"
	 * 3. Add the Solr-Record-ID (SYS-No.) to the record with atomic updates.
	 */


	/**
	 * Adding link to other edition based on the information of the record of the current edition. 
	 */
	public void addChildsToParentsFromChilds() {

		SolrDocumentList queryResults = relationHelper.getCurrentlyIndexedRecordsWithOtherEdition(true, null);

		// Get the number of documents that were found
		long noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Clear query results. We don't need them anymore.
			queryResults.clear();
			queryResults = null;

			// Calculate the number of Solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);


			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the SYS No of the other edition to the record of the current edition
				lastDocId = setOtherEdition(isFirstPage, lastDocId);

				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setOtherEdition(isFirstPage, lastDocId);

				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);
			}


			// Commit the changes
			try {
				this.solrServerBiblio.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				queryResults = null;
			}			
		}
	}


	private String setOtherEdition(boolean isFirstPage, String lastDocId) {
		// Variable for return value
		String returnValue = null;

		SolrDocumentList currentEditionRecords = relationHelper.getCurrentlyIndexedRecordsWithOtherEdition(isFirstPage, lastDocId);
		String newLastDocId = currentEditionRecords.get(currentEditionRecords.size()-1).getFieldValue("id").toString();

		String docId = null;

		for (SolrDocument currentEditionRecord : currentEditionRecords) {
			String currentEditionIds = (currentEditionRecord != null && currentEditionRecord.get("otherEditionId_str_mv") != null) ? currentEditionRecord.get("otherEditionId_str_mv").toString() : null;

			if (currentEditionIds != null) {
				String otherEditionSYS = getOtherEditionSys(currentEditionIds);

				if (otherEditionSYS != null) {
					// TODO: ATOMIC UPDATE HERE!!
				}
				docId = (currentEditionRecord.getFieldValue("id") != null) ? currentEditionRecord.getFieldValue("id").toString() : null;

				// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
				if (docId.equals(newLastDocId)) {
					returnValue = docId;
				}
			}
		}

		return returnValue;


	}

	/**
	 * Getting SYS No. (id) of the "other Edition" by its AC-No. or ZDB ID
	 * 
	 * @param id		AC-No. or ZDB ID of record
	 * @return			SolrDocument representing the record of the other Edition
	 */
	private String getOtherEditionSys(String id) {
		String otherEditionSys = null;
		SolrDocument otherEditionRecord = null;
		SolrQuery queryOtherEdition = new SolrQuery(); // New Solr query
		queryOtherEdition.setQuery("acNo_txt:\""+id+"\" || zdbId_txt:\""+id+"\""); // Define a query
		queryOtherEdition.setFields("id", "title"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultList = this.solrServerBiblio.query(queryOtherEdition).getResults();
			if (resultList.getNumFound() > 0 && resultList != null) {
				otherEditionRecord = resultList.get(0);// Get Solr document (there should only be one)
				otherEditionSys = otherEditionRecord.get("id").toString();
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return otherEditionSys;
	}
}
