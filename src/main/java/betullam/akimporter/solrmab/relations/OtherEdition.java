package main.java.betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class OtherEdition {

	private HttpSolrServer solrServerBiblio;
	private RelationHelper relationHelper;
	private SolrMabHelper smHelper;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private int NO_OF_ROWS = 500;
	private boolean print = false;
	private long noOfDocs = 0;
	private int counter = 0;
	

	public OtherEdition(HttpSolrServer solrServerBiblio, String timeStamp, boolean print) {
		this.solrServerBiblio = solrServerBiblio;
		this.print = print;
		this.smHelper = new SolrMabHelper();
		this.relationHelper = new RelationHelper(solrServerBiblio, null, timeStamp);
	}


	/**
	 * Adding link to "other edition" based on the information of the record of the current edition. 
	 */
	public void addOtherEditions() {

		// Get all currently indexed records that are containing data for other editions
		SolrDocumentList queryResults = relationHelper.getCurrentlyIndexedRecordsWithOtherEdition(true, null);

		// Get the number of documents that were found
		noOfDocs = queryResults.getNumFound();

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

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setOtherEdition(isFirstPage, lastDocId);

				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates, solrServerBiblio);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
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

		// Get currently indexed records that are containing data for other editions (use paging in Solr query for better performance)
		SolrDocumentList currentIndexedRecords = relationHelper.getCurrentlyIndexedRecordsWithOtherEdition(isFirstPage, lastDocId);
		String newLastDocId = currentIndexedRecords.get(currentIndexedRecords.size()-1).getFieldValue("id").toString();
		String docId = null;

		for (SolrDocument currentIndexedRecord : currentIndexedRecords) {
			docId = (currentIndexedRecord.getFieldValue("id") != null) ? currentIndexedRecord.getFieldValue("id").toString() : null;
			Collection<Object> currentEditionIds = (currentIndexedRecord != null && currentIndexedRecord.getFieldValues("otherEditionId_str_mv") != null && !currentIndexedRecord.getFieldValues("otherEditionId_str_mv").isEmpty()) ? currentIndexedRecord.getFieldValues("otherEditionId_str_mv") : null;

			if (currentEditionIds != null) {
				for (Object currentEditionId : currentEditionIds) {
					String editionId = currentEditionId.toString();
					String otherEditionSys = getOtherEditionSys(editionId);
					if (otherEditionSys != null) {

						// Prepare current record for atomic updates:
						SolrInputDocument atomicUpdateDoc = null;
						atomicUpdateDoc = new SolrInputDocument();
						atomicUpdateDoc.setField("id", docId);

						// Add ID (SYS No.) of "other edition" to current record
						Map<String, String> mapOtherEditionRecrodSys = new HashMap<String, String>();
						mapOtherEditionRecrodSys.put("set", otherEditionSys);
						atomicUpdateDoc.setField("otherEditionRecordId_txt_mv", mapOtherEditionRecrodSys);

						docsForAtomicUpdates.add(atomicUpdateDoc);

					}
				}
			}

			counter = counter + 1;
			this.smHelper.print(this.print, "Linking \"other editions\". Processing record no " + counter  + " of " + noOfDocs + "                                      \r");


			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}

		return returnValue;


	}

	/**
	 * Getting SYS No. (ID) of the "other edition" record by its AC-No. or ZDB ID
	 * 
	 * @param id		AC-No. or ZDB ID of record
	 * @return			String containing ID of the record of the "other edition"
	 */
	private String getOtherEditionSys(String id) {
		String otherEditionSys = null;
		SolrQuery queryOtherEdition = new SolrQuery(); // New Solr query
		queryOtherEdition.setQuery("acNo_txt:\""+id+"\" || zdbId_txt:\""+id+"\""); // Define a query
		queryOtherEdition.setFields("id"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultDocList = this.solrServerBiblio.query(queryOtherEdition).getResults();
			if (resultDocList.getNumFound() > 0 && resultDocList != null) {
				// Get ID of "other edition". We only take the first one, because we only can link to one other edition.
				otherEditionSys = resultDocList.get(0).getFieldValue("id").toString();
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return otherEditionSys;
	}
}
