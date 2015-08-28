package betullam.akimporter.solrmab2.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class ParentToChilds {

	// General variables
	SolrServer sServer;
	String timeStamp;
	Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	Helper helper;
	int NO_OF_ROWS = 1000;
	int counter = 0;
	long noOfDocs = 0;


	public ParentToChilds(SolrServer sServer, String timeStamp) {
		this.sServer = sServer;
		this.timeStamp = timeStamp;
		helper = new Helper(sServer, timeStamp);
	}



	public void addParentsToChilds() {

		SolrDocumentList queryResults = helper.getCurrentlyIndexedChildRecords(true, null);

		// Show how many documents were found
		noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;
				// Get the ID of the last document in the current page so that we can build a new filter query to iterate over the next page:
				lastDocId = linkParentsToChilds(isFirstPage, lastDocId);

				// Add documents to Solr
				helper.indexDocuments(docsForAtomicUpdates);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection to save memory
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				boolean isFirstPage = (wholePages <= 0) ? true : false;
				linkParentsToChilds(isFirstPage, lastDocId);

				// Add documents to Solr
				helper.indexDocuments(docsForAtomicUpdates);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection to save memory
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			// Commit the changes
			try {
				this.sServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates = null;
				queryResults = null;
			}
		}

	}



	public String linkParentsToChilds(boolean isFirstPage, String lastDocId) {

		// Variable for return value:
		String returnValue = null;

		SolrDocumentList resultDocList = helper.getCurrentlyIndexedChildRecords(isFirstPage, lastDocId);

		String newLastDocId = resultDocList.get(resultDocList.size()-1).getFieldValue("id").toString();
		
		for (SolrDocument childRecord : resultDocList) {
			
			counter = counter + 1;
			
			String docId = (childRecord.getFieldValue("id") != null) ? childRecord.getFieldValue("id").toString() : null;
			String parentAc = helper.getParentAc(childRecord);
			SolrDocument parentRecord = helper.getParentRecord(parentAc);

			if (parentRecord != null) {
				String parentRecordSys = (parentRecord.getFieldValue("id") != null) ? parentRecord.getFieldValue("id").toString() : "0";
				String parentRecordTitle = (parentRecord.getFieldValue("title") != null) ? parentRecord.getFieldValue("title").toString() : "0";

				// Prepare child record for atomic updates:
				SolrInputDocument parentToChildDoc = null;
				parentToChildDoc = new SolrInputDocument();
				parentToChildDoc.setField("id", docId);

				// Add values to child record with atomic update:
				Map<String, String> mapParentRecordSys = new HashMap<String, String>();
				mapParentRecordSys.put("set", parentRecordSys);
				parentToChildDoc.setField("parentSYS_str", mapParentRecordSys);

				Map<String, String> mapParentRecordTitle = new HashMap<String, String>();
				mapParentRecordTitle.put("set", parentRecordTitle);
				parentToChildDoc.setField("parentTitle_str", mapParentRecordTitle);

				// Add all values of MU child record to MH parent record:
				docsForAtomicUpdates.add(parentToChildDoc);

				System.out.print("Linking parent " + parentRecordSys + " to " + docId + ". Processing record no " + counter  + " of " + noOfDocs + "\r");
			}


			//System.out.print("Processing record no " + counter  + "\r");

			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}
		return returnValue;
	}



	/*
	private SolrDocumentList getQueryResults(boolean isFirstPage, String lastDocId) {

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
		if (isFirstPage) { // No range filter on first page
			query.setFilterQueries("parentAC_str:* || parentSeriesAC_str:*", "indexTimestamp_str:"+timeStamp, "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			query.setStart(1);
			query.setFilterQueries("parentAC_str:* || parentSeriesAC_str:*", "indexTimestamp_str:"+timeStamp, "id:[" + lastDocId + " TO *]");
		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_str", "parentSYS_str", "parentAC_str", "parentSeriesAC_str");


		try {
			// Execute query and get results
			queryResult = sServer.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;
	}
	 */

	/*
	private void indexDocuments(Collection<SolrInputDocument> docsForAtomicUpdates) {
		if (!docsForAtomicUpdates.isEmpty()) {
			try {
				this.sServer.add(docsForAtomicUpdates); // Add the collection of documents to Solr
				this.sServer.commit(); // Commit the changes
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates = null; // Set to null to save memory
			}
		}
	}
	 */


}
