package betullam.akimporter.solrmab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

// For deep paging and the idea of itteration with filter-queries, see:
// https://lucidworks.com/blog/coming-soon-to-solr-efficient-cursor-based-iteration-of-large-result-sets

public class SerialVolumeToParent {

	Collection<SolrInputDocument> serialVolumeAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	Collection<SolrInputDocument> parentSeriesAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	static private int NO_OF_ROWS = 1000;
	int rowCounter = 0;
	String timeStamp = null;
	boolean print = true;

	public SerialVolumeToParent() {};
	public SerialVolumeToParent(String timeStamp, boolean print) {
		this.timeStamp = timeStamp;
		this.print = print;
	};


	public void addSerialVolumes(SolrServer sServer) {

		// New Solr query
		SolrQuery querySerialVolumes = new SolrQuery();

		// Define a query for getting all documents. We get the serial volumes with a filter query because of performance (see below)
		querySerialVolumes.setQuery("*:*");

		// The no of rows over that we can iterate ( see "for(SolrDocument doc : resultDocList)" ):
		querySerialVolumes.setRows(NO_OF_ROWS);

		// Sort by id (more efficient for deep paging):
		querySerialVolumes.setSort(SolrQuery.SortClause.asc("id"));

		// Set a filter query (more efficient for deep paging).
		// If a timeStamp is set, then only process records with this timestamp. This is more efficient when doing an update of some records.
		// If no timeStamp is set, then unlink all volumes from it's parents.
		if (timeStamp == null) {
			querySerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:*");
		} else {
			querySerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:*", "indexTimestamp_str:"+timeStamp);
		}


		// Set fields that should be given back from the query
		querySerialVolumes.setFields("id", "title", "acNo_str", "parentSeriesSYS_str", "parentSeriesAC_str", "serialVolumeNo_str", "serialVolumeNoSort_str", "publishDate", "edition");

		// Initialize Variable for query response:
		QueryResponse responseSerialVolumes = null;

		try {
			// Execute query
			responseSerialVolumes = sServer.query(querySerialVolumes);

			// Get document-list from query result
			SolrDocumentList resultSerialVolumesList = responseSerialVolumes.getResults();

			// Show how many documents were found
			long noOfSerialVolumes = resultSerialVolumesList.getNumFound();
			print("No. of serial volumes to process: " + noOfSerialVolumes + "\n");

			// If there are some records, go on. If not, do nothing.
			if (resultSerialVolumesList != null && noOfSerialVolumes > 0) {

				// Calculate the number of solr result pages we need to iterate over
				long wholePages = (noOfSerialVolumes/NO_OF_ROWS);
				long fractionPages = (noOfSerialVolumes%NO_OF_ROWS);

				// Variable for lastDocId
				String lastDocId = null;				

				for (long l = 0; l < wholePages; l++) {
					boolean isFirstPage = (l == 0) ? true : false;

					// Get the ID of the last document in the current page so that we can build a new filter query to iterate over the next page:
					lastDocId = filterQuery(sServer, lastDocId, isFirstPage);
				}

				// Add documents on the last page:
				if (fractionPages != 0) {
					// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
					boolean isFirstPage = (wholePages <= 0) ? true : false;
					filterQuery(sServer, lastDocId, isFirstPage);
				}


				if (serialVolumeAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(serialVolumeAtomicUpdateDocs);
				}

				if (parentSeriesAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(parentSeriesAtomicUpdateDocs);
				}

				// Set doc-collections to null (save memory):
				serialVolumeAtomicUpdateDocs = null;
				parentSeriesAtomicUpdateDocs = null;

			}

		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}


	private String filterQuery(SolrServer sServer, String lastDocId, boolean isFirstPage) {

		// Variable for return value:
		String returnValue = null;

		// New Solr query
		SolrQuery fqSerialVolumes = new SolrQuery();

		// Define a query for getting all documents. We get the serial volumes with a filter query because of performance (see below)
		fqSerialVolumes.setQuery("*:*");

		// The no of rows over that we can iterate ( see "for(SolrDocument doc : resultDocList)" ):
		fqSerialVolumes.setRows(NO_OF_ROWS);

		// Sort by id (more efficient for deep paging):
		fqSerialVolumes.setSort(SolrQuery.SortClause.asc("id"));


		// Set a filter query (more efficient for deep paging).
		// If a timeStamp is set, then only process records with this timestamp. This is more efficient when doing an update of some records.
		// If no timeStamp is set, then unlink all volumes from it's parents.
		if (isFirstPage) { // No range filter on first page
			if (timeStamp == null) {
				fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:*");
			} else {
				fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:*", "indexTimestamp_str:"+timeStamp);
			}
		} else { // After the first query, we need to use ranges to get the appropriate results
			// Set start of query to 1 so that the "lastDocId" ist not the first id of the new page (we would have doubled documents then)
			fqSerialVolumes.setStart(1);

			if (timeStamp == null) {
				fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:[" + lastDocId + " TO *]");
			} else {
				fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:[" + lastDocId + " TO *]", "indexTimestamp_str:"+timeStamp);
			}
		}
		/*
		// CODE BEFORE TIMESTAMP
		if (isFirstPage) { // No range filter on first page
			fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			// Set start of query to 1 so that the "lastDocId" ist not the first id of the new page (we would have doubled documents then)
			fqSerialVolumes.setStart(1);
			fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:[" + lastDocId + " TO *]");
		}
		 */

		// Set fields that should be given back from the query
		fqSerialVolumes.setFields("id", "title", "acNo_str", "parentSeriesSYS_str", "parentSeriesAC_str", "serialVolumeNo_str", "serialVolumeNoSort_str", "publishDate", "edition");

		// Initialize Variable for query response:
		QueryResponse fqResponse = null;


		try {
			// Execute query
			fqResponse = sServer.query(fqSerialVolumes); 

			// Get document-list from query result
			SolrDocumentList resultDocList = fqResponse.getResults();

			String newLastDocId = resultDocList.get(resultDocList.size()-1).getFieldValue("id").toString();

			for (SolrDocument doc : resultDocList) {
				String docId = doc.getFieldValue("id").toString();
				print("Linking serial volume " + docId + "\r");

				// Variables for atomic updates of serial volume:
				String parentSeriesSYS = "0";
				String parentSeriesTitle = "0";

				// Variables for atomic updates of parent series:
				String serialvolAC = (doc.getFieldValue("acNo_str") != null) ? doc.getFieldValue("acNo_str").toString() : "0";
				String serialvolSYS = (doc.getFieldValue("id") != null) ? doc.getFieldValue("id").toString() : "0";
				String serialvolParentAC = (doc.getFieldValue("parentSeriesAC_str") != null) ? doc.getFieldValue("parentSeriesAC_str").toString() : null;
				String serialvolTitle = (doc.getFieldValue("title") != null) ? doc.getFieldValue("title").toString() : "0";
				String serialvolVolumeNo = (doc.getFieldValue("serialVolumeNo_str") != null) ? doc.getFieldValue("serialVolumeNo_str").toString() : "0";
				String serialvolVolumeNoSort = (doc.getFieldValue("serialVolumeNoSort_str") != null) ? doc.getFieldValue("serialVolumeNoSort_str").toString() : "0";
				String serialvolEdition = (doc.getFieldValue("edition") != null) ? doc.getFieldValue("edition").toString() : "0";
				String serialvolPublishDate = (doc.getFieldValue("publishDate") != null) ? doc.getFieldValue("publishDate").toString().replace("[", "").replace("]", "") : "0";

				// First "set" data (SYS-No and title) from parent series record to current serial volume record:
				// "set" data means: Set or replace the field value(s) with the specified value(s), or remove the values if 'null' or empty list is specified as the new value.
				SolrQuery queryParentSeries = new SolrQuery(); // Query for parent series of current serial voume
				queryParentSeries.setQuery("acNo_str:" + serialvolParentAC); // Query parent series
				queryParentSeries.setFields("id", "title"); // Set fields that should be given back from the query
				QueryResponse responseParentSeries = sServer.query(queryParentSeries); // Execute query
				SolrDocumentList resultListParentSeries = responseParentSeries.getResults();

				if (!resultListParentSeries.isEmpty() && resultListParentSeries != null && resultListParentSeries.getNumFound() > 0) { // Parent record exists
					SolrDocument resultDocParentSeries = resultListParentSeries.get(0); // Get first document from query result (there should be only one!)
					parentSeriesSYS = (resultDocParentSeries.getFieldValue("id") != null) ? resultDocParentSeries.getFieldValue("id").toString() : "0";
					parentSeriesTitle = (resultDocParentSeries.getFieldValue("title") != null) ? resultDocParentSeries.getFieldValue("title").toString() : "0";

					if (!parentSeriesSYS.equals("0")) {

						// Prepare serial volume for atomic updates:
						SolrInputDocument serialvolAtomicUpdateDoc = null;
						serialvolAtomicUpdateDoc = new SolrInputDocument();
						serialvolAtomicUpdateDoc.setField("id", serialvolSYS);

						// Set values from parent series record to serial volume:
						Map<String, String> mapParentSeriesSYS = new HashMap<String, String>();
						mapParentSeriesSYS.put("set", parentSeriesSYS);
						serialvolAtomicUpdateDoc.setField("parentSeriesSYS_str", mapParentSeriesSYS);

						Map<String, String> mapParentSeriesTitle = new HashMap<String, String>();
						mapParentSeriesTitle.put("set", parentSeriesTitle);
						serialvolAtomicUpdateDoc.setField("parentSeriesTitle_str", mapParentSeriesTitle);

						// Set all values of parent series to serial volume:
						serialVolumeAtomicUpdateDocs.add(serialvolAtomicUpdateDoc);




						// Prepare parent series record for atomic updates:
						SolrInputDocument parentSeriesAtomicUpdateDoc = null;
						parentSeriesAtomicUpdateDoc = new SolrInputDocument();
						parentSeriesAtomicUpdateDoc.setField("id", parentSeriesSYS);

						// Add all serial volumes to parent series record from scratch (we removed the existing ones before):
						Map<String, String> mapSerialVolumeSYS = new HashMap<String, String>();
						mapSerialVolumeSYS.put("add", serialvolSYS);
						parentSeriesAtomicUpdateDoc.setField("serialvolumeSYS_str_mv", mapSerialVolumeSYS);

						Map<String, String> mapSerialVolumeAC = new HashMap<String, String>();
						mapSerialVolumeAC.put("add", serialvolAC);
						parentSeriesAtomicUpdateDoc.setField("serialvolumeAC_str_mv", mapSerialVolumeAC);

						Map<String, String> mapSerialVolumeTitle = new HashMap<String, String>();
						mapSerialVolumeTitle.put("add", serialvolTitle);
						parentSeriesAtomicUpdateDoc.setField("serialvolumeTitle_str_mv", mapSerialVolumeTitle);

						Map<String, String> mapSerialVolumeVolumeNo = new HashMap<String, String>();
						mapSerialVolumeVolumeNo.put("add", serialvolVolumeNo);
						parentSeriesAtomicUpdateDoc.setField("serialvolumeVolumeNo_str_mv", mapSerialVolumeVolumeNo);

						Map<String, String> mapSerialVolumeVolumeNoSort = new HashMap<String, String>();
						mapSerialVolumeVolumeNoSort.put("add", serialvolVolumeNoSort);
						parentSeriesAtomicUpdateDoc.setField("serialvolumeVolumeNoSort_str_mv", mapSerialVolumeVolumeNoSort);

						Map<String, String> mapSerialVolumeEdition = new HashMap<String, String>();
						mapSerialVolumeEdition.put("add", serialvolEdition);
						parentSeriesAtomicUpdateDoc.setField("serialvolumeEdition_str_mv", mapSerialVolumeEdition);

						Map<String, String> mapSerialVolumePublishDate = new HashMap<String, String>();
						mapSerialVolumePublishDate.put("add", serialvolPublishDate);
						parentSeriesAtomicUpdateDoc.setField("serialvolumePublishDate_str_mv", mapSerialVolumePublishDate);


						// Add all values of serial volume to parent series record:
						parentSeriesAtomicUpdateDocs.add(parentSeriesAtomicUpdateDoc);
					}
				}

				// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
				if (docId.equals(newLastDocId)) {
					returnValue = docId;
				}
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return returnValue;
	}

	private void print(String text) {
		if (print) {
			System.out.print(text);
		}
	}




}
