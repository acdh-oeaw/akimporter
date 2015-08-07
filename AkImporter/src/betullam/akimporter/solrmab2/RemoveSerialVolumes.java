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

public class RemoveSerialVolumes {

	Collection<SolrInputDocument> serialVolumeAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	Collection<SolrInputDocument> parentSeriesAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	static private int NO_OF_ROWS = 1000;
	int rowCounter = 0;
	boolean print = true;

	public RemoveSerialVolumes() {};
	public RemoveSerialVolumes(boolean print) {
		this.print = print;
	};

	public void removeSerialVolumes(SolrServer sServer) {

		// New Solr query
		SolrQuery querySerialVolumes = new SolrQuery();

		// Define a query for getting all documents. We get the serial volumes with a filter query because of performance (see below)
		querySerialVolumes.setQuery("*:*");

		// The no of rows over that we can iterate ( see "for(SolrDocument doc : resultDocList)" ):
		querySerialVolumes.setRows(NO_OF_ROWS);

		// Sort by id (more efficient for deep paging):
		querySerialVolumes.setSort(SolrQuery.SortClause.asc("id"));

		// Set a filter query (more efficient for deep paging).
		querySerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:*");

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
			print("\nNo. of serial volumes found: " + noOfSerialVolumes);

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
					lastDocId = removeVolumes(sServer, lastDocId, isFirstPage);
				}

				// Add documents on the last page:
				if (fractionPages != 0) {
					// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
					boolean isFirstPage = (wholePages <= 0) ? true : false;
					removeVolumes(sServer, lastDocId, isFirstPage);
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




	private String removeVolumes(SolrServer sServer, String lastDocId, boolean isFirstPage) {
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
		if (isFirstPage) { // No range filter on first page
			fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			// Set start of query to 1 so that the "lastDocId" ist not the first id of the new page (we would have doubled documents then)
			fqSerialVolumes.setStart(1);
			fqSerialVolumes.setFilterQueries("parentSeriesAC_str:*", "id:[" + lastDocId + " TO *]");
		}

		// Set fields that should be given back from the query
		fqSerialVolumes.setFields("id", "parentSeriesAC_str");

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
				print("Removing serial volume " + docId + "\r");
				
				// Get parent AC-No. This is a must to get the parentSysNo (= id):
				String serialvolParentAC = (doc.getFieldValue("parentSeriesAC_str") != null) ? doc.getFieldValue("parentSeriesAC_str").toString() : null;

				
				// Variables for atomic updates of parent series:
				String parentSeriesSYS = "0";

				SolrQuery queryParentSeries = new SolrQuery(); // Query for parent series of current serial voume
				queryParentSeries.setQuery("acNo_str:" + serialvolParentAC); // Query parent series
				queryParentSeries.setFields("id"); // Set fields that should be given back from the query
				QueryResponse responseParentSeries = sServer.query(queryParentSeries); // Execute query
				SolrDocumentList resultListParentSeries = responseParentSeries.getResults();


				if (!resultListParentSeries.isEmpty() && resultListParentSeries != null && resultListParentSeries.getNumFound() > 0) { // Parent record exists
					SolrDocument resultDocParentSeries = resultListParentSeries.get(0); // Get first document from query result (there should be only one!)
					parentSeriesSYS = (resultDocParentSeries.getFieldValue("id") != null) ? resultDocParentSeries.getFieldValue("id").toString() : "0";
					

					// Prepare parent series record for atomic updates:
					SolrInputDocument parentSeriesRemoveVolumeDoc = null;
					parentSeriesRemoveVolumeDoc = new SolrInputDocument();
					parentSeriesRemoveVolumeDoc.addField("id", parentSeriesSYS);
					
					
					// Remove all field values to avoid double entries of the same serial volume.
					Map<String, String> mapRemoveSerialVolumeSYS = new HashMap<String, String>();
					mapRemoveSerialVolumeSYS.put("set", null);
					parentSeriesRemoveVolumeDoc.setField("serialvolumeSYS_str_mv", mapRemoveSerialVolumeSYS);

					Map<String, String> mapRemoveSerialVolumeAC = new HashMap<String, String>();
					mapRemoveSerialVolumeAC.put("set", null);
					parentSeriesRemoveVolumeDoc.setField("serialvolumeAC_str_mv", mapRemoveSerialVolumeAC);

					Map<String, String> mapRemoveSerialVolumeTitle = new HashMap<String, String>();
					mapRemoveSerialVolumeTitle.put("set", null);
					parentSeriesRemoveVolumeDoc.setField("serialvolumeTitle_str_mv", mapRemoveSerialVolumeTitle);

					Map<String, String> mapRemoveSerialVolumeVolumeNo = new HashMap<String, String>();
					mapRemoveSerialVolumeVolumeNo.put("set", null);
					parentSeriesRemoveVolumeDoc.setField("serialvolumeVolumeNo_str_mv", mapRemoveSerialVolumeVolumeNo);

					Map<String, String> mapRemoveSerialVolumeVolumeNoSort = new HashMap<String, String>();
					mapRemoveSerialVolumeVolumeNoSort.put("set", null);
					parentSeriesRemoveVolumeDoc.setField("serialvolumeVolumeNoSort_str_mv", mapRemoveSerialVolumeVolumeNoSort);

					Map<String, String> mapRemoveSerialVolumeEdition = new HashMap<String, String>();
					mapRemoveSerialVolumeEdition.put("set", null);
					parentSeriesRemoveVolumeDoc.setField("serialvolumeEdition_str_mv", mapRemoveSerialVolumeEdition);

					Map<String, String> mapRemoveSerialVolumePublishDate = new HashMap<String, String>();
					mapRemoveSerialVolumePublishDate.put("set", null);
					parentSeriesRemoveVolumeDoc.setField("serialvolumePublishDate_str_mv", mapRemoveSerialVolumePublishDate);
					 

					// Add all values of serial volume to parent series record:
					parentSeriesAtomicUpdateDocs.add(parentSeriesRemoveVolumeDoc);

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
