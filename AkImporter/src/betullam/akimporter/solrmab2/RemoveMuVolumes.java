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

public class RemoveMuVolumes {

	Collection<SolrInputDocument> mhRecordAtomicUpdateDocs = new ArrayList<SolrInputDocument>();

	static private int NO_OF_ROWS = 1000;

	int rowCounter = 0;

	public void removeMuVolumes(SolrServer sServer) {

		// New Solr query
		SolrQuery queryMuVolumes = new SolrQuery();

		// Define a query for getting all documents. We get the serial volumes with a filter query because of performance (see below)
		queryMuVolumes.setQuery("*:*");

		// The no of rows over that we can iterate ( see "for(SolrDocument doc : resultDocList)" ):
		queryMuVolumes.setRows(NO_OF_ROWS);

		// Sort by id (more efficient for deep paging):
		queryMuVolumes.setSort(SolrQuery.SortClause.asc("id"));

		// Set a filter query (more efficient for deep paging).
		queryMuVolumes.setFilterQueries("parentAC_str:*", "id:*");

		// Set fields that should be given back from the query
		queryMuVolumes.setFields("id");

		// Initialize Variable for query response:
		QueryResponse responseMuVolumes = null;

		try {
			// Execute query
			responseMuVolumes = sServer.query(queryMuVolumes);

			// Get document-list from query result
			SolrDocumentList resultMuVolumesList = responseMuVolumes.getResults();

			// Show how many documents were found
			long noOfMuVolumes = resultMuVolumesList.getNumFound();
			System.out.println("\nNo. of MU records found: " + noOfMuVolumes);

			// If there are some records, go on. If not, do nothing.
			if (resultMuVolumesList != null && noOfMuVolumes > 0) {

				// Calculate the number of solr result pages we need to iterate over
				long wholePages = (noOfMuVolumes/NO_OF_ROWS);
				long fractionPages = (noOfMuVolumes%NO_OF_ROWS);

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


				if (mhRecordAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(mhRecordAtomicUpdateDocs);
				}

				// Set doc-collection to null (save memory):
				mhRecordAtomicUpdateDocs = null;

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
		SolrQuery fqVolumes = new SolrQuery();

		// Define a query for getting all documents. We get the serial volumes with a filter query because of performance (see below)
		fqVolumes.setQuery("*:*");

		// The no of rows over that we can iterate ( see "for(SolrDocument doc : resultDocList)" ):
		fqVolumes.setRows(NO_OF_ROWS);

		// Sort by id (more efficient for deep paging):
		fqVolumes.setSort(SolrQuery.SortClause.asc("id"));

		// Set a filter query (more efficient for deep paging).
		if (isFirstPage) { // No range filter on first page
			fqVolumes.setFilterQueries("parentAC_str:*", "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			// Set start of query to 1 so that the "lastDocId" ist not the first id of the new page (we would have doubled documents then)
			fqVolumes.setStart(1);
			fqVolumes.setFilterQueries("parentAC_str:*", "id:[" + lastDocId + " TO *]");
		}

		// Set fields that should be given back from the query
		fqVolumes.setFields("id", "parentAC_str");

		// Initialize Variable for query response:
		QueryResponse fqResponse = null;


		try {
			// Execute query
			fqResponse = sServer.query(fqVolumes); 

			// Get document-list from query result
			SolrDocumentList resultDocList = fqResponse.getResults();

			String newLastDocId = resultDocList.get(resultDocList.size()-1).getFieldValue("id").toString();

			for (SolrDocument doc : resultDocList) {

				String docId = doc.getFieldValue("id").toString();
				System.out.print("Removing MU volume " + docId + "\r");
				
				// Get parent AC-No. This is a must to get the parentSysNo (= id):
				String mhAC = (doc.getFieldValue("parentAC_str") != null) ? doc.getFieldValue("parentAC_str").toString() : null;

				
				// Variables for atomic updates of parent series:
				String mhSYS = "0";
				
				SolrQuery queryMH = new SolrQuery(); // New query for parent of current volume
				queryMH.setQuery("acNo_str:" + mhAC); // Set query for parent
				queryMH.setFields("id"); // Set fields that should be given back from the query
				QueryResponse responseMHs = sServer.query(queryMH); // Execute query
				SolrDocumentList resultListMHs = responseMHs.getResults();


				if (!resultListMHs.isEmpty() && resultListMHs != null && resultListMHs.getNumFound() > 0) { // Parent record exists
					SolrDocument resultDocMH = resultListMHs.get(0); // Get first document from query result (there should be only one!)
					mhSYS = (resultDocMH.getFieldValue("id") != null) ? resultDocMH.getFieldValue("id").toString() : "0";
					
					
					// TODO: Check if the field for child SYS numbers exists. If not, there is nothing to remove and we can skip this record.

					// Prepare parent record for atomic updates:
					SolrInputDocument mhRemoveVolumeDoc = null;
					mhRemoveVolumeDoc = new SolrInputDocument();
					mhRemoveVolumeDoc.addField("id", mhSYS);
					
					
					// Remove all field values to avoid double entries of the same volume.
					Map<String, String> mapRemoveVolumeSYS = new HashMap<String, String>();
					mapRemoveVolumeSYS.put("set", null);
					mhRemoveVolumeDoc.setField("childSYS_str_mv", mapRemoveVolumeSYS);

					Map<String, String> mapRemoveVolumeAC = new HashMap<String, String>();
					mapRemoveVolumeAC.put("set", null);
					mhRemoveVolumeDoc.setField("childAC_str_mv", mapRemoveVolumeAC);

					Map<String, String> mapRemoveVolumeTitle = new HashMap<String, String>();
					mapRemoveVolumeTitle.put("set", null);
					mhRemoveVolumeDoc.setField("childTitle_str_mv", mapRemoveVolumeTitle);

					Map<String, String> mapRemoveVolumeNo = new HashMap<String, String>();
					mapRemoveVolumeNo.put("set", null);
					mhRemoveVolumeDoc.setField("childVolumeNo_str_mv", mapRemoveVolumeNo);

					Map<String, String> mapRemoveVolumeNoSort = new HashMap<String, String>();
					mapRemoveVolumeNoSort.put("set", null);
					mhRemoveVolumeDoc.setField("childVolumeNoSort_str_mv", mapRemoveVolumeNoSort);

					Map<String, String> mapRemoveVolumeEdition = new HashMap<String, String>();
					mapRemoveVolumeEdition.put("set", null);
					mhRemoveVolumeDoc.setField("childEdition_str_mv", mapRemoveVolumeEdition);

					Map<String, String> mapRemoveVolumePublishDate = new HashMap<String, String>();
					mapRemoveVolumePublishDate.put("set", null);
					mhRemoveVolumeDoc.setField("childPublishDate_str_mv", mapRemoveVolumePublishDate);
					 

					// Remove all values of serial volume to parent series record:
					mhRecordAtomicUpdateDocs.add(mhRemoveVolumeDoc);

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





}
