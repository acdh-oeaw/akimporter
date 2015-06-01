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
// https://lucidworks.com/blog/coming-soon-to-solr-efficient-cursor-based-iteration-of-large-result-sets/

public class MuVolumeToParent {

	Collection<SolrInputDocument> muAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	Collection<SolrInputDocument> mhAtomicUpdateDocs = new ArrayList<SolrInputDocument>();

	static private int NO_OF_ROWS = 1000;

	int rowCounter = 0;

	public void addMuRecords(SolrServer sServer) {

		// New Solr query
		SolrQuery queryMUs = new SolrQuery();

		// Define a query for getting all documents. We get the MU documents with a filter query because of performance (see below)
		queryMUs.setQuery("*:*");

		// The no of rows over that we can iterate ( see "for(SolrDocument doc : resultDocList)" ):
		queryMUs.setRows(NO_OF_ROWS);

		// Sort by id (more efficient for deep paging):
		queryMUs.setSort(SolrQuery.SortClause.asc("id"));

		// Set a filter query (more efficient for deep paging). Get all records, those "satztyp_str" fields conains the value "MU".
		queryMUs.setFilterQueries("satztyp_str:MU", "id:*");

		// Set fields that should be given back from the query
		queryMUs.setFields("id", "title", "acNo_str", "parentSYS_str", "parentAC_str", "volumeNo_str", "volumeNoSort_str", "publishDate", "edition");
		
		// Initialize Variable for query response:
		QueryResponse responseMUs = null;

		try {
			// Execute query
			responseMUs = sServer.query(queryMUs);

			// Get document-list from query result
			SolrDocumentList resultDocList = responseMUs.getResults();

			// Show how many documents were found
			long noOfMuRecords = resultDocList.getNumFound();
			System.out.println("\nNo. of MU records found: " + noOfMuRecords);

			// If there are some records, go on. If not, do nothing.
			if (resultDocList != null && noOfMuRecords > 0) {

				// Calculate the number of solr result pages we need to iterate over
				long wholePages = (noOfMuRecords/NO_OF_ROWS);
				long fractionPages = (noOfMuRecords%NO_OF_ROWS);
				
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
				

				if (muAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(muAtomicUpdateDocs);
				}

				if (mhAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(mhAtomicUpdateDocs);
				}

				// Set doc-collections to null (save memory):
				muAtomicUpdateDocs = null;
				mhAtomicUpdateDocs = null;

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
		SolrQuery filterqueryMUs = new SolrQuery();

		// Defin a query for getting all documents. We get the MU documents with a filter query because of performance (see below)
		filterqueryMUs.setQuery("*:*");

		// The no of rows over that we can iterate ( see "for(SolrDocument doc : resultDocList)" ):
		filterqueryMUs.setRows(NO_OF_ROWS);
		
		// Sort by id (more efficient for deep paging):
		filterqueryMUs.setSort(SolrQuery.SortClause.asc("id"));

		// Set a filter query (more efficient for deep paging). Get all records, those "satztyp_str" fields conains the value "MU".
		if (isFirstPage) { // No range filter on first page
			filterqueryMUs.setFilterQueries("satztyp_str:MU", "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			// Set start of query to 1 so that the "lastDocId" ist not the first id of the new page (we would have doubled documents then)
			filterqueryMUs.setStart(1);
			filterqueryMUs.setFilterQueries("satztyp_str:MU", "id:[" + lastDocId + " TO *]");
		}


		// Set fields that should be given back from the query
		filterqueryMUs.setFields("id", "title", "acNo_str", "parentSYS_str", "parentAC_str", "volumeNo_str", "volumeNoSort_str", "publishDate", "edition");
		
		// Initialize Variable for query response:
		QueryResponse filterresponseMUs = null;


		try {
			// Execute query
			filterresponseMUs = sServer.query(filterqueryMUs);
			
			// Get document-list from query result
			SolrDocumentList resultDocList = filterresponseMUs.getResults();
			
			String newLastDocId = resultDocList.get(resultDocList.size()-1).getFieldValue("id").toString();

			for (SolrDocument doc : resultDocList) {
				String docId = doc.getFieldValue("id").toString();
				System.out.print("Processing MU record " + docId + "\r");
				
				String muParentSYS = (doc.getFieldValue("parentSYS_str") != null) ? doc.getFieldValue("parentSYS_str").toString() : null;

				// If we already have a parentSYS_str, we already linked this MU record to it's parent MH record at some time before.
				// So we have to skip the linking process. If not, we will have the same value a second time in all the
				// multivalued fields.
				// So ONLY LINK IF muParentSYS is NULL!
				if (muParentSYS == null) {

					// Variables for atomic updates of MH and MU records:
					String muAC = (doc.getFieldValue("acNo_str") != null) ? doc.getFieldValue("acNo_str").toString() : "0";
					String muSYS = (doc.getFieldValue("id") != null) ? doc.getFieldValue("id").toString() : "0";
					String muParentAC = (doc.getFieldValue("parentAC_str") != null) ? doc.getFieldValue("parentAC_str").toString() : null;
					String muTitle = (doc.getFieldValue("title") != null) ? doc.getFieldValue("title").toString() : "0";
					String muVolumeNo = (doc.getFieldValue("volumeNo_str") != null) ? doc.getFieldValue("volumeNo_str").toString() : "0";
					String muVolumeNoSort = (doc.getFieldValue("volumeNoSort_str") != null) ? doc.getFieldValue("volumeNoSort_str").toString() : "0";
					String muEdition = (doc.getFieldValue("edition") != null) ? doc.getFieldValue("edition").toString() : "0";
					String muPublishDate = (doc.getFieldValue("publishDate") != null) ? doc.getFieldValue("publishDate").toString().replace("[", "").replace("]", "") : "0";						

					// First add data (SYS-No and title) from MH record to current MU record:
					SolrQuery queryMH = new SolrQuery(); // Query MH record of current MU record
					queryMH.setQuery("acNo_str:" + muParentAC); // Query all MU-fields
					queryMH.setFields("id", "title"); // Set fields that should be given back from the query
					QueryResponse responseMH = sServer.query(queryMH); // Execute query
					SolrDocumentList resultListMH = responseMH.getResults();

					if (!resultListMH.isEmpty() && resultListMH != null && resultListMH.getNumFound() > 0) { // Parent record exists
						SolrDocument resultDocMH = resultListMH.get(0); // Get first document from query result (there should be only one!)
						String mhSYS = (resultDocMH.getFieldValue("id") != null) ? resultDocMH.getFieldValue("id").toString() : "0";
						String mhTitle = (resultDocMH.getFieldValue("title") != null) ? resultDocMH.getFieldValue("title").toString() : "0";


						// Prepare MU record for atomic updates:
						SolrInputDocument muAtomicUpdateDoc = null;
						muAtomicUpdateDoc = new SolrInputDocument();
						muAtomicUpdateDoc.addField("id", muSYS);

						// Add values for atomic update of MU record:
						Map<String, String> mapMhSYS = new HashMap<String, String>();
						mapMhSYS.put("add", mhSYS);
						muAtomicUpdateDoc.setField("parentSYS_str", mapMhSYS);

						Map<String, String> mapMhTitle = new HashMap<String, String>();
						mapMhTitle.put("add", mhTitle);
						muAtomicUpdateDoc.setField("parentTitle_str", mapMhTitle);

						// Add all values of MU child record to MH parent record:
						muAtomicUpdateDocs.add(muAtomicUpdateDoc);


						// Prepare MH record for atomic updates:
						SolrInputDocument mhAtomicUpdateDoc = null;
						mhAtomicUpdateDoc = new SolrInputDocument();
						mhAtomicUpdateDoc.addField("id", mhSYS);

						// Add values for atomic update of MH record:
						Map<String, String> mapMuSYS = new HashMap<String, String>();
						mapMuSYS.put("add", muSYS);
						mhAtomicUpdateDoc.setField("childSYS_str_mv", mapMuSYS);

						Map<String, String> mapMuAC = new HashMap<String, String>();
						mapMuAC.put("add", muAC);
						mhAtomicUpdateDoc.setField("childAC_str_mv", mapMuAC);

						Map<String, String> mapMuTitle = new HashMap<String, String>();
						mapMuTitle.put("add", muTitle);
						mhAtomicUpdateDoc.setField("childTitle_str_mv", mapMuTitle);

						Map<String, String> mapMuVolumeNo = new HashMap<String, String>();
						mapMuVolumeNo.put("add", muVolumeNo);
						mhAtomicUpdateDoc.setField("childVolumeNo_str_mv", mapMuVolumeNo);

						Map<String, String> mapMuVolumeNoSort = new HashMap<String, String>();
						mapMuVolumeNoSort.put("add", muVolumeNoSort);
						mhAtomicUpdateDoc.setField("childVolumeNoSort_str_mv", mapMuVolumeNoSort);

						Map<String, String> mapMuEdition = new HashMap<String, String>();
						mapMuEdition.put("add", muEdition);
						mhAtomicUpdateDoc.setField("childEdition_str_mv", mapMuEdition);

						Map<String, String> mapMuPublishDate = new HashMap<String, String>();
						mapMuPublishDate.put("add", muPublishDate);
						mhAtomicUpdateDoc.setField("childPublishDate_str_mv", mapMuPublishDate);

						// Add all values of MU child record to MH parent record:
						mhAtomicUpdateDocs.add(mhAtomicUpdateDoc);
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





}
